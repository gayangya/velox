/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/common/memory/MmapAllocator.h"

#include <sys/mman.h>

#include "velox/common/base/Portability.h"
#include "velox/common/testutil/TestValue.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::memory {

MmapAllocator::MmapAllocator(const Options& options)
    : MemoryAllocator(),
      numAllocated_(0),
      numMapped_(0),
      capacity_(bits::roundUp(
          options.capacity / kPageSize,
          64 * sizeClassSizes_.back())),
      useMmapArena_(options.useMmapArena) {
  for (int size : sizeClassSizes_) {
    sizeClasses_.push_back(std::make_unique<SizeClass>(capacity_ / size, size));
  }

  if (useMmapArena_) {
    auto arenaSizeBytes = bits::roundUp(
        capacity_ * kPageSize / options.mmapArenaCapacityRatio, kPageSize);
    managedArenas_ = std::make_unique<ManagedMmapArenas>(
        arenaSizeBytes < MmapArena::kMinCapacityBytes
            ? MmapArena::kMinCapacityBytes
            : arenaSizeBytes);
  }
}

bool MmapAllocator::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    std::function<void(int64_t, bool)> userAllocCB,
    MachinePageCount minSizeClass) {
  auto numFreed = freeInternal(out);
  if (numFreed != 0) {
    numAllocated_.fetch_sub(numFreed);
  }
  auto mix = allocationSize(numPages, minSizeClass);
  if (numAllocated_ + mix.totalPages > capacity_) {
    return false;
  }
  if (numAllocated_.fetch_add(mix.totalPages) + mix.totalPages > capacity_) {
    numAllocated_.fetch_sub(mix.totalPages);
    return false;
  }
  ++numAllocations_;
  numAllocatedPages_ += mix.totalPages;
  if (userAllocCB != nullptr) {
    try {
      userAllocCB(mix.totalPages * kPageSize, true);
    } catch (const std::exception& e) {
      numAllocated_.fetch_sub(mix.totalPages);
      std::rethrow_exception(std::current_exception());
    }
  }
  MachinePageCount newMapsNeeded = 0;
  for (int i = 0; i < mix.numSizes; ++i) {
    bool success;
    stats_.recordAllocate(
        sizeClassSizes_[mix.sizeIndices[i]] * kPageSize,
        mix.sizeCounts[i],
        [&]() {
          success = sizeClasses_[mix.sizeIndices[i]]->allocate(
              mix.sizeCounts[i], newMapsNeeded, out);
        });
    if (TestValue::enabled()) {
      // NOTE: the test callback might overwrite 'success' to inject an
      // allocation failure for test purpose.
      TestValue::adjust(
          "facebook::velox::memory::MmapAllocator::allocate", &success);
    }
    if (!success) {
      // This does not normally happen since any size class can accommodate
      // all the capacity. 'allocatedPages_' must be out of sync.
      LOG(WARNING) << "Failed allocation in size class " << i << " for "
                   << mix.sizeCounts[i] << " pages";
      auto failedPages = mix.totalPages - out.numPages();
      freeNonContiguous(out);
      numAllocated_.fetch_sub(failedPages);
      if (userAllocCB != nullptr) {
        userAllocCB(mix.totalPages * kPageSize, false);
      }
      return false;
    }
  }
  if (newMapsNeeded == 0) {
    return true;
  }
  if (ensureEnoughMappedPages(newMapsNeeded)) {
    markAllMapped(out);
    return true;
  }
  freeNonContiguous(out);
  if (userAllocCB != nullptr) {
    userAllocCB(mix.totalPages * kPageSize, false);
  }
  return false;
}

bool MmapAllocator::ensureEnoughMappedPages(int32_t newMappedNeeded) {
  std::lock_guard<std::mutex> l(sizeClassBalanceMutex_);
  if (injectedFailure_ == Failure::kMadvise) {
    // Mimic case of not finding anything to advise away.
    injectedFailure_ = Failure::kNone;
    return false;
  }
  int totalMaps = numMapped_.fetch_add(newMappedNeeded) + newMappedNeeded;
  if (totalMaps <= capacity_) {
    // We are not at capacity. No need to advise away.
    return true;
  }
  // We need to advise away a number of pages or we fail the alloc.
  int target = totalMaps - capacity_;
  int numAdvised = adviseAway(target);
  numAdvisedPages_ += numAdvised;
  if (numAdvised >= target) {
    numMapped_.fetch_sub(numAdvised);
    return true;
  }
  numMapped_.fetch_sub(numAdvised + newMappedNeeded);
  return false;
}

int64_t MmapAllocator::freeNonContiguous(Allocation& allocation) {
  auto numFreed = freeInternal(allocation);
  numAllocated_.fetch_sub(numFreed);
  return numFreed * kPageSize;
}

MachinePageCount MmapAllocator::freeInternal(Allocation& allocation) {
  if (allocation.numRuns() == 0) {
    return 0;
  }
  MachinePageCount numFreed = 0;

  for (auto i = 0; i < sizeClasses_.size(); ++i) {
    auto& sizeClass = sizeClasses_[i];
    int32_t pages = 0;
    uint64_t clocks = 0;
    {
      ClockTimer timer(clocks);
      pages = sizeClass->free(allocation);
    }
    if (pages && FLAGS_velox_time_allocations) {
      // Increment the free time only if the allocation contained
      // pages in the class. Note that size class indices in the
      // allocator are not necessarily the same as in the stats.
      auto sizeIndex = Stats::sizeIndex(sizeClassSizes_[i] * kPageSize);
      stats_.sizes[sizeIndex].freeClocks += clocks;
    }
    numFreed += pages;
  }
  allocation.clear();
  return numFreed;
}

bool MmapAllocator::allocateContiguousImpl(
    MachinePageCount numPages,
    MmapAllocator::Allocation* FOLLY_NULLABLE collateral,
    MmapAllocator::ContiguousAllocation& allocation,
    std::function<void(int64_t, bool)> userAllocCB) {
  MachinePageCount numCollateralPages = 0;
  // 'collateral' and 'allocation' get freed anyway. But the counters
  // are not updated to reflect this. Rather, we add the delta that is
  // needed on top of collaterals to the allocation and mapped
  // counters. In this way another thread will not see the temporary
  // dip in allocation and we are sure to succeed if 'collateral' and
  // 'allocation' together cover 'numPages'. If we need more space and
  // fail to get this, then we subtract 'collateral' and 'allocation'
  // from the counters.
  //
  // Specifically, we do not subtract anything from counters with a
  // resource reservation semantic, i.e. 'numAllocated_' and
  // 'numMapped_' except at the end where the outcome of the
  // operation is clear. Otherwise we could not have the guarantee
  // that the operation succeeds if 'collateral' and 'allocation'
  // cover the new size, as other threads might grab the transiently
  // free pages.
  if (collateral) {
    numCollateralPages = freeInternal(*collateral);
  }
  int64_t numLargeCollateralPages = allocation.numPages();
  if (numLargeCollateralPages) {
    if (useMmapArena_) {
      std::lock_guard<std::mutex> l(arenaMutex_);
      managedArenas_->free(allocation.data(), allocation.size());
    } else {
      if (munmap(allocation.data(), allocation.size()) < 0) {
        LOG(ERROR) << "munmap got " << errno << "for " << allocation.data()
                   << ", " << allocation.size();
      }
    }
    allocation.reset(nullptr, nullptr, 0);
  }
  auto totalCollateralPages = numCollateralPages + numLargeCollateralPages;
  auto numCollateralUnmap = numLargeCollateralPages;
  int64_t newPages = numPages - totalCollateralPages;
  if (userAllocCB) {
    try {
      userAllocCB(newPages * kPageSize, true);
    } catch (const std::exception& e) {
      numAllocated_ -= totalCollateralPages;
      // We failed to grow by 'newPages. So we record the freeing off
      // the whole collaterall and the unmap of former 'allocation'.
      try {
        userAllocCB(
            static_cast<int64_t>(totalCollateralPages) * kPageSize, false);
      } catch (const std::exception& inner) {
      };
      numMapped_ -= numCollateralUnmap;
      numExternalMapped_ -= numCollateralUnmap;
      throw;
    }
  }

  // Rolls back the counters on failure. 'mappedDecrement is subtracted from
  // 'numMapped_' on top of other adjustment.
  auto rollbackAllocation = [&](int64_t mappedDecrement) {
    // The previous allocation and collateral were both freed but not counted as
    // freed.
    numAllocated_ -= numPages;
    try {
      userAllocCB(numPages * kPageSize, false);
    } catch (const std::exception& e) {
      // Ignore exception, this is run on failure return path.
    }
    // was incremented by numPages - numLargeCollateralPages. On failure,
    // numLargeCollateralPages are freed and numPages - numLargeCollateralPages
    // were never allocated.
    numExternalMapped_ -= numPages;
    numMapped_ -= numCollateralUnmap + mappedDecrement;
  };
  numExternalMapped_ += numPages - numCollateralUnmap;
  auto numAllocated = numAllocated_.fetch_add(newPages) + newPages;
  // Check if went over the limit. But a net decrease always succeeds even if
  // ending up over the limit because some other thread might be transiently
  // over the limit.
  if (newPages > 0 && numAllocated > capacity_) {
    rollbackAllocation(0);
    return false;
  }
  // Make sure there are free backing pages for the size minus what we just
  // unmapped.
  int64_t numToMap = numPages - numCollateralUnmap;
  if (numToMap > 0) {
    if (!ensureEnoughMappedPages(numToMap)) {
      LOG(WARNING) << "Could not advise away  enough for " << numToMap
                   << " pages for allocateContiguous";
      rollbackAllocation(0);
      return false;
    }
  } else {
    // We exchange a large mmap for a smaller one. Add negative delta.
    numMapped_ += numToMap;
  }
  void* data;
  if (injectedFailure_ == Failure::kMmap) {
    // Mimic running out of mmaps for process.
    injectedFailure_ = Failure::kNone;
    data = nullptr;
  } else {
    if (useMmapArena_) {
      std::lock_guard<std::mutex> l(arenaMutex_);
      data = managedArenas_->allocate(numPages * kPageSize);
    } else {
      data = mmap(
          nullptr,
          numPages * kPageSize,
          PROT_READ | PROT_WRITE,
          MAP_PRIVATE | MAP_ANONYMOUS,
          -1,
          0);
    }
  }
  if (!data) {
    // If the mmap failed, we have unmapped former 'allocation' and
    // the extra to be mapped.
    rollbackAllocation(numToMap);
    return false;
  }

  allocation.reset(this, data, numPages * kPageSize);
  return true;
}

void MmapAllocator::freeContiguousImpl(ContiguousAllocation& allocation) {
  if (allocation.data() && allocation.size()) {
    if (useMmapArena_) {
      std::lock_guard<std::mutex> l(arenaMutex_);
      managedArenas_->free(allocation.data(), allocation.size());
    } else {
      if (munmap(allocation.data(), allocation.size()) < 0) {
        LOG(ERROR) << "munmap returned " << errno << "for " << allocation.data()
                   << ", " << allocation.size();
      }
    }
    numMapped_ -= allocation.numPages();
    numExternalMapped_ -= allocation.numPages();
    numAllocated_ -= allocation.numPages();
    allocation.reset(nullptr, nullptr, 0);
  }
}

void* MmapAllocator::allocateBytes(uint64_t bytes, uint16_t alignment) {
  alignmentCheck(bytes, alignment);

  if (bytes <= kMaxMallocBytes) {
    auto* result = alignment > kMinAlignment ? ::aligned_alloc(alignment, bytes)
                                             : ::malloc(bytes);
    if (FOLLY_UNLIKELY(result == nullptr)) {
      LOG(ERROR) << "Failed to allocateBytes " << bytes << " bytes with "
                 << alignment << " alignment";
    }
    return result;
  }

  if (bytes <= sizeClassSizes_.back() * kPageSize) {
    Allocation allocation(this);
    auto numPages = roundUpToSizeClassSize(bytes, sizeClassSizes_);
    if (!allocateNonContiguous(numPages, allocation, nullptr, numPages)) {
      return nullptr;
    }
    auto run = allocation.runAt(0);
    VELOX_CHECK_EQ(
        1,
        allocation.numRuns(),
        "A size class allocateBytes must produce one run");
    allocation.clear();
    return run.data<char>();
  }

  ContiguousAllocation allocation;
  auto numPages = bits::roundUp(bytes, kPageSize) / kPageSize;
  if (!allocateContiguous(numPages, nullptr, allocation)) {
    return nullptr;
  }

  char* data = allocation.data<char>();
  allocation.reset(nullptr, nullptr, 0);
  return data;
}

void MmapAllocator::freeBytes(void* p, uint64_t bytes) noexcept {
  if (bytes <= kMaxMallocBytes) {
    ::free(p); // NOLINT
    return;
  }

  if (bytes <= sizeClassSizes_.back() * kPageSize) {
    Allocation allocation(this);
    auto numPages = roundUpToSizeClassSize(bytes, sizeClassSizes_);
    allocation.append(reinterpret_cast<uint8_t*>(p), numPages);
    freeNonContiguous(allocation);
    return;
  }

  ContiguousAllocation allocation;
  allocation.reset(this, p, bytes);
  freeContiguous(allocation);
}

void MmapAllocator::markAllMapped(const Allocation& allocation) {
  for (auto& sizeClass : sizeClasses_) {
    sizeClass->setAllMapped(allocation, true);
  }
}

MachinePageCount MmapAllocator::adviseAway(MachinePageCount target) {
  int numAway = 0;
  for (int i = sizeClasses_.size() - 1; i >= 0; --i) {
    numAway += sizeClasses_[i]->adviseAway(target - numAway, this);
    if (numAway >= target) {
      break;
    }
  }
  return numAway;
}

MmapAllocator::SizeClass::SizeClass(size_t capacity, MachinePageCount unitSize)
    : capacity_(capacity),
      unitSize_(unitSize),
      byteSize_(capacity_ * unitSize_ * kPageSize),
      // Min 8 words + 1 bit for every 512 bits in 'pageAllocated_'.
      mappedFreeLookup_((capacity_ / kPagesPerLookupBit / 64) + kSimdTail),
      pageBitmapSize_(capacity_ / 64),
      pageAllocated_(pageBitmapSize_ + kSimdTail),
      pageMapped_(pageBitmapSize_ + kSimdTail) {
  VELOX_CHECK(
      capacity_ % 64 == 0, "Sizeclass must have a multiple of 64 capacity.");
  void* ptr = mmap(
      nullptr,
      capacity_ * unitSize_ * kPageSize,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0);
  if (ptr == MAP_FAILED || !ptr) {
    LOG(ERROR) << "mmap failed with " << errno;
    VELOX_FAIL(
        "Could not allocate working memory"
        "mmap failed with {}",
        errno);
  }
  address_ = reinterpret_cast<uint8_t*>(ptr);
}

MmapAllocator::SizeClass::~SizeClass() {
  munmap(address_, byteSize_);
}
ClassPageCount MmapAllocator::SizeClass::checkConsistency(
    ClassPageCount& numMapped,
    int32_t& numErrors) const {
  constexpr int32_t kWordsPerLookupBit = kPagesPerLookupBit / 64;
  int count = 0;
  int mappedCount = 0;
  int mappedFreeCount = 0;
  for (int i = 0; i < pageBitmapSize_; ++i) {
    count += __builtin_popcountll(pageAllocated_[i]);
    mappedCount += __builtin_popcountll(pageMapped_[i]);
    mappedFreeCount +=
        __builtin_popcountll(~pageAllocated_[i] & pageMapped_[i]);

    if (i % kWordsPerLookupBit == 0) {
      int32_t mappedFreeInGroup = 0;
      // There is at least kSimdTail words of zeros after the last meaningful
      // word.
      for (auto j = i;
           j < std::min<int32_t>(pageBitmapSize_, i + kWordsPerLookupBit);
           ++j) {
        mappedFreeInGroup +=
            __builtin_popcountll(pageMapped_[j] & ~pageAllocated_[j]);
      }
      if (bits::isBitSet(mappedFreeLookup_.data(), i / kWordsPerLookupBit)) {
        if (!mappedFreeInGroup) {
          LOG(WARNING) << "Extra mapped free bit for group at " << i;
          ++numErrors;
        }
      } else if (mappedFreeInGroup) {
        ++numErrors;
        LOG(WARNING) << "Missing lookup bit for group at " << i;
      }
    }
  }
  if (mappedFreeCount != numMappedFreePages_) {
    ++numErrors;
    LOG(WARNING) << "Mismatched count of mapped free pages in size class "
                 << unitSize_ << ". Actual= " << mappedFreeCount
                 << " vs recorded= " << numMappedFreePages_
                 << ". Total mapped=" << mappedCount;
  }
  numMapped = mappedCount;
  return count;
}

std::string MmapAllocator::SizeClass::toString() const {
  std::stringstream out;
  int count = 0;
  int mappedCount = 0;
  int mappedFreeCount = 0;
  for (int i = 0; i < pageBitmapSize_; ++i) {
    count += __builtin_popcountll(pageAllocated_[i]);
    mappedCount += __builtin_popcountll(pageMapped_[i]);
    mappedFreeCount +=
        __builtin_popcountll(~pageAllocated_[i] & pageMapped_[i]);
  }
  auto mb = (count * MemoryAllocator::kPageSize * unitSize_) >> 20;
  out << "[size " << unitSize_ << ": " << count << "(" << mb << "MB) allocated "
      << mappedCount << " mapped";
  if (mappedFreeCount != numMappedFreePages_) {
    out << "Mismatched count of mapped free pages "
        << ". Actual= " << mappedFreeCount
        << " vs recorded= " << numMappedFreePages_
        << ". Total mapped=" << mappedCount;
  }
  out << "]";
  return out.str();
}

bool MmapAllocator::SizeClass::allocate(
    ClassPageCount numPages,
    MachinePageCount& numUnmapped,
    MmapAllocator::Allocation& out) {
  std::lock_guard<std::mutex> l(mutex_);
  return allocateLocked(numPages, &numUnmapped, out);
}

bool MmapAllocator::SizeClass::allocateLocked(
    const ClassPageCount numPages,
    MachinePageCount* FOLLY_NULLABLE numUnmapped,
    MmapAllocator::Allocation& out) {
  size_t numWords = pageBitmapSize_;
  ClassPageCount considerMappedOnly = std::min(numMappedFreePages_, numPages);
  auto numPagesToGo = numPages;
  if (considerMappedOnly) {
    int previousPages = out.numPages();
    allocateFromMappdFree(considerMappedOnly, out);
    auto numAllocated = (out.numPages() - previousPages) / unitSize_;
    if (numAllocated != considerMappedOnly) {
      VELOX_FAIL("Allocated different number of pages");
    }
    numMappedFreePages_ -= numAllocated;
    numPagesToGo -= numAllocated;
  }
  if (!numPagesToGo) {
    return true;
  }
  if (!numUnmapped) {
    return false;
  }
  uint32_t cursor = clockHand_;
  int numWordsTried = 0;
  for (;;) {
    auto previousCursor = cursor;
    if (++cursor >= numWords) {
      cursor = 0;
    }
    if (++numWordsTried > numWords) {
      return false;
    }
    uint64_t bits = pageAllocated_[cursor];
    if (bits != kAllSet) {
      int previousToGo = numPagesToGo;
      allocateAny(cursor, numPagesToGo, *numUnmapped, out);
      numAllocatedUnmapped_ += previousToGo - numPagesToGo;
      if (numPagesToGo == 0) {
        clockHand_ = previousCursor;
        return true;
      }
    }
  }
}

namespace {
bool isAllZero(xsimd::batch<uint64_t> bits) {
  return simd::allSetBitMask<uint64_t>() ==
      simd::toBitMask(bits == xsimd::broadcast<uint64_t>(0));
}
} // namespace

int32_t MmapAllocator::SizeClass::findMappedFreeGroup() {
  constexpr int32_t kWidth = xsimd::batch<int64_t>::size;
  int32_t index = lastLookupIndex_;
  if (index == kNoLastLookup) {
    index = 0;
  }
  auto lookupSize = mappedFreeLookup_.size() + kSimdTail;
  for (auto counter = 0; counter <= lookupSize; ++counter) {
    auto candidates = xsimd::load_unaligned(mappedFreeLookup_.data() + index);
    auto bits = simd::allSetBitMask<int64_t>() ^
        simd::toBitMask(candidates == xsimd::broadcast<uint64_t>(0LL));
    if (!bits) {
      index = index + kWidth <= mappedFreeLookup_.size() - kWidth
          ? index + kWidth
          : 0;
      continue;
    }
    lastLookupIndex_ = index;
    auto wordIndex = count_trailing_zeros(bits);
    auto word = mappedFreeLookup_[index + wordIndex];
    auto bit = count_trailing_zeros(word);
    return (index + wordIndex) * 64 + bit;
  }
  ClassPageCount ignore = 0;
  checkConsistency(ignore, ignore);
  LOG(FATAL) << "MMAPL: Inconsistent mapped free lookup class " << unitSize_;
}

xsimd::batch<uint64_t> MmapAllocator::SizeClass::mappedFreeBits(int32_t index) {
  return (xsimd::load_unaligned(pageAllocated_.data() + index) ^
          xsimd::broadcast<uint64_t>(~0UL)) &
      xsimd::load_unaligned(pageMapped_.data() + index);
}

void MmapAllocator::SizeClass::allocateFromMappdFree(
    int32_t numPages,
    Allocation& allocation) {
  constexpr int32_t kWidth = xsimd::batch<int64_t>::size;
  constexpr int32_t kWordsPerGroup = kPagesPerLookupBit / 64;
  int needed = numPages;
  for (;;) {
    auto group = findMappedFreeGroup() * kWordsPerGroup;
    if (group < 0) {
      return;
    }
    bool anyFound = false;
    for (auto index = group; index <= group + kWidth; index += kWidth) {
      auto bits = mappedFreeBits(index);
      uint16_t mask = simd::allSetBitMask<int64_t>() ^
          simd::toBitMask(bits == xsimd::broadcast<uint64_t>(0));
      if (!mask) {
        if (!(index < group + kWidth || anyFound)) {
          LOG(ERROR) << "MMAPL: Lookup bit set but no free mapped pages class "
                     << unitSize_;
          bits::setBit(mappedFreeLookup_.data(), group / kWordsPerGroup, false);
          return;
        }
        continue;
      }
      auto firstWord = bits::getAndClearLastSetBit(mask);
      anyFound = true;
      auto allUsed = bits::testBits(
          reinterpret_cast<uint64_t*>(&bits),
          firstWord * 64,
          sizeof(bits) * 8,
          true,
          [&](int32_t bit) {
            if (!needed) {
              return false;
            }
            auto page = index * 64 + bit;
            bits::setBit(pageAllocated_.data(), page);
            allocation.append(
                address_ + page * unitSize_ * kPageSize, unitSize_);
            --needed;
            return true;
          });

      if (allUsed) {
        if (index == group + kWidth ||
            isAllZero(mappedFreeBits(index + kWidth))) {
          bits::setBit(mappedFreeLookup_.data(), group / kWordsPerGroup, false);
        }
      }
      if (!needed) {
        return;
      }
    }
  }
}

MachinePageCount MmapAllocator::SizeClass::adviseAway(
    MachinePageCount numPages,
    MmapAllocator* allocator) {
  // Allocate as many mapped free pages as needed and advise them away.
  ClassPageCount target = bits::roundUp(numPages, unitSize_) / unitSize_;
  Allocation allocation(allocator);
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!numMappedFreePages_) {
      return 0;
    }
    target = std::min(target, numMappedFreePages_);
    allocateLocked(target, nullptr, allocation);
    VELOX_CHECK(allocation.numPages() == target * unitSize_);
    numAllocatedMapped_ -= target;
    numAdvisedAway_ += target;
  }
  // Outside of 'mutex_'.
  adviseAway(allocation);
  free(allocation);
  allocation.clear();
  return unitSize_ * target;
}

bool MmapAllocator::SizeClass::isInRange(uint8_t* ptr) const {
  if (ptr >= address_ && ptr < address_ + byteSize_) {
    // See that ptr falls on a page boundary.
    if ((ptr - address_) % unitSize_ != 0) {
      VELOX_FAIL("Pointer is in a SizeClass but not at page boundary");
    }
    return true;
  }
  return false;
}

void MmapAllocator::SizeClass::setAllMapped(
    const Allocation& allocation,
    bool value) {
  for (int i = 0; i < allocation.numRuns(); ++i) {
    MmapAllocator::PageRun run = allocation.runAt(i);
    if (!isInRange(run.data())) {
      continue;
    }
    std::lock_guard<std::mutex> l(mutex_);
    setMappedBits(run, value);
  }
}

void MmapAllocator::SizeClass::adviseAway(const Allocation& allocation) {
  for (int i = 0; i < allocation.numRuns(); ++i) {
    PageRun run = allocation.runAt(i);
    if (!isInRange(run.data())) {
      continue;
    }
    if (madvise(run.data(), run.numPages() * kPageSize, MADV_DONTNEED) < 0) {
      LOG(WARNING) << "madvise got errno " << errno;
    } else {
      std::lock_guard<std::mutex> l(mutex_);
      setMappedBits(run, false);
    }
  }
}

void MmapAllocator::SizeClass::setMappedBits(
    const MemoryAllocator::PageRun run,
    bool value) {
  const uint8_t* runAddress = run.data();
  const int firstBit = (runAddress - address_) / (unitSize_ * kPageSize);
  VELOX_CHECK(
      (runAddress - address_) % (kPageSize * unitSize_) == 0,
      "Unaligned allocation in setting mapped bits");
  const int numPages = run.numPages() / unitSize_;
  for (int page = firstBit; page < firstBit + numPages; ++page) {
    bits::setBit(pageMapped_.data(), page, value);
  }
}

MachinePageCount MmapAllocator::SizeClass::free(
    MemoryAllocator::Allocation& allocation) {
  MachinePageCount numFreed = 0;
  int firstRunInClass = -1;
  // Check if there are any runs in 'this' outside of 'mutex_'.
  for (int i = 0; i < allocation.numRuns(); ++i) {
    PageRun run = allocation.runAt(i);
    uint8_t* runAddress = run.data();
    if (isInRange(runAddress)) {
      firstRunInClass = i;
      break;
    }
  }
  if (firstRunInClass == -1) {
    return 0;
  }
  std::lock_guard<std::mutex> l(mutex_);
  for (int i = firstRunInClass; i < allocation.numRuns(); ++i) {
    PageRun run = allocation.runAt(i);
    uint8_t* runAddress = run.data();
    if (!isInRange(runAddress)) {
      continue;
    }
    const ClassPageCount numPages = run.numPages() / unitSize_;
    const int firstBit = (runAddress - address_) / (kPageSize * unitSize_);
    for (auto page = firstBit; page < firstBit + numPages; ++page) {
      if (!bits::isBitSet(pageAllocated_.data(), page)) {
        LOG(ERROR) << "Double free: page = " << page
                   << " sizeclass = " << unitSize_;
        continue;
      }
      if (bits::isBitSet(pageMapped_.data(), page)) {
        ++numMappedFreePages_;
        markMappedFree(page);
      }
      bits::clearBit(pageAllocated_.data(), page);
      numFreed += unitSize_;
    }
  }
  return numFreed;
}

void MmapAllocator::SizeClass::allocateAny(
    int32_t wordIndex,
    ClassPageCount& numPages,
    MachinePageCount& numUnmapped,
    MmapAllocator::Allocation& allocation) {
  uint64_t freeBits = ~pageAllocated_[wordIndex];
  int toAlloc = std::min(numPages, __builtin_popcountll(freeBits));
  for (int i = 0; i < toAlloc; ++i) {
    int bit = __builtin_ctzll(freeBits);
    bits::setBit(&pageAllocated_[wordIndex], bit);
    if (!(pageMapped_[wordIndex] & (1UL << bit))) {
      numUnmapped += unitSize_;
    } else {
      --numMappedFreePages_;
    }
    allocation.append(
        address_ + kPageSize * unitSize_ * (bit + wordIndex * 64), unitSize_);
    freeBits &= freeBits - 1;
  }
  numPages -= toAlloc;
}

bool MmapAllocator::checkConsistency() const {
  int count = 0;
  int mappedCount = 0;
  int32_t numErrors = 0;
  for (auto& sizeClass : sizeClasses_) {
    int mapped = 0;
    count +=
        sizeClass->checkConsistency(mapped, numErrors) * sizeClass->unitSize();
    mappedCount += mapped * sizeClass->unitSize();
  }
  if (count != numAllocated_ - numExternalMapped_) {
    ++numErrors;
    LOG(WARNING) << "Allocated count out of sync. Actual= " << count
                 << " recorded= " << numAllocated_ - numExternalMapped_;
  }
  if (mappedCount != numMapped_ - numExternalMapped_) {
    ++numErrors;
    LOG(WARNING) << "Mapped count out of sync. Actual= "
                 << mappedCount + numExternalMapped_
                 << " recorded= " << numMapped_;
  }
  if (numErrors) {
    LOG(ERROR) << "MmapAllocator::checkConsistency(): " << numErrors
               << " errors";
  }
  return numErrors == 0;
}

std::string MmapAllocator::toString() const {
  std::stringstream out;
  out << "[Memory capacity " << capacity_ << " free "
      << static_cast<int64_t>(capacity_ - numAllocated_) << " mapped "
      << numMapped_ << std::endl;
  for (auto& sizeClass : sizeClasses_) {
    out << sizeClass->toString() << std::endl;
  }
  out << "]" << std::endl;
  return out.str();
}

} // namespace facebook::velox::memory
