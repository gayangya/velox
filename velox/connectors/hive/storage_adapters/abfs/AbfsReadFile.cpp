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

#include "AbfsReadFile.h"
#include "AbfsUtil.h"

#include <fmt/format.h>
#include <folly/synchronization/CallOnce.h>
#include <glog/logging.h>

namespace facebook::velox::filesystems::abfs {

AbfsReadFile::AbfsReadFile(
    const std::string& path,
    const std::string& connectStr)
    : path_(path), connectStr_(connectStr) {
  auto abfsAccount = AbfsAccount(path_);
  fileSystem_ = abfsAccount.fileSystem();
  fileName_ = abfsAccount.filePath();
  fileClient_ =
      std::make_unique<BlobClient>(BlobClient::CreateFromConnectionString(
          connectStr_, fileSystem_, fileName_));
}

// Gets the length of the file.
// Checks if there are any issues reading the file.
void AbfsReadFile::initialize() {
  // Make it a no-op if invoked twice.
  if (length_ != -1) {
    return;
  }
  try {
    auto properties = fileClient_->GetProperties();
    length_ = properties.Value.BlobSize;
  } catch (Azure::Storage::StorageException& e) {
    throwStorageExceptionWithOperationDetails("GetProperties", fileName_, e);
  }

  VELOX_CHECK_GE(length_, 0);
}

std::string_view
AbfsReadFile::pread(uint64_t offset, uint64_t length, void* buffer) const {
  preadInternal(offset, length, static_cast<char*>(buffer));
  return {static_cast<char*>(buffer), length};
}

std::string AbfsReadFile::pread(uint64_t offset, uint64_t length) const {
  std::string result(length, 0);
  preadInternal(offset, length, result.data());
  return result;
}

uint64_t AbfsReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  size_t length = 0;
  auto size = buffers.size();
  for (auto& range : buffers) {
    length += range.size();
  }
  std::string result(length, 0);
  preadInternal(offset, length, static_cast<char*>(result.data()));
  size_t resultOffset = 0;
  for (auto range : buffers) {
    if (range.data()) {
      memcpy(range.data(), &(result.data()[resultOffset]), range.size());
    }
    resultOffset += range.size();
  }

  return length;
}

void AbfsReadFile::preadv(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs) const {
  VELOX_CHECK_EQ(regions.size(), iobufs.size());
  for (size_t i = 0; i < regions.size(); ++i) {
    const auto& region = regions[i];
    auto& output = iobufs[i];
    output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
    pread(region.offset, region.length, output.writableData());
    output.append(region.length);
  }
}

uint64_t AbfsReadFile::size() const {
  return length_;
}

uint64_t AbfsReadFile::memoryUsage() const {
  return 3 * sizeof(std::string) + sizeof(int64_t);
}

bool AbfsReadFile::shouldCoalesce() const {
  return false;
}

std::string AbfsReadFile::getName() const {
  return fileName_;
}

uint64_t AbfsReadFile::getNaturalReadSize() const {
  return kNaturalReadSize;
}

void AbfsReadFile::preadInternal(
    uint64_t offset,
    uint64_t length,
    char* position) const {
  // Read the desired range of bytes.
  Azure::Core::Http::HttpRange range;
  range.Offset = offset;
  range.Length = length;

  Azure::Storage::Blobs::DownloadBlobOptions what;
  what.Range = range;

  auto response = fileClient_->Download(what);
  response.Value.BodyStream->ReadToCount(
      reinterpret_cast<uint8_t*>(position), length);
}

// static
uint64_t AbfsReadFile::calculateSplitQuantum(
    const uint64_t length,
    const uint64_t loadQuantum) {
  if (length <= loadQuantum * kReadConcurrency) {
    return loadQuantum;
  } else {
    return length / kReadConcurrency;
  }
}

// static
void AbfsReadFile::splitRegion(
    const uint64_t length,
    const uint64_t loadQuantum,
    std::vector<std::tuple<uint64_t, uint64_t>>& range) {
  uint64_t cursor = 0;
  while (cursor + loadQuantum < length) {
    range.emplace_back(cursor, loadQuantum);
    cursor += loadQuantum;
  }

  if ((length - cursor) > (loadQuantum / 2)) {
    range.emplace_back(cursor, (length - cursor));
  } else {
    auto last = range.back();
    range.pop_back();
    range.emplace_back(
        std::get<0>(last), std::get<1>(last) + (length - cursor));
  }
}
} // namespace facebook::velox::filesystems::abfs
