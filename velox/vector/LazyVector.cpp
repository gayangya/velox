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

#include "velox/vector/LazyVector.h"
#include "velox/common/base/RawVector.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox {

namespace {
void writeIOTiming(const CpuWallTiming& delta) {
  addThreadLocalRuntimeStat(
      LazyVector::kWallNanos,
      RuntimeCounter(delta.wallNanos, RuntimeCounter::Unit::kNanos));
  addThreadLocalRuntimeStat(
      LazyVector::kCpuNanos,
      RuntimeCounter(delta.cpuNanos, RuntimeCounter::Unit::kNanos));
}
} // namespace

void VectorLoader::load(RowSet rows, ValueHook* hook, VectorPtr* result) {
  {
    DeltaCpuWallTimer timer([&](auto& delta) { writeIOTiming(delta); });
    loadInternal(rows, hook, result);
  }
  if (hook) {
    // Record number of rows loaded directly into ValueHook bypassing
    // materialization into vector. This counter can be used to understand
    // whether aggregation pushdown is happening or not.
    addThreadLocalRuntimeStat("loadedToValueHook", RuntimeCounter(rows.size()));
  }
}

void VectorLoader::load(
    const SelectivityVector& rows,
    ValueHook* hook,
    VectorPtr* result) {
  {
    DeltaCpuWallTimer timer([&](auto& delta) { writeIOTiming(delta); });
    loadInternal(rows, hook, result);
  }

  if (hook) {
    // Record number of rows loaded directly into ValueHook bypassing
    // materialization into vector. This counter can be used to understand
    // whether aggregation pushdown is happening or not.
    addThreadLocalRuntimeStat("loadedToValueHook", RuntimeCounter(rows.size()));
  }
}

void VectorLoader::loadInternal(
    const SelectivityVector& rows,
    ValueHook* hook,
    VectorPtr* result) {
  if (rows.isAllSelected()) {
    const auto& indices = DecodedVector::consecutiveIndices();
    assert(!indices.empty());
    if (rows.end() <= indices.size()) {
      load(
          RowSet(&indices[rows.begin()], rows.end() - rows.begin()),
          hook,
          result);
      return;
    }
  }
  std::vector<vector_size_t> positions(rows.countSelected());
  int index = 0;
  rows.applyToSelected([&](vector_size_t row) { positions[index++] = row; });
  load(positions, hook, result);
}

VectorPtr LazyVector::slice(vector_size_t offset, vector_size_t length) const {
  VELOX_CHECK(isLoaded(), "Cannot take slice on unloaded lazy vector");
  VELOX_DCHECK(vector_);
  return vector_->slice(offset, length);
}

//   static
void LazyVector::ensureLoadedRows(
    VectorPtr& vector,
    const SelectivityVector& rows) {
  if (isLazyNotLoaded(*vector)) {
    DecodedVector decoded;
    SelectivityVector baseRows;
    ensureLoadedRows(vector, rows, decoded, baseRows);
  } else {
    // Even if LazyVectors have been loaded, via some other path, this
    // is needed for initializing wrappers.
    vector->loadedVector();
  }
}

// static
void LazyVector::ensureLoadedRowsImpl(
    VectorPtr& vector,
    DecodedVector& decoded,
    const SelectivityVector& rows,
    SelectivityVector& baseRows) {
  if (decoded.base()->encoding() != VectorEncoding::Simple::LAZY) {
    if (decoded.base()->encoding() == VectorEncoding::Simple::ROW &&
        isLazyNotLoaded(*decoded.base())) {
      decoded.unwrapRows(baseRows, rows);
      auto children = decoded.base()->asUnchecked<RowVector>()->children();
      DecodedVector decodedChild;
      SelectivityVector childRows;
      for (auto& child : children) {
        decodedChild.decode(*child, baseRows, false);
        ensureLoadedRowsImpl(child, decodedChild, baseRows, childRows);
      }
      vector->loadedVector();
    }
    return;
  }
  auto lazyVector = decoded.base()->asUnchecked<LazyVector>();
  if (lazyVector->isLoaded()) {
    if (isLazyNotLoaded(*lazyVector)) {
      decoded.unwrapRows(baseRows, rows);
      decoded.decode(*lazyVector->vector_, baseRows, false);
      SelectivityVector nestedRows;
      ensureLoadedRowsImpl(lazyVector->vector_, decoded, baseRows, nestedRows);
    }

    vector->loadedVector();
    return;
  }
  raw_vector<vector_size_t> rowNumbers;
  RowSet rowSet;
  if (decoded.isConstantMapping()) {
    rowNumbers.push_back(decoded.index(rows.begin()));
    rowSet = RowSet(rowNumbers);
  } else if (decoded.isIdentityMapping()) {
    if (rows.isAllSelected()) {
      auto iota = velox::iota(rows.end(), rowNumbers);
      rowSet = RowSet(iota, rows.end());
    } else {
      rowNumbers.resize(rows.end());
      rowNumbers.resize(simd::indicesOfSetBits(
          rows.asRange().bits(), 0, rows.end(), rowNumbers.data()));
      rowSet = RowSet(rowNumbers);
    }
  } else {
    decoded.unwrapRows(baseRows, rows);

    rowNumbers.resize(baseRows.end());
    rowNumbers.resize(simd::indicesOfSetBits(
        baseRows.asRange().bits(), 0, baseRows.end(), rowNumbers.data()));

    rowSet = RowSet(rowNumbers);

    lazyVector->load(rowSet, nullptr);
    VectorPtr loadedVector = lazyVector->loadedVectorShared();
    if (isLazyNotLoaded(*loadedVector)) {
      decoded.unwrapRows(baseRows, rows);
      DecodedVector nestedDecoded;
      nestedDecoded.decode(*lazyVector, baseRows, false);
      SelectivityVector nestedRows;
      ensureLoadedRowsImpl(loadedVector, nestedDecoded, baseRows, nestedRows);
    }

    // The loaded base vector may have fewer rows than the original. Make sure
    // there are no indices referring to rows past the end of the base vector.

    BufferPtr indices = allocateIndices(rows.end(), vector->pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    auto decodedIndices = decoded.indices();
    rows.applyToSelected(
        [&](auto row) { rawIndices[row] = decodedIndices[row]; });

    BufferPtr nulls = nullptr;
    if (decoded.nulls()) {
      if (!baseRows.hasSelections()) {
        // All valid values in 'rows' are nulls. Set the nulls buffer to all
        // nulls to avoid hitting DCHECK when creating a dictionary with a zero
        // sized base vector.
        nulls = allocateNulls(rows.end(), vector->pool(), bits::kNull);
      } else {
        nulls = allocateNulls(rows.end(), vector->pool());
        std::memcpy(
            nulls->asMutable<uint64_t>(),
            decoded.nulls(),
            bits::nbytes(rows.end()));
      }
    }

    vector = BaseVector::wrapInDictionary(
        std::move(nulls), std::move(indices), rows.end(), loadedVector);
    return;
  }
  lazyVector->load(rowSet, nullptr);

  if (isLazyNotLoaded(*lazyVector)) {
    decoded.unwrapRows(baseRows, rows);
    decoded.decode(*lazyVector->vector_, baseRows, false);
    SelectivityVector nestedRows;
    ensureLoadedRowsImpl(lazyVector->vector_, decoded, baseRows, nestedRows);
  }

  // An explicit call to loadedVector() is necessary to allow for proper
  // initialization of dictionaries, sequences, etc. on top of lazy vector
  // after it has been loaded, even if loaded via some other path.
  vector->loadedVector();
}

// static
void LazyVector::ensureLoadedRows(
    VectorPtr& vector,
    const SelectivityVector& rows,
    DecodedVector& decoded,
    SelectivityVector& baseRows) {
  decoded.decode(*vector, rows, false);
  ensureLoadedRowsImpl(vector, decoded, rows, baseRows);
}

void LazyVector::validate(const VectorValidateOptions& options) const {
  if (isLoaded() || options.loadLazy) {
    auto loadedVector = this->loadedVector();
    VELOX_CHECK_NOT_NULL(loadedVector);
    loadedVector->validate(options);
  }
}

} // namespace facebook::velox
