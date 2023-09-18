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

#include "AbfsWriteFile.h"
#include <iostream>
#include "AbfsUtil.h"

namespace facebook::velox::filesystems::abfs {
AbfsWriteFile::AbfsWriteFile(
    const std::string& path,
    const std::string& connectStr)
    : path_(path), connectStr_(connectStr) {
  // Make it a no-op if invoked twice.
  if (position_ != -1) {
    return;
  }
  position_ = 0;
  ioExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(
      4 * std::thread::hardware_concurrency());
}

void AbfsWriteFile::initialize() {
  if (!fileClient_) {
    auto abfsAccount = AbfsAccount(path_);
    auto dataLakeFileClient = DataLakeFileClient::CreateFromConnectionString(
        connectStr_, abfsAccount.fileSystem(), abfsAccount.filePath());
    fileClient_ = std::make_unique<DataLakeFileClientWrapper>(
        abfsAccount.filePath(), dataLakeFileClient);
  }
  VELOX_CHECK(!fileClient_->Exist(), "File already exists");
  fileClient_->Create();
}

void AbfsWriteFile::close() {
  if (!closed_) {
    flush();
    fileClient_->Close();
    closed_ = true;
  }
}

void AbfsWriteFile::flush() {
  if (!closed_) {
    if (enableParallelUpload_) {
      if (!bufferToUploads_.empty()) {
        uploadBlock();
      }
      waitForAppendsToComplete();
    }
    fileClient_->Flush(position_);
  }
}

void AbfsWriteFile::append(std::string_view data) {
  VELOX_CHECK(!closed_, "File is not open");
  if (data.size() == 0) {
    return;
  }
  append(data.data(), data.size());
}

uint64_t AbfsWriteFile::size() const {
  return fileClient_->Size();
}

void AbfsWriteFile::append(const char* buffer, size_t size) {
  if (enableParallelUpload_) {
    if (bufferToUploads_.empty()) {
      bufferToUploads_.emplace_back(std::make_shared<folly::IOBuf>(
          folly::IOBuf(folly::IOBuf::CREATE, kNaturalWriteSize)));
    }
    auto bufferToUpload = bufferToUploads_.back();
    uint64_t remaingCapacity = bufferToUpload->tailroom();
    char* b = reinterpret_cast<char*>(bufferToUpload->writableTail());
    if (size > remaingCapacity) {
      memcpy(b, buffer, remaingCapacity);
      bufferToUpload->append(remaingCapacity);
      uploadBlock();
      this->append(buffer + remaingCapacity, size - remaingCapacity);
    } else {
      memcpy(b, buffer, size);
      bufferToUpload->append(size);
      if (bufferToUpload->tailroom() == 0) {
        uploadBlock();
      }
    }
  } else {
    auto offset = position_;
    position_ += size;
    fileClient_->Append(reinterpret_cast<const uint8_t*>(buffer), size, offset);
  }
}

void AbfsWriteFile::uploadBlock() {
  VELOX_CHECK_EQ(bufferToUploads_.size(), 1);
  auto bufferToUpload = bufferToUploads_.back();
  bufferToUploads_.pop_back();
  auto offset = position_;
  position_ += bufferToUpload->length();
  auto job = std::make_shared<folly::Future<folly::Unit>>(folly::via(
      ioExecutor_.get(), [this, offset, buffer = bufferToUpload]() mutable {
        fileClient_->Append(buffer->writableData(), buffer->length(), offset);
      }));
  writeOpFutures.push_back(job);
  shrinkWriteOperationQueue();
}

void AbfsWriteFile::waitForAppendsToComplete() {
  for (int64_t i = writeOpFutures.size() - 1; i >= 0; --i) {
    writeOpFutures[i]->wait();
  }
}

void AbfsWriteFile::shrinkWriteOperationQueue() {
  auto op = writeOpFutures.begin()->get();
  while (op->isReady()) {
    writeOpFutures.pop_front();
    if (!writeOpFutures.empty()) {
      op = writeOpFutures.begin()->get();
    } else
      break;
  }
}

} // namespace facebook::velox::filesystems::abfs
