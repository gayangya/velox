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
#pragma once

#include <azure/storage/files/datalake.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <iostream>
#include "AbfsUtil.h"
#include "folly/io/Cursor.h"
#include "velox/common/file/File.h"

namespace facebook::velox::filesystems::abfs {
using namespace Azure::Storage::Blobs;
using namespace Azure::Storage::Files::DataLake;

class IFileClient {
 public:
  virtual void Create() = 0;
  virtual bool Exist() = 0;
  virtual void Append(const uint8_t* buffer, size_t size, uint64_t offset) = 0;
  virtual void Flush(uint64_t position) = 0;
  virtual uint64_t Size() = 0;
  virtual void Close() = 0;
};

class DataLakeFileClientWrapper : public IFileClient {
 public:
  DataLakeFileClientWrapper(
      const std::string& filePath,
      const DataLakeFileClient client)
      : filePath_(filePath),
        client_(std::make_unique<DataLakeFileClient>(client)) {}

  void Create() override {
    client_->Create();
  }

  bool Exist() override {
    try {
      client_->GetProperties();
      return true;
    } catch (Azure::Storage::StorageException& e) {
      if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
        return false;
      } else {
        throwStorageExceptionWithOperationDetails(
            "GetProperties", filePath_, e);
      }
    }
  }

  void Append(const uint8_t* buffer, size_t size, uint64_t offset) override {
    auto bodyStream = Azure::Core::IO::MemoryBodyStream(buffer, size);
    client_->Append(bodyStream, offset);
  }

  void Flush(uint64_t position) override {
    client_->Flush(position);
  }

  uint64_t Size() override {
    auto properties = client_->GetProperties();
    return properties.Value.FileSize;
  }

  void Close() override {
    // do nothing.
  }

 private:
  const std::string filePath_;
  std::unique_ptr<DataLakeFileClient> client_;
};

/// Implementation of abfs write file. Nothing written to the file should be
/// read back until it is closed.
class AbfsWriteFile : public WriteFile {
 public:
  constexpr static uint64_t kNaturalWriteSize = 8 << 20; // 8M
  /// The constructor.
  /// @param path The file path to write.
  /// @param connectStr the connection string used to auth the storage account.
  AbfsWriteFile(const std::string& path, const std::string& connectStr);

  /// check any issue reading file.
  void initialize();

  /// Get the file size.
  uint64_t size() const override;

  /// Flush the data.
  void flush() override;

  /// Write the data by append mode.
  void append(std::string_view data) override;

  /// Close the file.
  void close() override;

  /// mainly for test purpose.
  void setFileClient(std::unique_ptr<IFileClient> fileClient) {
    fileClient_ = std::move(fileClient);
  }

 private:
  const std::string path_;
  const std::string connectStr_;
  std::string fileSystem_;
  std::string fileName_;
  std::unique_ptr<IFileClient> fileClient_;

  bool enableParallelUpload_ = true;
  uint64_t position_ = -1;
  // maintain queue of bufferToUpload_ with size is 1.
  std::vector<std::shared_ptr<folly::IOBuf>> bufferToUploads_;
  std::deque<std::shared_ptr<folly::Future<folly::Unit>>> writeOpFutures;
  std::shared_ptr<folly::Executor> ioExecutor_;
  std::atomic<bool> closed_{false};

  void append(const char* buffer, size_t size);

  /// upload current active local buffer to abfs storage in ioExecutor.
  void uploadBlock();

  /// wait all appends operations finished.
  void waitForAppendsToComplete();

  /// remove all completed appends operations from queue.
  void shrinkWriteOperationQueue();
};
} // namespace facebook::velox::filesystems::abfs
