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

#include "connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"
#include <azure/storage/files/datalake.hpp>
#include <gmock/gmock.h>
#include "connectors/hive/storage_adapters/abfs/AbfsReadFile.h"
#include "connectors/hive/storage_adapters/abfs/AbfsWriteFile.h"
#include "connectors/hive/storage_adapters/abfs/tests/AzuriteServer.h"
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include <atomic>
#include <filesystem>
#include <random>

using namespace facebook::velox;
using namespace Azure::Storage::Files::DataLake;

constexpr int kOneMB = 1 << 20;
static const std::string filePath = "test_file.txt";
static const std::string fullFilePath =
    facebook::velox::filesystems::test::AzuriteABFSEndpoint + filePath;

static const std::string localTempFileDBPath = "/tmp/velox_abfs_test_gpw3VL";

class MockDataLakeFileClientImpl
    : public facebook::velox::filesystems::abfs::IFileClient {
 public:
  void Create() override {
    fileStream_ = std::ofstream(
        filePath_,
        std::ios_base::out | std::ios_base::binary | std::ios_base::app);
  }

  bool Exist() override {
    return std::filesystem::exists(filePath_);
  }

  void Append(const uint8_t* buffer, size_t size, uint64_t offset) override {
    fileStream_.seekp(offset);
    fileStream_.write(reinterpret_cast<const char*>(buffer), size);
  }

  void Flush(uint64_t position) override {
    fileStream_.flush();
  }

  uint64_t Size() override {
    std::ifstream file(filePath_, std::ios::binary | std::ios::ate);
    return static_cast<uint64_t>(file.tellg());
  }

  void Close() override {
    fileStream_.flush();
    fileStream_.close();
  }

 private:
  std::string filePath_ = localTempFileDBPath;
  std::ofstream fileStream_;
};

std::unique_ptr<WriteFile> openFileForWrite(std::string_view path) {
  auto abfsfile =
      std::make_unique<facebook::velox::filesystems::abfs::AbfsWriteFile>(
          std::string(path),
          facebook::velox::filesystems::test::AzuriteConnectionString);
  abfsfile->setFileClient(std::make_unique<MockDataLakeFileClientImpl>(
      MockDataLakeFileClientImpl()));
  abfsfile->initialize();
  return abfsfile;
}

class AbfsFileSystemTest : public testing::Test {
 public:
  static std::shared_ptr<const Config> hiveConfig(
      const std::unordered_map<std::string, std::string> configOverride = {},
      bool useAzuriteConnectionStr = true) {
    std::unordered_map<std::string, std::string> config({
        {"fs.azure.account.key.test.dfs.core.windows.net", "test"},
        {filesystems::abfs::AbfsFileSystem::kReadAbfsConnectionStr,
         facebook::velox::filesystems::test::AzuriteConnectionString},
    });

    if (!useAzuriteConnectionStr) {
      config.erase(filesystems::abfs::AbfsFileSystem::kReadAbfsConnectionStr);
    }

    // Update the default config map with the supplied configOverride map
    for (const auto& item : configOverride) {
      config[item.first] = item.second;
    }

    return std::make_shared<const core::MemConfig>(std::move(config));
  }

 public:
  static void SetUpTestSuite() {
    if (azuriteServer_ == nullptr) {
      auto config = hiveConfig();
      azuriteServer_ =
          std::make_shared<facebook::velox::filesystems::test::AzuriteServer>();
      azuriteServer_->start();
      auto tempFile = createFile();
      azuriteServer_->addFile(
          tempFile->path,
          filePath,
          config->get<std::string>(
              filesystems::abfs::AbfsFileSystem::kReadAbfsConnectionStr, ""));
    }
    filesystems::abfs::registerAbfsFileSystem();

    if (std::filesystem::exists(localTempFileDBPath)) {
      std::filesystem::remove(localTempFileDBPath);
    }
  }

  static void TearDownTestSuite() {
    if (azuriteServer_ != nullptr) {
      azuriteServer_->stop();
    }
  }

  static std::string generateRandomData(int size) {
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    std::string data(size, ' ');

    for (int i = 0; i < size; ++i) {
      int index = rand() % (sizeof(charset) - 1);
      data[i] = charset[index];
    }

    return data;
  }

  static std::atomic<bool> startThreads;

 private:
  static std::shared_ptr<::exec::test::TempFilePath> createFile(
      uint64_t size = -1) {
    auto tempFile = ::exec::test::TempFilePath::create();
    if (size == -1) {
      tempFile->append("aaaaa");
      tempFile->append("bbbbb");
      tempFile->append(std::string(kOneMB, 'c'));
      tempFile->append("ddddd");
    } else {
      const uint64_t totalSize = size * 1024 * 1024;
      const uint64_t chunkSize = 5 * 1024 * 1024;
      uint64_t remainingSize = totalSize;
      while (remainingSize > 0) {
        uint64_t dataSize = std::min(remainingSize, chunkSize);
        std::string randomData = generateRandomData(dataSize);
        tempFile->append(randomData);
        remainingSize -= dataSize;
      }
    }
    return tempFile;
  }

  static std::shared_ptr<facebook::velox::filesystems::test::AzuriteServer>
      azuriteServer_;
};

std::shared_ptr<facebook::velox::filesystems::test::AzuriteServer>
    AbfsFileSystemTest::azuriteServer_ = nullptr;
std::atomic<bool> AbfsFileSystemTest::startThreads = false;

void readData(ReadFile* readFile) {
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer1[5];
  ASSERT_EQ(readFile->pread(10 + kOneMB, 5, &buffer1), "ddddd");
  char buffer2[10];
  ASSERT_EQ(readFile->pread(0, 10, &buffer2), "aaaaabbbbb");
  auto buffer3 = new char[kOneMB];
  ASSERT_EQ(readFile->pread(10, kOneMB, buffer3), std::string(kOneMB, 'c'));
  delete[] buffer3;
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer4[10];
  const std::string_view arf = readFile->pread(5, 10, &buffer4);
  const std::string zarf = readFile->pread(kOneMB, 15);
  auto buf = std::make_unique<char[]>(8);
  const std::string_view warf = readFile->pread(4, 8, buf.get());
  const std::string_view warfFromBuf(buf.get(), 8);
  ASSERT_EQ(arf, "bbbbbccccc");
  ASSERT_EQ(zarf, "ccccccccccddddd");
  ASSERT_EQ(warf, "abbbbbcc");
  ASSERT_EQ(warfFromBuf, "abbbbbcc");
}

TEST_F(AbfsFileSystemTest, readFile) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);
  auto readFile = abfs->openFileForRead(fullFilePath);
  readData(readFile.get());
}

TEST_F(AbfsFileSystemTest, multipleThreadsWithReadFile) {
  startThreads = false;
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);

  std::vector<std::thread> threads;
  std::mt19937 generator(std::random_device{}());
  std::vector<int> sleepTimesInMicroseconds = {0, 500, 5000};
  std::uniform_int_distribution<std::size_t> distribution(
      0, sleepTimesInMicroseconds.size() - 1);
  for (int i = 0; i < 10; i++) {
    auto thread = std::thread(
        [&abfs, &distribution, &generator, &sleepTimesInMicroseconds] {
          int index = distribution(generator);
          while (!AbfsFileSystemTest::startThreads) {
            std::this_thread::yield();
          }
          std::this_thread::sleep_for(
              std::chrono::microseconds(sleepTimesInMicroseconds[index]));
          auto readFile = abfs->openFileForRead(fullFilePath);
          readData(readFile.get());
        });
    threads.emplace_back(std::move(thread));
  }
  startThreads = true;
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(AbfsFileSystemTest, missingFile) {
  try {
    auto hiveConfig = AbfsFileSystemTest::hiveConfig();
    const std::string abfsFile =
        facebook::velox::filesystems::test::AzuriteABFSEndpoint + "test.txt";
    auto abfs = filesystems::getFileSystem(abfsFile, hiveConfig);
    auto readFile = abfs->openFileForRead(abfsFile);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_TRUE(err.message().find("404") != std::string::npos);
  }
}

TEST_F(AbfsFileSystemTest, OpenFileForWriteTest) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);
  const std::string abfsFile =
      facebook::velox::filesystems::test::AzuriteABFSEndpoint + "writetest.txt";
  auto abfsWriteFile = openFileForWrite(abfsFile);
  EXPECT_EQ(abfsWriteFile->size(), 0);
  uint64_t totalSize = 0;
  std::string randomData =
      AbfsFileSystemTest::generateRandomData(1 * 1024 * 1024);
  abfsWriteFile->append(randomData);
  abfsWriteFile->append(randomData);
  abfsWriteFile->append(randomData);
  abfsWriteFile->append(randomData);
  abfsWriteFile->append(randomData);
  abfsWriteFile->append(randomData);
  abfsWriteFile->append(randomData);
  abfsWriteFile->append(randomData);
  totalSize = randomData.size() * 8;
  abfsWriteFile->flush();
  EXPECT_EQ(abfsWriteFile->size(), totalSize);

  randomData = AbfsFileSystemTest::generateRandomData(9 * 1024 * 1024);
  abfsWriteFile->append(randomData);
  totalSize += randomData.size();
  randomData = AbfsFileSystemTest::generateRandomData(2 * 1024 * 1024);
  totalSize += randomData.size();
  abfsWriteFile->append(randomData);
  abfsWriteFile->flush();
  EXPECT_EQ(abfsWriteFile->size(), totalSize);
  abfsWriteFile->flush();
  abfsWriteFile->close();
  VELOX_ASSERT_THROW(abfsWriteFile->append("abc"), "File is not open");
  VELOX_ASSERT_THROW(openFileForWrite(abfsFile), "File already exists");
}

TEST_F(AbfsFileSystemTest, renameNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);
  try {
    abfs->rename("text", "text2");
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(err.message(), std::string("rename for abfs not implemented"));
  }
}

TEST_F(AbfsFileSystemTest, removeNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);
  try {
    abfs->remove("text");
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(err.message(), std::string("remove for abfs not implemented"));
  }
}

TEST_F(AbfsFileSystemTest, existsNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);
  try {
    abfs->exists("text");
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(err.message(), std::string("exists for abfs not implemented"));
  }
}

TEST_F(AbfsFileSystemTest, listNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);
  try {
    abfs->list("dir");
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(err.message(), std::string("list for abfs not implemented"));
  }
}

TEST_F(AbfsFileSystemTest, mkdirNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);
  try {
    abfs->mkdir("dir");
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(err.message(), std::string("mkdir for abfs not implemented"));
  }
}

TEST_F(AbfsFileSystemTest, rmdirNotImplemented) {
  auto hiveConfig = AbfsFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, hiveConfig);
  try {
    abfs->rmdir("dir");
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(err.message(), std::string("rmdir for abfs not implemented"));
  }
}

TEST_F(AbfsFileSystemTest, credNotFOund) {
  const std::string abfsFile =
      std::string("abfs://test@test1.dfs.core.windows.net/test");
  auto hiveConfig = AbfsFileSystemTest::hiveConfig({}, false);
  auto abfs =
      std::make_shared<facebook::velox::filesystems::abfs::AbfsFileSystem>(
          hiveConfig);
  try {
    abfs->openFileForRead(abfsFile);
    FAIL() << "Expected VeloxException";
  } catch (VeloxException const& err) {
    EXPECT_EQ(err.errorCode(), std::string("INVALID_ARGUMENT"));
  }
}

TEST_F(AbfsFileSystemTest, splitRegion) {
  std::vector<std::tuple<uint64_t, uint64_t>> ranges;
  filesystems::abfs::AbfsReadFile::splitRegion(5, 5, ranges);
  EXPECT_EQ(1, ranges.size());
  ranges.clear();
  filesystems::abfs::AbfsReadFile::splitRegion(6, 5, ranges);
  EXPECT_EQ(1, ranges.size());
  ranges.clear();
  filesystems::abfs::AbfsReadFile::splitRegion(8, 5, ranges);
  EXPECT_EQ(2, ranges.size());
  ranges.clear();
  filesystems::abfs::AbfsReadFile::splitRegion(4, 5, ranges);
  EXPECT_EQ(1, ranges.size());
}