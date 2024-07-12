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

#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/Expr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/parse/Expressions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;
using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class SplitTest : public SparkFunctionBaseTest {
 protected:
  // Method runs the given split function, f.e. split(C0, C1), where C0 is the
  // input column and the C1 is delimiter column.
  // Encoding arguments control what kind of vectors we should create for the
  // function arguments.
  // limit should be set to the corresponding limit, if query contains limit
  // argument (C2).
  VectorPtr run(
      const std::vector<std::string>& input,
      const std::string& delim,
      const char* query,
      std::optional<int32_t> limit = std::nullopt,
      VectorEncoding::Simple encodingStrings = VectorEncoding::Simple::FLAT,
      VectorEncoding::Simple encodingDelims = VectorEncoding::Simple::CONSTANT,
      VectorEncoding::Simple encodingLimit = VectorEncoding::Simple::CONSTANT) {
    VectorPtr strings, delims, limits;
    const vector_size_t numRows = input.size();

    // Functors to create flat vectors, used as is and for lazy vector.
    auto createFlatStrings = [&](RowSet /*rows*/) {
      return makeFlatVector<StringView>(
          numRows, [&](vector_size_t row) { return StringView{input[row]}; });
    };

    auto createFlatDelims = [&](RowSet /*rows*/) {
      return makeFlatVector<StringView>(
          numRows, [&](vector_size_t row) { return StringView{delim}; });
    };

    auto createFlatLimits = [&](RowSet /*rows*/) {
      return makeFlatVector<int32_t>(
          numRows, [&](vector_size_t row) { return limit.value(); });
    };

    auto reverseIndices = [&](vector_size_t row) { return numRows - 1 - row; };

    // Generate strings vector
    if (isFlat(encodingStrings)) {
      strings = createFlatStrings({});
    } else if (isConstant(encodingStrings)) {
      strings = BaseVector::wrapInConstant(numRows, 0, createFlatStrings({}));
    } else if (isLazy(encodingStrings)) {
      strings = std::make_shared<LazyVector>(
          execCtx_.pool(),
          CppToType<StringView>::create(),
          numRows,
          std::make_unique<SimpleVectorLoader>(createFlatStrings));
    } else if (isDictionary(encodingStrings)) {
      strings = wrapInDictionary(
          makeIndices(numRows, reverseIndices), numRows, createFlatStrings({}));
    }

    // Generate delimiters vector
    if (isFlat(encodingDelims)) {
      delims = createFlatDelims({});
    } else if (isConstant(encodingDelims)) {
      delims = makeConstant(delim.c_str(), numRows);
    } else if (isLazy(encodingDelims)) {
      delims = std::make_shared<LazyVector>(
          execCtx_.pool(),
          CppToType<StringView>::create(),
          numRows,
          std::make_unique<SimpleVectorLoader>(createFlatDelims));
    } else if (isDictionary(encodingDelims)) {
      delims = wrapInDictionary(
          makeIndices(numRows, reverseIndices), numRows, createFlatDelims({}));
    }

    // Generate limits vector
    if (limit.has_value()) {
      if (isFlat(encodingLimit)) {
        limits = createFlatLimits({});
      } else if (isConstant(encodingLimit)) {
        limits = makeConstant(limit, numRows);
      } else if (isLazy(encodingLimit)) {
        limits = std::make_shared<LazyVector>(
            execCtx_.pool(),
            CppToType<int32_t>::create(),
            numRows,
            std::make_unique<SimpleVectorLoader>(createFlatLimits));
      } else if (isDictionary(encodingLimit)) {
        limits = wrapInDictionary(
            makeIndices(numRows, reverseIndices),
            numRows,
            createFlatLimits({}));
      }
    }

    VectorPtr result = (!limit.has_value())
        ? evaluate<BaseVector>(query, makeRowVector({strings, delims}))
        : evaluate<BaseVector>(query, makeRowVector({strings, delims, limits}));

    return VectorMaker::flatten(result);
  }

  // For expected result vectors, for some combinations of input encodings, we
  // need to massage the expected vector.
  // Const we wrap in const, dictionary we wrap in dictionary and the reast
  // leave 'as is'. In the end we flatten.
  VectorPtr prepare(
      const std::vector<std::vector<std::string>>& arrays,
      VectorEncoding::Simple stringEncoding) {
    auto arrayVector = toArrayVector(arrays);

    // Constant: we will have all rows as the 1st one.
    if (isConstant(stringEncoding)) {
      auto constVector =
          BaseVector::wrapInConstant(arrayVector->size(), 0, arrayVector);
      return VectorMaker::flatten(constVector);
    }

    // Dictionary: we will have reversed rows, because we use reverse index
    // functor to generate indices when wrapping in dictionary.
    if (isDictionary(stringEncoding)) {
      auto reverseIndices = [&](vector_size_t row) {
        return arrayVector->size() - 1 - row;
      };

      auto dictVector = wrapInDictionary(
          makeIndices(arrayVector->size(), reverseIndices),
          arrayVector->size(),
          arrayVector);
      return VectorMaker::flatten(dictVector);
    }

    // Non-const string. Unchanged.
    return arrayVector;
  }

  // Creates array vector (we use it to create expected result).
  VectorPtr toArrayVector(const std::vector<std::vector<std::string>>& data) {
    auto fSizeAt = [&](vector_size_t row) { return data[row].size(); };
    auto fValueAt = [&](vector_size_t row, vector_size_t idx) {
      return StringView{data[row][idx]};
    };

    return makeArrayVector<StringView>(data.size(), fSizeAt, fValueAt);
  }
};

// Test split vector function on vectors with different encodings.
TEST_F(SplitTest, split) {
  std::vector<std::string> inputStrings;
  std::string delim;
  std::vector<std::vector<std::string>> actualArrays;
  VectorPtr actual;
  std::vector<std::vector<std::string>> expectedArrays1;
  std::vector<std::vector<std::string>> expectedArrays2;
  std::vector<std::vector<std::string>> expectedArrays3;

  // We want to check these encodings for the vectors.
  std::vector<VectorEncoding::Simple> encodings{
      VectorEncoding::Simple::CONSTANT,
      VectorEncoding::Simple::FLAT,
      VectorEncoding::Simple::LAZY,
      VectorEncoding::Simple::DICTIONARY,
  };

  // Ascii, flat strings, flat delimiter, no limit.
  delim = ",";
  inputStrings = std::vector<std::string>{
      {"I,he,she,they"}, // Simple
      {"one,,,four,"}, // Empty strings
      {""}, // The whole string is empty
  };
  // Base expected data.
  expectedArrays1 = std::vector<std::vector<std::string>>{
      {"I", "he", "she", "they"},
      {"one", "", "", "four", ""},
      {""},
  };
  expectedArrays2 = std::vector<std::vector<std::string>>{
      {"I", "he", "she,they"},
      {"one", "", ",four,"},
      {""},
  };
  expectedArrays3 = std::vector<std::vector<std::string>>{
      {inputStrings[0]},
      {inputStrings[1]},
      {inputStrings[2]},
  };

  // Mix and match encodings.
  for (const auto& sEn : encodings) {
    for (const auto& dEn : encodings) {
      for (const auto& lEn : encodings) {
        // Cover 'no limit', 'limit <= 0', 'high limit',
        // 'small limit', 'limit = 1'.
        actual = run(
            inputStrings, delim, "split(C0, C1)", std::nullopt, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", -1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 10, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 3, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays2, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays3, sEn), actual);
      }
    }
  }

  // Check the empty delimiter special case.
  delim = "";
  auto expected = makeArrayVector<StringView>({
      {"I", ",", "h", "e", ",", "s", "h", "e", ",", "t", "h", "e", "y"},
      {"o", "n", "e", ",", ",", ",", "f", "o", "u", "r", ","},
      {""},
  });
  assertEqualVectors(expected, run(inputStrings, delim, "split(C0, C1)"));
  assertEqualVectors(
      expected, run(inputStrings, delim, "split(C0, C1, C2)", 0));
  assertEqualVectors(
      expected, run(inputStrings, delim, "split(C0, C1, C2)", -1));
  assertEqualVectors(
      expected, run(inputStrings, delim, "split(C0, C1, C2)", 20));
  auto expected2 = makeArrayVector<StringView>({
      {"I", ",", "h"},
      {"o", "n", "e"},
      {""},
  });
  assertEqualVectors(
      expected2, run(inputStrings, delim, "split(C0, C1, C2)", 3));
  auto expected3 = makeArrayVector<StringView>({
      {"I"},
      {"o"},
      {""},
  });
  assertEqualVectors(
      expected3, run(inputStrings, delim, "split(C0, C1, C2)", 1));

  delim = "A|";
  auto expected4 = makeArrayVector<StringView>({
      {"I", ",", "h", "e", ",", "s", "h", "e", ",", "t", "h", "e", "y", ""},
      {"o", "n", "e", ",", ",", ",", "f", "o", "u", "r", ",", ""},
      {""},
  });
  assertEqualVectors(expected4, run(inputStrings, delim, "split(C0, C1)"));
  auto expected5 = makeArrayVector<StringView>({
      {"I", ",he,she,they"},
      {"o", "ne,,,four,"},
      {""},
  });
  assertEqualVectors(
      expected5, run(inputStrings, delim, "split(C0, C1, C2)", 2));

  delim = "A";
  auto expected6 = makeArrayVector<StringView>({
      {"I,he,she,they"},
      {"one,,,four,"},
      {""},
  });
  assertEqualVectors(expected6, run(inputStrings, delim, "split(C0, C1)"));
  assertEqualVectors(
      expected6, run(inputStrings, delim, "split(C0, C1, C2)", 2));

  delim = "A|";
  inputStrings = std::vector<std::string>{
      {"синяя赤いトマト緑の"},
      {"Hello世界🙂"},
      {""},
  };
  auto expected7 = makeArrayVector<StringView>({
      {"с", "и", "н", "я", "я", "赤", "い", "ト", "マ", "ト", "緑", "の", ""},
      {"H", "e", "l", "l", "o", "世", "界", "🙂", ""},
      {""},
  });
  auto expected8 = makeArrayVector<StringView>({
      {"с", "иняя赤いトマト緑の"},
      {"H", "ello世界🙂"},
      {""},
  });
  assertEqualVectors(expected7, run(inputStrings, delim, "split(C0, C1)"));
  assertEqualVectors(
      expected8, run(inputStrings, delim, "split(C0, C1, C2)", 2));

  // Non-ascii, empty delimiter
  delim = "";
  inputStrings = std::vector<std::string>{
      {"синяя赤いトマト緑の空"},
      {"Hello世界🙂"},
      {""},
  };
  auto expected9 = makeArrayVector<StringView>({
      {"с", "и", "н", "я", "я", "赤", "い", "ト", "マ", "ト", "緑", "の", "空"},
      {"H", "e", "l", "l", "o", "世", "界", "🙂"},
      {""},
  });
  auto expected10 = makeArrayVector<StringView>({
      {"с", "и"},
      {"H", "e"},
      {""},
  });
  assertEqualVectors(expected9, run(inputStrings, delim, "split(C0, C1)"));
  assertEqualVectors(
      expected10, run(inputStrings, delim, "split(C0, C1, C2)", 2));

  // Non-ascii, flat strings, non-empty flat delimiter, no limit.
  delim = "లేదా";
  inputStrings = std::vector<std::string>{
      {"синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear"}, // Simple
      {"зелёное небоలేదాలేదాలేదా緑の空లేదా"}, // Empty strings
      {""}, // The whole string is empty
  };
  // Base expected data.
  expectedArrays1 = std::vector<std::vector<std::string>>{
      {"синяя слива", "赤いトマト", "黃苹果", "brown pear"},
      {"зелёное небо", "", "", "緑の空", ""},
      {""},
  };
  expectedArrays2 = std::vector<std::vector<std::string>>{
      {"синяя слива", "赤いトマト", "黃苹果లేదాbrown pear"},
      {"зелёное небо", "", "లేదా緑の空లేదా"},
      {""},
  };
  expectedArrays3 = std::vector<std::vector<std::string>>{
      {inputStrings[0]},
      {inputStrings[1]},
      {inputStrings[2]},
  };
  // Mix and match encodings.
  for (const auto& sEn : encodings) {
    for (const auto& dEn : encodings) {
      for (const auto& lEn : encodings) {
        // Cover 'no limit', 'limit <= 0', 'high limit', 'small limit', 'limit
        // 1'.
        actual = run(
            inputStrings, delim, "split(C0, C1)", std::nullopt, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", -1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 10, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 3, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays2, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays3, sEn), actual);
      }
    }
  }
}

TEST_F(SplitTest, splitWithRegex) {
  std::vector<std::string> inputStrings;
  std::string delim;
  std::vector<std::vector<std::string>> actualArrays;
  VectorPtr actual;
  std::vector<std::vector<std::string>> expectedArrays;
  std::vector<std::vector<std::string>> expectedArrays3;
  std::vector<std::vector<std::string>> expectedArrays1;
  std::vector<VectorEncoding::Simple> encodings{
      VectorEncoding::Simple::CONSTANT,
      VectorEncoding::Simple::FLAT,
      VectorEncoding::Simple::LAZY,
      VectorEncoding::Simple::DICTIONARY,
  };
  delim = "\\s*[a-z]+\\s*";
  inputStrings = std::vector<std::string>{
      "1a 2b 14m",
      "1a 2b 14",
      "",
      "a123b",
  };
  expectedArrays = std::vector<std::vector<std::string>>{
      {"1", "2", "14", ""},
      {"1", "2", "14"},
      {""},
      {"", "123", ""},
  };
  expectedArrays3 = std::vector<std::vector<std::string>>{
      {"1", "2", "14m"},
      {"1", "2", "14"},
      {""},
      {"", "123", ""},
  };
  expectedArrays1 = std::vector<std::vector<std::string>>{
      {"1a 2b 14m"},
      {"1a 2b 14"},
      {""},
      {"a123b"},
  };

  for (const auto& sEn : encodings) {
    for (const auto& dEn : encodings) {
      for (const auto& lEn : encodings) {
        // Cover 'limit <= 0', 'high limit', 'small limit', 'limit 1'.
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", -1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 10, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 3, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays3, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
      }
    }
  }
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
