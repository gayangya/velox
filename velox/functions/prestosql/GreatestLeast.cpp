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

#include <cmath>
#include <type_traits>
#include "velox/common/base/Exceptions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions {

namespace {

template <bool>
class ExtremeValueFunction;

using LeastFunction = ExtremeValueFunction<true>;
using GreatestFunction = ExtremeValueFunction<false>;

/**
 * This class implements two functions:
 *
 * greatest(value1, value2, ..., valueN) → [same as input]
 * Returns the largest of the provided values.
 *
 * least(value1, value2, ..., valueN) → [same as input]
 * Returns the smallest of the provided values.
 **/
template <bool isLeast>
class ExtremeValueFunction : public exec::VectorFunction {
 private:
  template <typename T>
  bool shouldOverride(const T& currentValue, const T& candidateValue) const {
    return isLeast ? candidateValue < currentValue
                   : candidateValue > currentValue;
  }

  // For double, presto should throw error if input is Nan
  template <typename T>
  void checkNan(const T& value) const {
    if constexpr (std::is_same_v<T, TypeTraits<TypeKind::DOUBLE>::NativeType>) {
      if (std::isnan(value)) {
        VELOX_USER_FAIL(
            "Invalid argument to {}: NaN", isLeast ? "least()" : "greatest()");
      }
    }
  }

  template <typename T>
  void applyTyped(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, outputType, result);
    result->clearNulls(rows);

    auto* flatResult = result->as<FlatVector<T>>();
    BufferPtr resultValues = flatResult->mutableValues(rows.end());
    T* __restrict rawResult = resultValues->asMutable<T>();

    exec::DecodedArgs decodedArgs(rows, args, context);

    std::set<size_t> usedInputs;
    context.applyToSelectedNoThrow(rows, [&](int row) {
      size_t valueIndex = 0;

      T currentValue = decodedArgs.at(0)->valueAt<T>(row);
      checkNan(currentValue);

      for (auto i = 1; i < args.size(); ++i) {
        auto candidateValue = decodedArgs.at(i)->template valueAt<T>(row);
        checkNan(candidateValue);

        if constexpr (isLeast) {
          if (candidateValue < currentValue) {
            currentValue = candidateValue;
            valueIndex = i;
          }
        } else {
          if (candidateValue > currentValue) {
            currentValue = candidateValue;
            valueIndex = i;
          }
        }
      }
      usedInputs.insert(valueIndex);

      if constexpr (std::is_same_v<bool, T>) {
        flatResult->set(row, currentValue);
      } else {
        rawResult[row] = currentValue;
      }
    });

    if constexpr (std::is_same_v<T, StringView>) {
      for (auto index : usedInputs) {
        flatResult->acquireSharedStringBuffers(args[index].get());
      }
    }
  }

 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    switch (outputType.get()->kind()) {
      case TypeKind::BOOLEAN:
        applyTyped<bool>(rows, args, outputType, context, result);
        return;
      case TypeKind::TINYINT:
        applyTyped<int8_t>(rows, args, outputType, context, result);
        return;
      case TypeKind::SMALLINT:
        applyTyped<int16_t>(rows, args, outputType, context, result);
        return;
      case TypeKind::INTEGER:
        applyTyped<int32_t>(rows, args, outputType, context, result);
        return;
      case TypeKind::BIGINT:
        applyTyped<int64_t>(rows, args, outputType, context, result);
        return;
      case TypeKind::HUGEINT:
        applyTyped<int128_t>(rows, args, outputType, context, result);
        return;
      case TypeKind::REAL:
        applyTyped<float>(rows, args, outputType, context, result);
        return;
      case TypeKind::DOUBLE:
        applyTyped<double>(rows, args, outputType, context, result);
        return;
      case TypeKind::VARCHAR:
        applyTyped<StringView>(rows, args, outputType, context, result);
        return;
      case TypeKind::TIMESTAMP:
        applyTyped<Timestamp>(rows, args, outputType, context, result);
        return;
      default:
        VELOX_FAIL(
            "Unsupported input type for {}: {}",
            isLeast ? "least()" : "greatest()",
            outputType->toString());
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    const std::vector<std::string> types = {
        "boolean",
        "tinyint",
        "smallint",
        "integer",
        "bigint",
        "double",
        "real",
        "varchar",
        "timestamp",
        "date",
    };
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    for (const auto& type : types) {
      signatures.emplace_back(exec::FunctionSignatureBuilder()
                                  .returnType(type)
                                  .argumentType(type)
                                  .variableArity()
                                  .build());
    }
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .integerVariable("precision")
                                .integerVariable("scale")
                                .returnType("DECIMAL(precision, scale)")
                                .argumentType("DECIMAL(precision, scale)")
                                .variableArity()
                                .build());
    return signatures;
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_least,
    LeastFunction::signatures(),
    std::make_unique<LeastFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_greatest,
    GreatestFunction::signatures(),
    std::make_unique<GreatestFunction>());

} // namespace facebook::velox::functions
