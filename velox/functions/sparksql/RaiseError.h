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

#include "velox/functions/Macros.h"
namespace facebook::velox::functions::sparksql {

template <typename T>
struct RaiseErrorFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      UnknownValue& result,
      const arg_type<Varchar>& input) {
    throw std::runtime_error(input);
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void callNullable(
      UnknownValue& result,
      const TInput* /*input*/) {
    throw std::runtime_error("");
  }
};
} // namespace facebook::velox::functions::sparksql
