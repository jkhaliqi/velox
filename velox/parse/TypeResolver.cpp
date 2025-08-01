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

#include "velox/parse/TypeResolver.h"
#include "velox/core/ITypedExpr.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/parse/Expressions.h"
#include "velox/type/Type.h"
#include "velox/vector/VariantToVector.h"

namespace facebook::velox::parse {
namespace {

std::string toString(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  std::ostringstream signature;
  signature << functionName << "(";
  for (auto i = 0; i < argTypes.size(); i++) {
    if (i > 0) {
      signature << ", ";
    }
    signature << argTypes[i]->toString();
  }
  signature << ")";
  return signature.str();
}

std::string toString(
    const std::vector<const exec::FunctionSignature*>& signatures) {
  std::stringstream out;
  for (auto i = 0; i < signatures.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << signatures[i]->toString();
  }
  return out.str();
}

TypePtr resolveType(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs,
    const std::shared_ptr<const core::CallExpr>& expr,
    bool nullOnFailure) {
  std::vector<TypePtr> inputTypes;
  inputTypes.reserve(inputs.size());
  for (auto& input : inputs) {
    inputTypes.emplace_back(input->type());
  }

  if (auto resolvedType =
          exec::resolveTypeForSpecialForm(expr->name(), inputTypes)) {
    return resolvedType;
  }

  return resolveScalarFunctionType(expr->name(), inputTypes, nullOnFailure);
}

} // namespace

void registerTypeResolver() {
  core::Expressions::setTypeResolverHook(&resolveType);
}

TypePtr resolveScalarFunctionType(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    bool nullOnFailure) {
  auto returnType = resolveFunction(name, argTypes);
  if (returnType) {
    return returnType;
  }

  if (nullOnFailure) {
    return nullptr;
  }

  auto allSignatures = getFunctionSignatures();
  auto it = allSignatures.find(name);
  if (it == allSignatures.end()) {
    VELOX_USER_FAIL("Scalar function doesn't exist: {}.", name);
  } else {
    const auto& functionSignatures = it->second;
    VELOX_USER_FAIL(
        "Scalar function signature is not supported: {}. Supported signatures: {}.",
        toString(name, argTypes),
        toString(functionSignatures));
  }
}

} // namespace facebook::velox::parse

namespace facebook::velox::core {

// static
Expressions::TypeResolverHook Expressions::resolverHook_;
// static
Expressions::FieldAccessHook Expressions::fieldAccessHook_;

namespace {

// Determine output type based on input types.
TypePtr resolveTypeImpl(
    const std::vector<TypedExprPtr>& inputs,
    const std::shared_ptr<const CallExpr>& expr,
    bool nullOnFailure) {
  VELOX_CHECK_NOT_NULL(Expressions::getResolverHook());
  return Expressions::getResolverHook()(inputs, expr, nullOnFailure);
}

CastTypedExprPtr makeTypedCast(const TypePtr& type, const TypedExprPtr& input) {
  return std::make_shared<CastTypedExpr>(type, input, false);
}

std::vector<TypePtr> implicitCastTargets(const TypePtr& type) {
  std::vector<TypePtr> targetTypes;
  switch (type->kind()) {
    // We decide not to implicitly upcast booleans because it maybe funky.
    case TypeKind::BOOLEAN:
      break;
    case TypeKind::TINYINT:
      targetTypes.emplace_back(SMALLINT());
      [[fallthrough]];
    case TypeKind::SMALLINT:
      targetTypes.emplace_back(INTEGER());
      targetTypes.emplace_back(REAL());
      [[fallthrough]];
    case TypeKind::INTEGER:
      targetTypes.emplace_back(BIGINT());
      targetTypes.emplace_back(DOUBLE());
      [[fallthrough]];
    case TypeKind::BIGINT:
      break;
    case TypeKind::REAL:
      targetTypes.emplace_back(DOUBLE());
      [[fallthrough]];
    case TypeKind::DOUBLE:
      break;
    case TypeKind::ARRAY: {
      auto childTargetTypes = implicitCastTargets(type->childAt(0));
      for (const auto& childTarget : childTargetTypes) {
        targetTypes.emplace_back(ARRAY(childTarget));
      }
      break;
    }
    default: // make compilers happy
      break;
  }
  return targetTypes;
}

// All acceptable implicit casts on this expression.
// TODO: If we get this to be recursive somehow, we can save on cast function
// signatures that need to be compiled and registered.
std::vector<TypedExprPtr> genImplicitCasts(const TypedExprPtr& typedExpr) {
  auto targetTypes = implicitCastTargets(typedExpr->type());

  std::vector<TypedExprPtr> implicitCasts;
  implicitCasts.reserve(targetTypes.size());
  for (const auto& targetType : targetTypes) {
    implicitCasts.emplace_back(makeTypedCast(targetType, typedExpr));
  }
  return implicitCasts;
}

// TODO: Arguably all of this could be done with just Types.
TypedExprPtr adjustLastNArguments(
    std::vector<TypedExprPtr> inputs,
    const std::shared_ptr<const CallExpr>& expr,
    size_t n) {
  auto type = resolveTypeImpl(inputs, expr, true /*nullOnFailure*/);
  if (type != nullptr) {
    return std::make_shared<CallTypedExpr>(
        type, inputs, std::string{expr->name()});
  }

  if (n == 0) {
    return nullptr;
  }

  size_t firstOfLastN = inputs.size() - n;
  // use it
  std::vector<TypedExprPtr> viableExprs{inputs[firstOfLastN]};
  // or lose it
  auto&& implicitCasts = genImplicitCasts(inputs[firstOfLastN]);
  std::move(
      implicitCasts.begin(),
      implicitCasts.end(),
      std::back_inserter(viableExprs));

  for (auto& viableExpr : viableExprs) {
    inputs[firstOfLastN] = viableExpr;
    auto adjustedExpr = adjustLastNArguments(inputs, expr, n - 1);
    if (adjustedExpr != nullptr) {
      return adjustedExpr;
    }
  }

  return nullptr;
}

TypedExprPtr createWithImplicitCast(
    const std::shared_ptr<const CallExpr>& expr,
    const std::vector<TypedExprPtr>& inputs) {
  if (auto adjusted = adjustLastNArguments(inputs, expr, inputs.size())) {
    return adjusted;
  }
  auto type = resolveTypeImpl(inputs, expr, /*nullOnFailure=*/false);
  return std::make_shared<CallTypedExpr>(type, inputs, expr->name());
}

bool isLambdaArgument(
    const velox::exec::FunctionSignature& signature,
    size_t index,
    size_t numInputs) {
  return signature.isLambdaArgumentAt(index) &&
      (signature.argumentTypeAt(index).parameters().size() == numInputs + 1);
}

} // namespace

// static
TypedExprPtr Expressions::inferTypes(
    const std::shared_ptr<const IExpr>& expr,
    const TypePtr& inputRow,
    memory::MemoryPool* pool,
    const VectorPtr& complexConstants) {
  return inferTypes(expr, inputRow, {}, pool, complexConstants);
}

// static
TypedExprPtr Expressions::inferTypes(
    const std::shared_ptr<const IExpr>& expr,
    const TypePtr& inputRow,
    const std::vector<TypePtr>& lambdaInputTypes,
    memory::MemoryPool* pool,
    const VectorPtr& complexConstants) {
  VELOX_CHECK_NOT_NULL(expr);

  if (auto lambdaExpr = std::dynamic_pointer_cast<const LambdaExpr>(expr)) {
    return resolveLambdaExpr(
        lambdaExpr, inputRow, lambdaInputTypes, pool, complexConstants);
  }

  if (auto call = std::dynamic_pointer_cast<const CallExpr>(expr)) {
    if (!expr->inputs().empty()) {
      if (auto returnType = tryResolveCallWithLambdas(
              call, inputRow, pool, complexConstants)) {
        return returnType;
      }
    }
  }

  // try rebuilding complex constant type from vector
  if (auto fun = std::dynamic_pointer_cast<const CallExpr>(expr)) {
    if (fun->name() == "__complex_constant") {
      VELOX_CHECK_NOT_NULL(
          complexConstants,
          "Expression contains __complex_constant function call, but complexConstants is missing");

      auto ccInputRow = complexConstants->as<RowVector>();
      VELOX_CHECK_NOT_NULL(
          ccInputRow,
          "Expected RowVector for complexConstants: {}",
          complexConstants->toString());
      auto name =
          std::dynamic_pointer_cast<const FieldAccessExpr>(fun->inputAt(0))
              ->name();
      auto rowType = asRowType(ccInputRow->type());
      return std::make_shared<ConstantTypedExpr>(
          ccInputRow->childAt(rowType->getChildIdx(name)));
    }
  }

  std::vector<TypedExprPtr> children;
  for (auto& child : expr->inputs()) {
    children.push_back(
        inferTypes(child, inputRow, lambdaInputTypes, pool, complexConstants));
  }

  if (auto fae = std::dynamic_pointer_cast<const FieldAccessExpr>(expr)) {
    if (fieldAccessHook_) {
      if (auto result = fieldAccessHook_(fae, children)) {
        return result;
      }
    }
    VELOX_CHECK(!fae->name().empty(), "Anonymous columns are not supported");
    VELOX_CHECK_EQ(
        children.size(), 1, "Unexpected number of children in FieldAccessExpr");
    auto input = children.at(0)->type();
    auto& row = input->asRow();
    auto childIndex = row.getChildIdx(fae->name());
    if (fae->isRootColumn()) {
      return std::make_shared<FieldAccessTypedExpr>(
          input->childAt(childIndex), children.at(0), std::string{fae->name()});
    } else {
      return std::make_shared<DereferenceTypedExpr>(
          input->childAt(childIndex), children.at(0), childIndex);
    }
  }

  if (auto fun = std::dynamic_pointer_cast<const CallExpr>(expr)) {
    return createWithImplicitCast(fun, children);
  }

  if (auto input = std::dynamic_pointer_cast<const InputExpr>(expr)) {
    return std::make_shared<InputTypedExpr>(inputRow);
  }

  if (auto constant = std::dynamic_pointer_cast<const ConstantExpr>(expr)) {
    if (constant->type()->kind() == TypeKind::ARRAY) {
      // Transform variant vector into an ArrayVector, then wrap it into a
      // ConstantVector<ComplexType>.
      VELOX_CHECK_NOT_NULL(
          pool, "parsing array literals requires a memory pool");
      VectorPtr constantVector;
      if (constant->value().isNull()) {
        constantVector =
            BaseVector::createNullConstant(constant->type(), 1, pool);
      } else {
        constantVector =
            variantToVector(constant->type(), constant->value(), pool);
      }
      return std::make_shared<ConstantTypedExpr>(constantVector);
    }
    return std::make_shared<ConstantTypedExpr>(
        constant->type(), constant->value());
  }

  if (auto cast = std::dynamic_pointer_cast<const CastExpr>(expr)) {
    return std::make_shared<CastTypedExpr>(
        cast->type(), std::move(children), cast->isTryCast());
  }

  if (auto alreadyTyped = std::dynamic_pointer_cast<const ITypedExpr>(expr)) {
    return alreadyTyped;
  }

  VELOX_FAIL("Unknown expression type: {}", expr->toString());
}

// static
TypedExprPtr Expressions::resolveLambdaExpr(
    const std::shared_ptr<const LambdaExpr>& lambdaExpr,
    const TypePtr& inputRow,
    const std::vector<TypePtr>& lambdaInputTypes,
    memory::MemoryPool* pool,
    const VectorPtr& complexConstants) {
  auto names = lambdaExpr->arguments();
  auto body = lambdaExpr->body();

  VELOX_CHECK_LE(names.size(), lambdaInputTypes.size());
  std::vector<TypePtr> types;
  types.reserve(names.size());
  for (auto i = 0; i < names.size(); ++i) {
    types.push_back(lambdaInputTypes[i]);
  }

  auto signature =
      ROW(std::vector<std::string>(names), std::vector<TypePtr>(types));

  auto& inputRowType = inputRow->asRow();
  for (auto i = 0; i < inputRowType.size(); ++i) {
    if (!signature->containsChild(inputRowType.names()[i])) {
      names.push_back(inputRowType.names()[i]);
      types.push_back(inputRowType.childAt(i));
    }
  }

  auto lambdaRow = ROW(std::move(names), std::move(types));

  return std::make_shared<LambdaTypedExpr>(
      signature, inferTypes(body, lambdaRow, pool, complexConstants));
}

namespace {
bool isLambdaSignature(
    const exec::FunctionSignature& signature,
    const std::shared_ptr<const CallExpr>& callExpr) {
  if (!signature.hasLambdaArgument()) {
    return false;
  }

  const auto numArguments = callExpr->inputs().size();

  if (numArguments != signature.argumentTypes().size()) {
    return false;
  }

  bool match = true;
  for (auto i = 0; i < numArguments; ++i) {
    if (auto lambda =
            dynamic_cast<const LambdaExpr*>(callExpr->inputAt(i).get())) {
      const auto numLambdaInputs = lambda->arguments().size();
      if (!isLambdaArgument(signature, i, numLambdaInputs)) {
        match = false;
        break;
      }
    }
  }

  return match;
}

const exec::FunctionSignature* FOLLY_NULLABLE findLambdaSignature(
    const std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures,
    const std::shared_ptr<const CallExpr>& callExpr) {
  const exec::FunctionSignature* matchingSignature = nullptr;
  for (const auto& signature : signatures) {
    if (isLambdaSignature(*signature, callExpr)) {
      VELOX_CHECK_NULL(
          matchingSignature,
          "Cannot resolve ambiguous lambda function signatures for {}.",
          callExpr->name());
      matchingSignature = signature.get();
    }
  }

  return matchingSignature;
}

const exec::FunctionSignature* FOLLY_NULLABLE findLambdaSignature(
    const std::vector<const exec::FunctionSignature*>& signatures,
    const std::shared_ptr<const CallExpr>& callExpr) {
  const exec::FunctionSignature* matchingSignature = nullptr;
  for (const auto& signature : signatures) {
    if (isLambdaSignature(*signature, callExpr)) {
      VELOX_CHECK_NULL(
          matchingSignature,
          "Cannot resolve ambiguous lambda function signatures for {}.",
          callExpr->name());
      matchingSignature = signature;
    }
  }

  return matchingSignature;
}
} // namespace

const exec::FunctionSignature* findLambdaSignature(
    const std::shared_ptr<const CallExpr>& callExpr) {
  // Look for a scalar lambda function.
  auto scalarSignatures = getFunctionSignatures(callExpr->name());
  if (!scalarSignatures.empty()) {
    return findLambdaSignature(scalarSignatures, callExpr);
  }

  // Look for an aggregate lambda function.
  if (auto signatures =
          exec::getAggregateFunctionSignatures(callExpr->name())) {
    return findLambdaSignature(signatures.value(), callExpr);
  }

  return nullptr;
}

// static
TypedExprPtr Expressions::tryResolveCallWithLambdas(
    const std::shared_ptr<const CallExpr>& callExpr,
    const TypePtr& inputRow,
    memory::MemoryPool* pool,
    const VectorPtr& complexConstants) {
  auto signature = findLambdaSignature(callExpr);

  if (signature == nullptr) {
    return nullptr;
  }

  // Resolve non-lambda arguments first.
  auto numArgs = callExpr->inputs().size();
  std::vector<TypedExprPtr> children(numArgs);
  std::vector<TypePtr> childTypes(numArgs);
  for (auto i = 0; i < numArgs; ++i) {
    if (!signature->isLambdaArgumentAt(i)) {
      children[i] =
          inferTypes(callExpr->inputAt(i), inputRow, pool, complexConstants);
      childTypes[i] = children[i]->type();
    }
  }

  // Resolve lambda arguments.
  exec::SignatureBinder binder(*signature, childTypes);
  binder.tryBind();
  for (auto i = 0; i < numArgs; ++i) {
    if (signature->isLambdaArgumentAt(i)) {
      const auto& lambdaSignature = signature->argumentTypeAt(i);
      const auto& params = lambdaSignature.parameters();
      const auto lambdaTypes = binder.tryResolveTypes(
          folly::Range(params.data(), params.size() - 1));

      children[i] = inferTypes(
          callExpr->inputAt(i), inputRow, lambdaTypes, pool, complexConstants);
    }
  }

  return createWithImplicitCast(callExpr, children);
}

} // namespace facebook::velox::core
