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

#include <fmt/format.h>
#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::tpch {

struct TpchConnectorSplit : public connector::ConnectorSplit {
  explicit TpchConnectorSplit(
      const std::string& connectorId,
      size_t totalParts,
      size_t partNumber)
      : TpchConnectorSplit(connectorId, true, totalParts, partNumber) {}

  TpchConnectorSplit(
      const std::string& connectorId,
      bool cacheable,
      size_t totalParts,
      size_t partNumber)
      : ConnectorSplit(connectorId, /*_splitWeight=*/0, cacheable),
        totalParts(totalParts),
        partNumber(partNumber) {
    VELOX_CHECK_GE(totalParts, 1, "totalParts must be >= 1");
    VELOX_CHECK_GT(totalParts, partNumber, "totalParts must be > partNumber");
  }

  // In how many parts the generated TPC-H table will be segmented, roughly
  // `rowCount / totalParts`
  size_t totalParts{1};

  // Which of these parts will be read by this split.
  size_t partNumber{0};
};

} // namespace facebook::velox::connector::tpch

template <>
struct fmt::formatter<facebook::velox::connector::tpch::TpchConnectorSplit>
    : formatter<std::string> {
  auto format(
      facebook::velox::connector::tpch::TpchConnectorSplit s,
      format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<
    std::shared_ptr<facebook::velox::connector::tpch::TpchConnectorSplit>>
    : formatter<std::string> {
  auto format(
      std::shared_ptr<facebook::velox::connector::tpch::TpchConnectorSplit> s,
      format_context& ctx) const {
    return formatter<std::string>::format(s->toString(), ctx);
  }
};
