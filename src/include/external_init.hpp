#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

void AddExternal(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb

