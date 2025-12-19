#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class LogicalExternalOperator;

bool FilterUsesAddressViolatingRows(const Expression &expr);
unique_ptr<LogicalOperator> RewriteAsyncExternalFlow(unique_ptr<LogicalOperator> op);

} // namespace duckdb

