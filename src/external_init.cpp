#include "external_init.hpp"
#include "async_rewrite.hpp"

namespace duckdb {

void AddExternal(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    if (!plan) {
        return; // No plan to modify
    }
    plan = RewriteAsyncExternalFlow(std::move(plan));
}

} // namespace duckdb

