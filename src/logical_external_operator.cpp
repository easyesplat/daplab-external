#include "logical_external_operator.hpp"
#include "physical_external_operator.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

LogicalExternalOperator::LogicalExternalOperator(idx_t estimated_cardinality)
    : LogicalExtensionOperator(), types_preserved(false) {
    this->estimated_cardinality = estimated_cardinality;
}

void LogicalExternalOperator::PreserveTypes(const vector<LogicalType> &types_to_preserve) {
    preserved_types = types_to_preserve;
    types_preserved = true;
    types = types_to_preserve; 
}

void LogicalExternalOperator::SetStreamEndpoint(const std::string &endpoint) {
    stream_endpoint = endpoint;
}

void LogicalExternalOperator::ResolveTypes() {
    if (!types_preserved || preserved_types.empty()) {
        throw InternalException("Types not resolved.");
    }

    types = preserved_types;
}

vector<ColumnBinding> LogicalExternalOperator::GetColumnBindings() {
    vector<ColumnBinding> bindings;
    bindings.reserve(types.size());

    idx_t table_index = 0;
    for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
        bindings.emplace_back(table_index, col_idx);
    }
    return bindings;
}

PhysicalOperator &LogicalExternalOperator::CreatePlan(ClientContext &context,
                                                      PhysicalPlanGenerator &generator) {
    // TODO(@eric): Remove this unnecessary function call.
    ResolveTypes();                                            

    return generator.Make<PhysicalExternalOperator>(
        types,
        estimated_cardinality,
        stream_endpoint
    );
}

void LogicalExternalOperator::Serialize(Serializer &serializer) const {
    LogicalExtensionOperator::Serialize(serializer);
    serializer.WriteProperty(100, "estimated_cardinality", estimated_cardinality);
}

} // namespace duckdb

