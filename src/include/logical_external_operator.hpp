#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

class LogicalExternalOperator : public LogicalExtensionOperator {
public:
    LogicalExternalOperator(idx_t estimated_cardinality);

    string GetName() const override {
        return "EXTERNAL_OPERATOR";
    }

    string GetExtensionName() const override {
        return "external_operator";
    }

    PhysicalOperator &CreatePlan(ClientContext &context,
                                 PhysicalPlanGenerator &generator) override;
    void Serialize(Serializer &serializer) const override;
    void PreserveTypes(const vector<LogicalType> &types_to_preserve);
    void SetStreamEndpoint(const std::string &endpoint);

protected:
    void ResolveTypes() override;
    vector<ColumnBinding> GetColumnBindings() override;
    
private:
    vector<LogicalType> preserved_types;
    bool types_preserved = false;
    std::string stream_endpoint;
};

} // namespace duckdb

