#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include <string>
#include <fstream>
#include <sstream>

namespace duckdb {

class ExternalGlobalSourceState : public GlobalSourceState {
public:
    std::string stream_endpoint;
    idx_t polling_ms = 100; // Default polling interval in milliseconds
    bool finished = false;
    bool file_opened = false;
    std::ifstream stream_file;
    std::string line_buffer; // Buffer for incomplete lines
    idx_t rows_emitted = 0;
    bool has_waited_for_data = false; // Track if we've already waited for data
    
    ~ExternalGlobalSourceState() {
        if (stream_file.is_open()) {
            stream_file.close();
        }
    }
};

class PhysicalExternalOperator : public PhysicalOperator {
public:
    // Constructor
    PhysicalExternalOperator(
        PhysicalPlan &plan,
        vector<LogicalType> types,
        idx_t estimated_cardinality,
        std::string stream_endpoint = ""
    );

    // Source operator methods
    bool IsSource() const override { return true; }

    OrderPreservationType SourceOrder() const override {
        return OrderPreservationType::NO_ORDER;
    }

    unique_ptr<GlobalSourceState>
    GetGlobalSourceState(ClientContext &context) const override;

    SourceResultType GetData(ExecutionContext &context,
                             DataChunk &chunk,
                             OperatorSourceInput &input) const override;

    // Optional: give your operator a visible name in EXPLAIN
    string GetName() const override {
        return "EXTERNAL_OPERATOR";
    }
    
private:
    std::string stream_endpoint; // Stream endpoint from UDF bind_data
};

} // namespace duckdb
