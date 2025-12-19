#define DUCKDB_EXTENSION_MAIN

#include "external_extension.hpp"
#include "external_init.hpp"

#include <iostream>

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "external_pragma.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb.hpp"

namespace duckdb {

// Pragma functions for configuring the external operator.
static string PragmaExternalStreamEndpoint(ClientContext &context, const FunctionParameters &parameters) {
    auto& storage = ExternalPragmaStorage::GetInstance();
    if (parameters.values.empty()) {
        return "SELECT '" + storage.GetStreamEndpoint() + "' AS stream_endpoint;";
    } else {
        storage.SetStreamEndpoint(parameters.values[0].ToString());
        return "SELECT 'Stream endpoint set to: " + parameters.values[0].ToString() + "' AS result;";
    }
}

static string PragmaExternalPollingMs(ClientContext &context, const FunctionParameters &parameters) {
    auto& storage = ExternalPragmaStorage::GetInstance();
    if (parameters.values.empty()) {
        return "SELECT " + std::to_string(storage.GetPollingMs()) + " AS polling_ms;";
    } else {
        // Set value
        auto polling_value = parameters.values[0].GetValue<int64_t>();
        storage.SetPollingMs(UnsafeNumericCast<idx_t>(polling_value));
        return "SELECT 'Polling interval set to: " + std::to_string(storage.GetPollingMs()) + " ms' AS result;";
    }
}

static string PragmaExternalFinish(ClientContext &context, const FunctionParameters &parameters) {
    auto& storage = ExternalPragmaStorage::GetInstance();
    storage.SetFinish(true);
    return "SELECT 'External stream marked as finished' AS result;";
}

static void LoadInternal(ExtensionLoader &loader) {
    auto &db = loader.GetDatabaseInstance();

    loader.RegisterFunction(PragmaFunction::PragmaCall("external_stream_endpoint", 
        PragmaExternalStreamEndpoint, 
        {},
        LogicalType::VARCHAR)); 
    loader.RegisterFunction(PragmaFunction::PragmaCall("external_polling_ms", 
        PragmaExternalPollingMs, 
        {},
        LogicalType::BIGINT));
    
    loader.RegisterFunction(PragmaFunction::PragmaStatement("external_finish", 
        PragmaExternalFinish));

    OptimizerExtension optimizer_extension;
    optimizer_extension.optimize_function =
        [](OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
            std::cout << "============ LOGICAL PLAN ============\n";
            if (plan) {
                std::cout << plan->ToString() << std::endl;
            } else {
                std::cout << "<null logical plan>" << std::endl;
            }
            std::cout << "======================================\n";
            
            // Call AddExternal to inject the extension
            AddExternal(input, plan);
			std::cout << "============ Edited PLAN ============\n";
            if (plan) {
                std::cout << plan->ToString() << std::endl;
            } else {
                std::cout << "<null logical plan>" << std::endl;
            }
            std::cout << "======================================\n";
        };

    db.config.optimizer_extensions.push_back(std::move(optimizer_extension));
}

void ExternalExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string ExternalExtension::Name() {
    return "external";
}

std::string ExternalExtension::Version() const {
    return "";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(external, loader) {
    duckdb::LoadInternal(loader);
}

DUCKDB_EXTENSION_API const char *external_version() {
    return duckdb::DuckDB::LibraryVersion();
}

} // extern "C"
