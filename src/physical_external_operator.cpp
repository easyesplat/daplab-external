#include "physical_external_operator.hpp"
#include "external_pragma.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include <fstream>
#include <chrono>
#include <thread>
#include <algorithm>
#include <cctype>

namespace duckdb {

static std::string LogicalTypeToString(const LogicalType &type) {
    if (type == LogicalType::INTEGER) return "INTEGER";
    if (type == LogicalType::BIGINT) return "BIGINT";
    if (type == LogicalType::VARCHAR) return "VARCHAR";
    if (type == LogicalType::BOOLEAN) return "BOOLEAN";
    if (type == LogicalType::DOUBLE) return "DOUBLE";
    return type.ToString();
}

PhysicalExternalOperator::PhysicalExternalOperator(PhysicalPlan &plan, vector<LogicalType> types,
                                                   idx_t estimated_cardinality, std::string stream_endpoint)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      stream_endpoint(std::move(stream_endpoint)) {
}

unique_ptr<GlobalSourceState> PhysicalExternalOperator::GetGlobalSourceState(ClientContext &context) const {
	auto state = make_uniq<ExternalGlobalSourceState>();

	// Use stream_endpoint from bind_data; otherwise, use PRAGMA value instead. 
	// TODO(@eric): Just remove pragma altogether. 
	if (!stream_endpoint.empty()) {
		state->stream_endpoint = stream_endpoint;
	} else {
		auto &pragma_storage = ExternalPragmaStorage::GetInstance();
		state->stream_endpoint = pragma_storage.GetStreamEndpoint();
	}
	
	// polling_ms and finished are retrieved as PRAGMA values. 
	auto &pragma_storage = ExternalPragmaStorage::GetInstance();
	state->polling_ms = pragma_storage.GetPollingMs();
	state->finished = pragma_storage.GetFinish();

	return state;
}

SourceResultType PhysicalExternalOperator::GetData(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<ExternalGlobalSourceState>();

	// Check if we're finished
	if (gstate.finished) {
		return SourceResultType::FINISHED;
	}

	// Check finish flag from pragma
	auto &pragma_storage = ExternalPragmaStorage::GetInstance();
	if (pragma_storage.GetFinish()) {
		gstate.finished = true;
		if (gstate.file_opened && gstate.stream_file.is_open()) {
			gstate.stream_file.close();
		}
		return SourceResultType::FINISHED;
	}

	if (!gstate.file_opened) {
		if (!gstate.stream_endpoint.empty()) {
			// TODO(@eric): Remove hardcoded elements.	
			int max_wait_seconds = 60;
			int poll_interval_ms = 100;
			int waited_ms = 0;
			
			while (waited_ms < max_wait_seconds * 1000) {
				std::ifstream file_check(gstate.stream_endpoint);
				if (file_check.good()) {
					file_check.close();
					break;
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
				waited_ms += poll_interval_ms;
			}
			
			gstate.stream_file.open(gstate.stream_endpoint, std::ios::in);
			if (!gstate.stream_file.is_open()) {
				gstate.finished = true;
				return SourceResultType::FINISHED;
			}
			// Clear any error flags
			gstate.stream_file.clear();
			gstate.file_opened = true;
		} else {
			gstate.finished = true;
			return SourceResultType::FINISHED;
		}
	}

	// Check if file is still open
	if (!gstate.stream_file.is_open()) {
		gstate.finished = true;
		return SourceResultType::FINISHED;
	}

	// Clear any error flags before reading
	gstate.stream_file.clear();

	// Reset chunk and prepare for reading
	chunk.Reset();
	chunk.SetCardinality(0);

	// Prepare vectors for each column
	for (idx_t i = 0; i < types.size(); i++) {
		chunk.data[i].SetVectorType(VectorType::FLAT_VECTOR);
	}

	// Read data in streaming fashion - fill chunk up to STANDARD_VECTOR_SIZE
	idx_t rows_read = 0;
	const idx_t chunk_capacity = STANDARD_VECTOR_SIZE; // Use STANDARD_VECTOR_SIZE instead of chunk.size()
	std::string line;

	while (rows_read < chunk_capacity) {
		// Read a line from the stream
		std::getline(gstate.stream_file, line);

		// Check stream state after getline
		bool is_eof = gstate.stream_file.eof();
		bool is_fail = gstate.stream_file.fail();
		bool is_good = gstate.stream_file.good();

		// If getline failed and we're at EOF, or if we're at EOF and line is empty (no more data)
		if ((is_fail && is_eof) || (is_eof && line.empty() && !is_good)) {
			// Don't set finished yet - we might have rows to return first
			// Just break out of the loop to return what we have
			break;
		}

		// If getline failed for other reasons (not EOF)
		if (is_fail && !is_eof) {
			gstate.stream_file.close();
			gstate.finished = true;
			break;
		}

		// Skip empty lines (but only if we're not at EOF)
		if (line.empty() && !is_eof) {
			continue;
		}
		
		// If we're at EOF and line is empty, we're done
		if (line.empty() && is_eof) {
			break;
		}

		// Parse tab-separated values
		// Map tokens to output columns by position (assuming input file columns match output column order)
		vector<string> tokens;
		std::istringstream iss(line);
		std::string token;
		while (std::getline(iss, token, '\t')) {
			tokens.push_back(token);
		}

		// Skip if we don't have enough tokens for all output columns
		if (tokens.size() < types.size()) {
			continue;
		}

		// Map each token to the corresponding output column by position
		bool parse_success = true;
		
		for (idx_t col_idx = 0; col_idx < types.size() && col_idx < tokens.size(); col_idx++) {
			try {
				auto &vec = chunk.data[col_idx];
				const LogicalType &output_type = types[col_idx];
				const std::string &token_value = tokens[col_idx];
				
				if (output_type == LogicalType::VARCHAR) {
					auto data = FlatVector::GetData<string_t>(vec);
					data[rows_read] = StringVector::AddString(vec, token_value);
				} else if (output_type == LogicalType::INTEGER) {
					auto data = FlatVector::GetData<int32_t>(vec);
					data[rows_read] = std::stoi(token_value);
				} else if (output_type == LogicalType::BIGINT) {
					auto data = FlatVector::GetData<int64_t>(vec);
					data[rows_read] = std::stoll(token_value);
				} else if (output_type == LogicalType::DOUBLE) {
					auto data = FlatVector::GetData<double>(vec);
					data[rows_read] = std::stod(token_value);
				} else if (output_type == LogicalType::BOOLEAN) {
					auto data = FlatVector::GetData<bool>(vec);
					// Support true/false, TRUE/FALSE, 1/0
					std::string lower_token = token_value;
					std::transform(lower_token.begin(), lower_token.end(), lower_token.begin(), ::tolower);
					data[rows_read] = (lower_token == "true" || lower_token == "1");
				} else {
					// Unsupported type - skip this row
					parse_success = false;
					break;
				}
			} catch (const std::exception &e) {
				parse_success = false;
				break;
			}
		}

		if (parse_success) {
			rows_read++;
			gstate.rows_emitted++;
		}
	}

	// Set the cardinality if we read any rows
	if (rows_read > 0) {
		chunk.SetCardinality(rows_read);
		// Check if we're at EOF - if so, mark as finished for next call
		if (gstate.stream_file.eof()) {
			gstate.stream_file.close();
			gstate.finished = true;
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	// No rows read - check if we're at EOF
	if (gstate.stream_file.eof() || !gstate.stream_file.is_open()) {
		if (gstate.stream_file.is_open()) {
			gstate.stream_file.close();
		}
		gstate.finished = true;
		return SourceResultType::FINISHED;
	}

	gstate.finished = true;
	return SourceResultType::FINISHED;
}

} // namespace duckdb