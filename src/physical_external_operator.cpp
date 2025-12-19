#include "physical_external_operator.hpp"
#include "external_pragma.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>

namespace duckdb {

static std::string LogicalTypeToString(const LogicalType &type) {
    if (type == LogicalType::INTEGER) return "INTEGER";
    if (type == LogicalType::BIGINT) return "BIGINT";
    if (type == LogicalType::VARCHAR) return "VARCHAR";
    if (type == LogicalType::BOOLEAN) return "BOOLEAN";
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
			bool file_found = false;
			
			while (waited_ms < max_wait_seconds * 1000) {
				std::ifstream file_check(gstate.stream_endpoint);
				if (file_check.good()) {
					file_found = true;
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
			std::cout << "Reached EOF" << std::endl;
			gstate.stream_file.close();
			gstate.finished = true;
			break;
		}

		// If getline failed for other reasons (not EOF)
		if (is_fail && !is_eof) {
			gstate.stream_file.close();
			gstate.finished = true;
			break;
		}

		// Skip empty lines
		if (line.empty()) {
			continue;
		}

		// Parse tab-separated values
		// Hard-coded schema: id (INTEGER), status (BOOLEAN), value (INTEGER)
		vector<string> tokens;
		std::istringstream iss(line);
		std::string token;
		while (std::getline(iss, token, '\t')) {
			if (!token.empty()) {
				tokens.push_back(token);
			}
		}

		int32_t parsed_id = std::stoi(tokens[0]);
		std::string status_str = tokens[1];
		bool parsed_status = (status_str == "true" || status_str == "TRUE" || status_str == "1");
		int32_t parsed_value = std::stoi(tokens[2]);
		
		// Map to output columns: find which output column is INTEGER (id), BOOLEAN (status), INTEGER (value)
		idx_t integer_count = 0;  // Track which INTEGER column we're on (0=id, 1=value)
		bool parse_success = true;
		
		for (idx_t output_idx = 0; output_idx < types.size(); output_idx++) {
			auto &vec = chunk.data[output_idx];
			const LogicalType &output_type = types[output_idx];
			
			try {
				if (output_type == LogicalType::INTEGER) {
					auto data = FlatVector::GetData<int32_t>(vec);
					if (integer_count == 0) {
						// First INTEGER column = id
						data[rows_read] = parsed_id;
						integer_count++;
					} else {
						// Second INTEGER column = value
						data[rows_read] = parsed_value;
					}
				} else if (output_type == LogicalType::BOOLEAN) {
					auto data = FlatVector::GetData<bool>(vec);
					data[rows_read] = parsed_status;
				} else {
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
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	// No rows read - check if we're at EOF
	if (gstate.stream_file.eof() || !gstate.stream_file.is_open()) {
		gstate.stream_file.close();
		gstate.finished = true;
		return SourceResultType::FINISHED;
	}

	gstate.finished = true;
	return SourceResultType::FINISHED;
}

} // namespace duckdb