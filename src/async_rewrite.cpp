#include "async_rewrite.hpp"
#include "logical_external_operator.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

// TODO(@charlie) Change this accordingly. 
const string UDF_FUNCTION_NAME = "address_violating_rows";

// Helper function to check if an expression contains a specific UDF function name.
bool FilterUsesAddressViolatingRows(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		if (func_expr.function.name == UDF_FUNCTION_NAME) {
			return true;
		}
	}

	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (FilterUsesAddressViolatingRows(child)) {
			found = true;
		}
	});

	return found;
}

// Helper function to walk up the operator tree to find INSERT and get expected column count
idx_t FindInsertExpectedColumnCount(LogicalOperator* op, int max_depth = 20) {
	if (!op || max_depth <= 0) {
		return 0;
	}
	
	// Check if this is an INSERT operator
	if (op->type == LogicalOperatorType::LOGICAL_INSERT) {
		// We can't access LogicalInsert directly without the header, but we can check types
		// The INSERT's child should have the expected column count
		if (!op->children.empty() && op->children[0]) {
			op->children[0]->ResolveOperatorTypes();
			idx_t expected_count = op->children[0]->types.size();
			return expected_count;
		}
	}
	
	// Recursively check children (walking down the tree)
	for (auto &child : op->children) {
		if (child) {
			idx_t count = FindInsertExpectedColumnCount(child.get(), max_depth - 1);
			if (count > 0) {
				return count;
			}
		}
	}
	
	return 0;
}

// Main rewrite function.
// Helper function to extract UDF information from a filter
struct UDFInfo {
	bool uses_udf = false;
	std::string stream_endpoint;
	idx_t udf_column_count = 0;
	vector<LogicalType> udf_column_types;
	idx_t source_column_count = 0;
	idx_t original_column_count = 0;  // SELECT column count (excluding source columns)
};

UDFInfo ExtractUDFInfo(LogicalFilter &filter_op) {
	UDFInfo info;
	
	for (auto &expr : filter_op.expressions) {
		if (FilterUsesAddressViolatingRows(*expr)) {
			info.uses_udf = true;
			
			ExpressionIterator::VisitExpressionClass(*expr, ExpressionClass::BOUND_FUNCTION, [&](const Expression &child) {
				auto &func_expr = child.Cast<BoundFunctionExpression>();
				if (func_expr.function.name == UDF_FUNCTION_NAME && !func_expr.children.empty()) {
					// The UDF signature is: address_violating_rows(col1, col2, ..., constraint, description, column_names_json, stream_endpoint)
					// So we have: N column values + 4 fixed arguments = total
					// Therefore: N = total - 4
					idx_t total_args = func_expr.children.size();
					if (total_args >= 4) {
						info.udf_column_count = total_args - 4;
						
						// Extract types from the first N column arguments
						info.udf_column_types.clear();
						info.udf_column_types.reserve(info.udf_column_count);
						for (idx_t i = 0; i < info.udf_column_count; i++) {
							auto &col_arg = func_expr.children[i];
							LogicalType col_type = col_arg->return_type;
							info.udf_column_types.push_back(col_type);
						}
						
						// Extract column_names_json (second-to-last argument) to count source columns
						if (total_args >= 2) {
							auto &col_names_arg = func_expr.children[total_args - 2];
							if (col_names_arg->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
								auto &const_expr = col_names_arg->Cast<BoundConstantExpression>();
								if (const_expr.value.type() == LogicalType::VARCHAR) {
									std::string col_names_json = const_expr.value.GetValue<std::string>();
									// Count source columns (those with a dot, e.g., "bank_txn.category")
									info.source_column_count = 0;
									size_t pos = 0;
									while ((pos = col_names_json.find('"', pos)) != std::string::npos) {
										size_t start = pos + 1;
										size_t end = col_names_json.find('"', start);
										if (end != std::string::npos) {
											std::string col_name = col_names_json.substr(start, end - start);
											if (col_name.find('.') != std::string::npos) {
												info.source_column_count++;
											}
											pos = end + 1;
										} else {
											break;
										}
									}
								}
							}
						}
						
						// Also extract stream_endpoint (last argument)
						auto &last_arg = func_expr.children.back();
						if (last_arg->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
							auto &const_expr = last_arg->Cast<BoundConstantExpression>();
							if (const_expr.value.type() == LogicalType::VARCHAR) {
								info.stream_endpoint = const_expr.value.GetValue<std::string>();
							}
						}
					}
				}
			});
			break;
		}
	}
	
	// Calculate SELECT column count: UDF receives source columns first, then SELECT columns
	info.original_column_count = (info.udf_column_count > info.source_column_count) ? (info.udf_column_count - info.source_column_count) : info.udf_column_count;
	
	return info;
}

unique_ptr<LogicalOperator> RewriteAsyncExternalFlow(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> op) {
	if (!op) {
		return op;
	}

	// First, recursively process children
	for (auto &child : op->children) {
		child = RewriteAsyncExternalFlow(input, std::move(child));
	}

	// After processing children, check if this is an INSERT operator
	// If so, check if its child (SELECT projection) has a filter with UDF
	if (op->type == LogicalOperatorType::LOGICAL_INSERT) {
		if (!op->children.empty() && op->children[0]) {
			// The INSERT's child should be the SELECT projection
			// Check if it has a filter with UDF in its descendants
			UDFInfo udf_info;
			bool found_udf = false;
			
			// Walk down the tree to find a filter with UDF
			std::function<void(LogicalOperator*)> find_udf = [&](LogicalOperator* node) {
				if (!node) return;
				if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
					auto &filter_op = node->Cast<LogicalFilter>();
					UDFInfo info = ExtractUDFInfo(filter_op);
					if (info.uses_udf) {
						udf_info = info;
						found_udf = true;
						return;
					}
				}
				for (auto &child : node->children) {
					if (child) {
						find_udf(child.get());
						if (found_udf) return;
					}
				}
			};
			
			find_udf(op->children[0].get());
			
			if (found_udf) {
				// The INSERT's child is the SELECT projection
				// We'll add a UNION after it, combining:
				// 1. The SELECT projection output (filtered results)
				// 2. The external operator (fixed rows)
				
				auto select_projection = std::move(op->children[0]);
				select_projection->ResolveOperatorTypes();
				
				// Get the column count and types from the SELECT projection
				idx_t union_column_count = select_projection->types.size();
				vector<LogicalType> union_types = select_projection->types;
				
				// Create external operator with types matching what the UDF writes
				vector<LogicalType> external_op_types = udf_info.udf_column_types;
				
				auto external_op = make_uniq<LogicalExternalOperator>(op->estimated_cardinality);
				external_op->PreserveTypes(external_op_types);
				external_op->ResolveOperatorTypes();
				
				if (!udf_info.stream_endpoint.empty()) {
					external_op->SetStreamEndpoint(udf_info.stream_endpoint);
				}
				
				// Project external operator to match SELECT projection
				// The UDF receives columns in order: source table columns + SELECT output columns
				// We want to keep only the SELECT output columns (last N columns)
				unique_ptr<LogicalOperator> external_op_for_union = std::move(external_op);
				if (external_op_for_union->types.size() != union_column_count) {
					if (external_op_for_union->types.size() < union_column_count) {
						throw InternalException("External operator has fewer columns (%llu) than UNION requires (%llu)", 
						                        external_op_for_union->types.size(), union_column_count);
					}
					// Take the last N columns (SELECT output), skipping source columns at the beginning
					idx_t skip_count = external_op_for_union->types.size() - union_column_count;
					
					vector<unique_ptr<Expression>> projection_expressions;
					projection_expressions.reserve(union_column_count);
					for (idx_t i = 0; i < union_column_count; i++) {
						idx_t source_idx = skip_count + i;
						unique_ptr<Expression> ref_expr = make_uniq<BoundReferenceExpression>(external_op_for_union->types[source_idx], source_idx);
						
						// Cast to match SELECT projection type if needed
						if (ref_expr->return_type != union_types[i]) {
							ref_expr = BoundCastExpression::AddCastToType(input.context, std::move(ref_expr), union_types[i]);
						}
						
						projection_expressions.push_back(std::move(ref_expr));
					}
					
					// Get table_index from SELECT projection
					auto select_bindings = select_projection->GetColumnBindings();
					idx_t union_table_index = 0;
					if (!select_bindings.empty()) {
						union_table_index = select_bindings[0].table_index;
					}
					
					idx_t projection_table_index = union_table_index + 1;
					auto projection = make_uniq<LogicalProjection>(projection_table_index, std::move(projection_expressions));
					projection->children.push_back(std::move(external_op_for_union));
					projection->ResolveOperatorTypes();
					external_op_for_union = std::move(projection);
				}
				
				// Create UNION combining SELECT projection and external operator
				auto select_bindings = select_projection->GetColumnBindings();
				idx_t union_table_index = 0;
				if (!select_bindings.empty()) {
					union_table_index = select_bindings[0].table_index;
				}
				
				auto union_op = make_uniq<LogicalSetOperation>(union_table_index,             
				                                               union_column_count,
				                                               std::move(select_projection),  // top (SELECT projection output)
				                                               std::move(external_op_for_union),    // bottom (external source, projected to match)
				                                               LogicalOperatorType::LOGICAL_UNION,
				                                               true,  // setop_all (true = UNION ALL - keep duplicates)
				                                               true   // allow_out_of_order
				);
				
				// Replace INSERT's child with UNION
				op->children[0] = std::move(union_op);
				op->ResolveOperatorTypes();
			}
		}
	}

	return op;
}

} // namespace duckdb
