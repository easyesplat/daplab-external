#include "async_rewrite.hpp"
#include "logical_external_operator.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
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

// Main rewrite function.
unique_ptr<LogicalOperator> RewriteAsyncExternalFlow(unique_ptr<LogicalOperator> op) {
	if (!op) {
		return op;
	}

	// TODO(@charlie) Currently, this is purely checking for a logical filter. Change accordingly.
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter_op = op->Cast<LogicalFilter>();

		bool uses_udf = false;
		std::string stream_endpoint;
		
		for (auto &expr : filter_op.expressions) {
			if (FilterUsesAddressViolatingRows(*expr)) {
				uses_udf = true;
				
				ExpressionIterator::VisitExpressionClass(*expr, ExpressionClass::BOUND_FUNCTION, [&](const Expression &child) {
					auto &func_expr = child.Cast<BoundFunctionExpression>();
					if (func_expr.function.name == UDF_FUNCTION_NAME && !func_expr.children.empty()) {
						auto &last_arg = func_expr.children.back();
						if (last_arg->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
							auto &const_expr = last_arg->Cast<BoundConstantExpression>();
							// The last argument has the `stream_endpoint` value.
							if (const_expr.value.type() == LogicalType::VARCHAR) {
								stream_endpoint = const_expr.value.GetValue<std::string>();
							}
						}
					}
				});
				break;
			}
		}

		// If this filter uses the violating UDF, wrap the entire subtree in a UNION
		if (uses_udf) {
			op->children[0]->ResolveOperatorTypes();
			op->ResolveOperatorTypes();

			vector<LogicalType> filter_types = op->types;
			vector<LogicalType> child_types = op->children[0]->types;
			
			if (child_types.empty()) {
				return op;
			}

			auto external_op = make_uniq<LogicalExternalOperator>(op->estimated_cardinality);
			external_op->PreserveTypes(filter_types);
			if (!stream_endpoint.empty()) {
				external_op->SetStreamEndpoint(stream_endpoint);
			}

			auto union_op = make_uniq<LogicalSetOperation>(0,             
			                                               filter_types.size(),
			                                               std::move(op),          // top (original subtree)
			                                               std::move(external_op), // bottom (external source)
			                                               LogicalOperatorType::LOGICAL_UNION,
			                                               true,  // setop_all (true = UNION ALL - keep duplicates)
			                                               true   // allow_out_of_order
			);

			return union_op;
		}
	}

	for (auto &child : op->children) {
		child = RewriteAsyncExternalFlow(std::move(child));
	}

	return op;
}

} // namespace duckdb
