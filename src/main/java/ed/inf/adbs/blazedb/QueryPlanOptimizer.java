package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

import java.util.List;
import java.util.Map;

/**
 * Optimizes query plans by removing unnecessary operators.
 */
public class QueryPlanOptimizer {

    /**
     * Optimizes a query plan by removing unnecessary operators.
     * @param rootOp The root operator of the query plan to optimize
     * @return The optimized query plan
     */
    public static Operator optimize(Operator rootOp) {
        // Apply various optimization rules
        rootOp = removeUnnecessaryProjects(rootOp);
        rootOp = removeUnnecessarySelects(rootOp);
        rootOp = combineConsecutiveSelects(rootOp);

        // Add more optimization rules as needed

        return rootOp;
    }

    // Implementation of specific optimization rules follows

    /**
     * Removes ProjectOperators that don't actually project anything (keep all columns).
     * @param op The operator to optimize
     * @return The optimized operator
     */
    private static Operator removeUnnecessaryProjects(Operator op) {
        if (op == null) {
            return null;
        }

        // Recursive case: optimize child operators first
        if (op.hasChild()) {
            Operator optimizedChild = removeUnnecessaryProjects(op.getChild());

            // If this is a ProjectOperator, check if it's necessary
            if (op instanceof ProjectOperator) {
                ProjectOperator projectOp = (ProjectOperator) op;

                // Check if this project is trivial (doesn't eliminate any columns)
                if (isProjectTrivial(projectOp, optimizedChild)) {
                    System.out.println("Optimizer: Removing unnecessary ProjectOperator");
                    return optimizedChild; // Skip this ProjectOperator
                }

                // Non-trivial projection, just update its child
                projectOp.setChild(optimizedChild);
                return projectOp;
            }

            // For other operator types, just update their child
            op.setChild(optimizedChild);
        }

        // Special case for JoinOperator which has two children
        if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            Operator optimizedOuterChild = removeUnnecessaryProjects(joinOp.getOuterChild());
            joinOp.setOuterChild(optimizedOuterChild);
        }

        return op;
    }

    /**
     * Checks if a ProjectOperator doesn't actually reduce the columns.
     * A projection is trivial if it keeps all columns in the same order.
     */
    private static boolean isProjectTrivial(ProjectOperator projectOp, Operator childOp) {
        // For simplicity, we'll check if the projection has the same number of columns
        // as the child operator's schema

        // This is a simplified implementation - ideally you would check
        // if all columns are projected and in the same order

        List<Column> projectedColumns = projectOp.getColumns();

        // If this is a SELECT *, it's likely trivial
        if (projectedColumns.isEmpty()) {
            return true;
        }

        // Get the schema of the child operator
        String childSchemaId = childOp.propagateSchemaId();

        if (childSchemaId.startsWith("temp_")) {
            // If it's an intermediate schema, compare column counts
            // This is a simplification - a more robust implementation would
            // check if all columns are included and in the same order
            Map<String, Integer> childSchema =
                    DBCatalog.getInstance().getIntermediateSchema(childSchemaId);

            return childSchema != null && childSchema.size() == projectedColumns.size();
        } else {
            // For a base table schema
            Map<String, Integer> childSchema =
                    DBCatalog.getInstance().getDBSchemata(childSchemaId);

            return childSchema != null && childSchema.size() == projectedColumns.size();
        }
    }

    /**
     * Removes SelectOperators with conditions that are always true.
     * @param op The operator to optimize
     * @return The optimized operator
     */
    private static Operator removeUnnecessarySelects(Operator op) {
        if (op == null) {
            return null;
        }

        // Recursive case: optimize child operators first
        if (op.hasChild()) {
            Operator optimizedChild = removeUnnecessarySelects(op.getChild());

            // If this is a SelectOperator, check if it's necessary
            if (op instanceof SelectOperator) {
                SelectOperator selectOp = (SelectOperator) op;

                // Check if this selection is trivial (always true)
                if (isSelectionTrivial(selectOp)) {
                    System.out.println("Optimizer: Removing unnecessary SelectOperator");
                    return optimizedChild; // Skip this SelectOperator
                }

                // Non-trivial selection, just update its child
                selectOp.setChild(optimizedChild);
                return selectOp;
            }

            // For other operator types, just update their child
            op.setChild(optimizedChild);
        }

        // Special case for JoinOperator which has two children
        if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            Operator optimizedOuterChild = removeUnnecessarySelects(joinOp.getOuterChild());
            joinOp.setOuterChild(optimizedOuterChild);
        }

        return op;
    }

    /**
     * Checks if a SelectOperator has a condition that is always true.
     */
    private static boolean isSelectionTrivial(SelectOperator selectOp) {
        Expression condition = selectOp.getCondition();

        // Check for common trivial conditions

        // 1 = 1, true, etc.
        if (condition instanceof EqualsTo) {
            EqualsTo equals = (EqualsTo) condition;
            if (equals.getLeftExpression() instanceof LongValue &&
                    equals.getRightExpression() instanceof LongValue) {
                LongValue left = (LongValue) equals.getLeftExpression();
                LongValue right = (LongValue) equals.getRightExpression();
                return left.getValue() == right.getValue();
            }
        }

        // You can add more cases for other trivial conditions

        return false;
    }

    /**
     * Combines consecutive SelectOperators into a single SelectOperator.
     * @param op The operator to optimize
     * @return The optimized operator
     */
    private static Operator combineConsecutiveSelects(Operator op) {
        if (op == null) {
            return null;
        }

        // Recursive case: optimize child operators first
        if (op.hasChild()) {
            Operator optimizedChild = combineConsecutiveSelects(op.getChild());

            // If this is a SelectOperator and its child is also a SelectOperator,
            // combine them
            if (op instanceof SelectOperator && optimizedChild instanceof SelectOperator) {
                SelectOperator parentSelect = (SelectOperator) op;
                SelectOperator childSelect = (SelectOperator) optimizedChild;

                // Combine the two selection conditions
                Expression combinedCondition = combineConditions(
                        parentSelect.getCondition(),
                        childSelect.getCondition()
                );

                System.out.println("Optimizer: Combining consecutive SelectOperators");

                // Create a new SelectOperator with the combined condition
                return new SelectOperator(childSelect.getChild(), combinedCondition);
            }

            // No combination possible, just update the child
            op.setChild(optimizedChild);
        }

        // Special case for JoinOperator which has two children
        if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            Operator optimizedOuterChild = combineConsecutiveSelects(joinOp.getOuterChild());
            joinOp.setOuterChild(optimizedOuterChild);
        }

        return op;
    }

    /**
     * Combines two selection conditions using AND.
     */
    private static Expression combineConditions(Expression cond1, Expression cond2) {
        // Create an AND expression that combines the two conditions
        return new AndExpression(cond1, cond2);
    }
}
