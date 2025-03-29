package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * Optimizes query plans by removing unnecessary operators and reorganizing
 * the plan to reduce the number of tuples processed while maintaining correctness.
 * This optimizer respects the key requirements:
 * 1. Proper operator implementation with getNextTuple() and reset()
 * 2. Tree-based evaluation model
 * 3. Left-deep join tree that follows the FROM clause ordering
 */
public class QueryPlanOptimizer {

    /**
     * Optimizes a query plan by applying various transformation rules.
     * @param rootOp The root operator of the query plan to optimize
     * @return The optimized query plan
     */
    public static Operator optimize(Operator rootOp) {
        System.out.println("Starting query plan optimization...");

        // Apply optimization rules in a specific order
        // First, remove trivial operators
        rootOp = removeUnnecessaryProjects(rootOp);
        rootOp = removeUnnecessarySelects(rootOp);

        rootOp = pushSelectionsDown(rootOp);
        // Then, combine operators where possible
//        rootOp = combineConsecutiveSelects(rootOp);

        // Lastly, reorder operators to minimize intermediate results
        // Note: we must maintain the left-deep join tree with the original table order

        System.out.println("Query plan optimization complete.");
        return rootOp;
    }

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
     * A projection is trivial if it keeps all columns from its child.
     */
    private static boolean isProjectTrivial(ProjectOperator projectOp, Operator childOp) {
        List<Column> projectedColumns = projectOp.getColumns();

        // If projecting zero columns, it's definitely not trivial
        if (projectedColumns.isEmpty()) {
            return false;
        }

        String childSchemaId = childOp.propagateSchemaId();

        if (childSchemaId.startsWith("temp_")) {
            // For intermediate schemas
            Map<String, Integer> childSchema = DBCatalog.getInstance().getIntermediateSchema(childSchemaId);
            if (childSchema == null) {
                return false;
            }

            // If projecting all columns, check if counts match
            return childSchema.size() == projectedColumns.size();
        } else {
            // For base table schemas
            Map<String, Integer> childSchema = DBCatalog.getInstance().getDBSchemata(childSchemaId);
            if (childSchema == null) {
                return false;
            }

            // If projecting all columns, check if counts match
            return childSchema.size() == projectedColumns.size();
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
                    System.out.println("Optimizer: Removing unnecessary SelectOperator with always-true condition");
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

        // More cases could be added for other types of trivial conditions

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
                Expression combinedCondition = new AndExpression(
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
     * Pushes selection operations down the operator tree to reduce
     * intermediate result sizes as early as possible.
     * This respects the left-deep join tree structure.
     * @param op The operator to optimize
     * @return The optimized operator
     */
    private static Operator pushSelectionsDown(Operator op) {
        if (op == null) return null;

        // Process selections at the current level
        if (op instanceof SelectOperator) {
            SelectOperator selectOp = (SelectOperator) op;
            Expression condition = selectOp.getCondition();

            // Try to push selection down
            if (selectOp.getChild() instanceof JoinOperator) {
                return pushSelectionIntoJoin(selectOp);
            }
            else if (selectOp.getChild() instanceof ProjectOperator) {
                return pushSelectionThroughProjection(selectOp);
            }
        }

        // Recursively process children
        if (op.hasChild()) {
            op.setChild(pushSelectionsDown(op.getChild()));
        }

        // Special case for JoinOperator's outer child
        if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            joinOp.setOuterChild(pushSelectionsDown(joinOp.getOuterChild()));
        }

        return op;
    }

    // Push selection down into a join
    private static Operator pushSelectionIntoJoin(SelectOperator selectOp) {
        JoinOperator joinOp = (JoinOperator) selectOp.getChild();
        Expression condition = selectOp.getCondition();

        // Split the selection condition
        Map<Operator, Expression> splitConditions = splitJoinCondition(
                condition,
                joinOp.getOuterChild(),
                joinOp.getChild()
        );

        // Apply conditions to each child where applicable
        Expression outerCondition = splitConditions.get(joinOp.getOuterChild());
        if (outerCondition != null) {
            joinOp.setOuterChild(new SelectOperator(joinOp.getOuterChild(), outerCondition));
            System.out.println("Pushed selection to outer join child: " + outerCondition);
        }

        Expression innerCondition = splitConditions.get(joinOp.getChild());
        if (innerCondition != null) {
            joinOp.setChild(new SelectOperator(joinOp.getChild(), innerCondition));
            System.out.println("Pushed selection to inner join child: " + innerCondition);
        }

        // Remaining conditions stay with the join
        Expression remainingCondition = splitConditions.get(null);
        if (remainingCondition != null) {
            // Either modify join condition or keep a selection above the join
            if (joinOp.getJoinCondition() != null) {
                // Combine with existing join condition
                Expression combinedCondition = new AndExpression(joinOp.getJoinCondition(), remainingCondition);
                return new SelectOperator(joinOp, remainingCondition);
            } else {
                return new SelectOperator(joinOp, remainingCondition);
            }
        }

        return joinOp;
    }

    // Push selection through a projection if possible
    private static Operator pushSelectionThroughProjection(SelectOperator selectOp) {
        ProjectOperator projectOp = (ProjectOperator) selectOp.getChild();
        Expression condition = selectOp.getCondition();

        // Check if all columns used in the selection are preserved by the projection
        if (canPushSelectionThroughProjection(condition, projectOp)) {
            // Create a new selection below the projection
            Operator newChild = new SelectOperator(projectOp.getChild(), condition);
            projectOp.setChild(newChild);
            System.out.println("Pushed selection through projection");
            return projectOp;
        }

        return selectOp;
    }

    // Split a condition for join selection pushdown using schema information
    private static Map<Operator, Expression> splitJoinCondition(
            Expression condition,
            Operator outerChild,
            Operator innerChild) {

        Map<Operator, Expression> result = new HashMap<>();

        // Get schema IDs for both children
        String outerSchemaId = outerChild.propagateSchemaId();
        String innerSchemaId = innerChild.propagateSchemaId();

        // Split using table references in condition
        ConditionSplitter splitter = new ConditionSplitter(outerSchemaId, innerSchemaId);
        condition.accept(splitter);

        // Get the split conditions
        result.put(outerChild, splitter.getOuterCondition());
        result.put(innerChild, splitter.getInnerCondition());
        result.put(null, splitter.getJoinCondition());

        return result;
    }

    // Helper to check if a selection can be pushed through projection
    private static boolean canPushSelectionThroughProjection(
            Expression condition,
            ProjectOperator projectOp) {

        // Extract columns referenced in the condition
        List<Column> conditionColumns = extractColumns(condition);

        // Get projected columns
        List<Column> projectedColumns = projectOp.getColumns();

        // Check if all condition columns are in the projection
        for (Column condCol : conditionColumns) {
            boolean found = false;
            for (Column projCol : projectedColumns) {
                if (columnsMatch(condCol, projCol)) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }

        return true;
    }

    // Extract columns from an expression
    private static List<Column> extractColumns(Expression expression) {
        ColumnExtractor extractor = new ColumnExtractor();
        expression.accept(extractor);
        return extractor.getColumns();
    }

    // Check if two columns match (considering table and column names)
    private static boolean columnsMatch(Column col1, Column col2) {
        return col1.getColumnName().equalsIgnoreCase(col2.getColumnName()) &&
                col1.getTable().getName().equalsIgnoreCase(col2.getTable().getName());
    }


    /**
     * Gets the set of table names referenced by an operator and its descendants.
     */
    private static Set<String> getReferencedTables(Operator op) {
        Set<String> tables = new HashSet<>();

        // Base table scan
        if (op instanceof ScanOperator) {
            tables.add(op.propagateTableName());
            return tables;
        }

        // JoinOperator combines tables from both children
        if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            tables.addAll(getReferencedTables(joinOp.getOuterChild()));
            tables.addAll(getReferencedTables(joinOp.getChild()));
            return tables;
        }

        // Other operators pass through tables from their child
        if (op.hasChild()) {
            tables.addAll(getReferencedTables(op.getChild()));
        }

        return tables;
    }

    /**
     * Gets the set of table names referenced in an expression.
     */
    private static Set<String> getTablesInExpression(Expression expr) {
        final Set<String> tables = new HashSet<>();

        // Use a visitor to collect table names from the expression
        ExpressionVisitorAdapter visitor = new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                if (column.getTable() != null && column.getTable().getName() != null) {
                    tables.add(column.getTable().getName());
                }
            }
        };

        expr.accept(visitor);
        return tables;
    }

    /**
     * Checks if a selection can be pushed before a projection.
     * This is possible if all columns referenced in the selection
     * are preserved by the projection.
     */
    private static boolean canPushSelectionBeforeProjection(Expression selectCondition, ProjectOperator projectOp) {
        // Get columns referenced in selection condition
        final Set<Column> selectColumns = new HashSet<>();

        ExpressionVisitorAdapter visitor = new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                selectColumns.add(column);
            }
        };

        selectCondition.accept(visitor);

        // Get columns preserved by projection
        List<Column> projectionColumns = projectOp.getColumns();
        Set<Column> projectionColumnSet = new HashSet<>(projectionColumns);

        // Check if all selection columns are in projection
        return projectionColumnSet.containsAll(selectColumns);
    }
}