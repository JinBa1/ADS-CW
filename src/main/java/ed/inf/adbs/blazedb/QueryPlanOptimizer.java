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

        rootOp = pushProjectionsDown(rootOp);

        rootOp.updateSchema();
        // Then, combine operators where possible
        rootOp = combineConsecutiveSelects(rootOp);

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

            // First, try to split complex conditions
            List<Expression> splitConditions = splitAndConditions(condition);

            if (splitConditions.size() > 1) {
                System.out.println("Splitting AND condition into " + splitConditions.size() + " parts: " + splitConditions);

                // Get the underlying child
                Operator baseChild = selectOp.getChild();

                // Process each condition individually
                for (Expression splitCondition : splitConditions) {
                    System.out.println("Processing split condition: " + splitCondition);

                    // Create a temporary SelectOperator for this condition
                    SelectOperator tempSelect = new SelectOperator(baseChild, splitCondition);

                    // Try to push it down
                    Operator optimizedOp = pushSelectionsDown(tempSelect);

                    // If optimization changed the operator type, it was pushed down
                    if (!(optimizedOp instanceof SelectOperator)) {
                        baseChild = optimizedOp;
                    } else {
                        // Couldn't push this selection down, keep it above the result so far
                        baseChild = optimizedOp;
                    }
                }

                return baseChild;
            }

            // Try to push the single selection down
            if (selectOp.getChild() instanceof JoinOperator) {
                JoinOperator joinOp = (JoinOperator) selectOp.getChild();

                // Analyze the condition
                Set<String> tablesInCondition = extractTableNames(condition);
                String outerSchemaId = joinOp.getOuterChild().propagateSchemaId();
                String innerSchemaId = joinOp.getChild().propagateSchemaId();
                Set<String> outerTables = getTablesInSchema(outerSchemaId);
                Set<String> innerTables = getTablesInSchema(innerSchemaId);

                System.out.println("OPTIMIZER: Analyzing condition " + condition);
                System.out.println("OPTIMIZER: Tables in condition: " + tablesInCondition);
                System.out.println("OPTIMIZER: Outer schema tables: " + outerTables);
                System.out.println("OPTIMIZER: Inner schema tables: " + innerTables);

                // Check if condition applies to only outer child
                if (outerTables.containsAll(tablesInCondition)) {
                    System.out.println("OPTIMIZER: Pushing selection to outer child: " + condition);
                    joinOp.setOuterChild(new SelectOperator(joinOp.getOuterChild(), condition));
                    return joinOp;
                }

                // Check if condition applies to only inner child
                if (innerTables.containsAll(tablesInCondition)) {
                    System.out.println("OPTIMIZER: Pushing selection to inner child: " + condition);
                    joinOp.setChild(new SelectOperator(joinOp.getChild(), condition));
                    return joinOp;
                }

                // If it's a join condition or can't be pushed down, keep it
                return selectOp;
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

    private static List<Expression> splitAndConditions(Expression expr) {
        List<Expression> result = new ArrayList<>();

        if (expr instanceof AndExpression) {
            AndExpression and = (AndExpression) expr;
            result.addAll(splitAndConditions(and.getLeftExpression()));
            result.addAll(splitAndConditions(and.getRightExpression()));
        } else {
            result.add(expr);
        }

        return result;
    }

    private static Set<String> extractTableNames(Expression expr) {
        final Set<String> tables = new HashSet<>();

        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                if (column.getTable() != null && column.getTable().getName() != null) {
                    tables.add(column.getTable().getName());
                }
            }
        });

        return tables;
    }

    private static Set<String> getTablesInSchema(String schemaId) {
        Set<String> tables = new HashSet<>();

        if (schemaId.startsWith("temp_")) {
            // For intermediate schemas, extract table names from column keys
            Map<String, Integer> schema = DBCatalog.getInstance().getIntermediateSchema(schemaId);
            if (schema != null) {
                for (String key : schema.keySet()) {
                    int dotIndex = key.indexOf('.');
                    if (dotIndex > 0) {
                        tables.add(key.substring(0, dotIndex));
                    }
                }
            }
        } else {
            // For base tables, the schema ID is the table name
            tables.add(schemaId);
        }

        return tables;
    }

    private static boolean isSelectionTrivial(SelectOperator selectOp) {
        Expression condition = selectOp.getCondition();

        // Check for common trivial conditions like 1 = 1
        if (condition instanceof EqualsTo) {
            EqualsTo equals = (EqualsTo) condition;
            if (equals.getLeftExpression() instanceof LongValue &&
                    equals.getRightExpression() instanceof LongValue) {
                LongValue left = (LongValue) equals.getLeftExpression();
                LongValue right = (LongValue) equals.getRightExpression();
                return left.getValue() == right.getValue();
            }
        }

        return false;
    }

    /**
     * New method: Analyzes a query plan and pushes projections down where possible.
     * This reduces the amount of data processed at each operator.
     */
    private static Operator pushProjectionsDown(Operator rootOp) {
        System.out.println("Starting projection pushdown optimization...");

        // Only perform optimization if the root is a ProjectOperator
        if (rootOp instanceof ProjectOperator) {
            ProjectOperator projectOp = (ProjectOperator) rootOp;
            List<Column> projectedColumns = projectOp.getColumns();

            System.out.println("Root is a ProjectOperator with columns: " + projectedColumns);

            // Get the child operator
            Operator child = projectOp.getChild();

            // Collect all required columns through the operator tree
            Set<Column> requiredColumns = collectRequiredColumns(child, new HashSet<>(projectedColumns));

            System.out.println("Required columns for operations: " + requiredColumns);

            // Push the projection as far down as possible
            Operator optimizedChild = pushProjectionDown(child, requiredColumns);

            // Update the project operator's child
            projectOp.setChild(optimizedChild);



            System.out.println("Projection pushdown optimization complete.");
            return projectOp;
        }

        System.out.println("Root is not a ProjectOperator, skipping projection pushdown.");
        return rootOp;
    }

    /**
     * New method: Recursively pushes projections down the operator tree.
     */
    private static Operator pushProjectionDown(Operator op, Set<Column> requiredColumns) {
        if (op == null) {
            return null;
        }

        // Handle different operator types
        if (op instanceof SelectOperator) {
            // For SelectOperator, add any columns used in the selection condition
            SelectOperator selectOp = (SelectOperator) op;
            Expression condition = selectOp.getCondition();

            // Add columns used in the condition to required columns
            ColumnExtractor extractor = new ColumnExtractor();
            condition.accept(extractor);
            requiredColumns.addAll(extractor.getColumns());

            // Recursively push projection down
            Operator optimizedChild = pushProjectionDown(selectOp.getChild(), requiredColumns);
            selectOp.setChild(optimizedChild);

            return selectOp;
        }
        else if (op instanceof JoinOperator) {
            // For JoinOperator, split required columns between left and right children
            JoinOperator joinOp = (JoinOperator) op;

            // First add columns used in join condition
            Expression joinCondition = joinOp.getJoinCondition();
            if (joinCondition != null) {
                ColumnExtractor extractor = new ColumnExtractor();
                joinCondition.accept(extractor);
                requiredColumns.addAll(extractor.getColumns());
            }

            // Split columns by table
            Set<Column> leftColumns = new HashSet<>();
            Set<Column> rightColumns = new HashSet<>();

            String outerSchemaId = joinOp.getOuterChild().propagateSchemaId();
            String innerSchemaId = joinOp.getChild().propagateSchemaId();

            for (Column col : requiredColumns) {
                String tableName = col.getTable().getName();
                Set<String> outerTables = getTablesInSchema(outerSchemaId);
                Set<String> innerTables = getTablesInSchema(innerSchemaId);

                if (outerTables.contains(tableName)) {
                    leftColumns.add(col);
                } else if (innerTables.contains(tableName)) {
                    rightColumns.add(col);
                } else {
                    // If we can't determine, add to both to be safe
                    leftColumns.add(col);
                    rightColumns.add(col);
                }
            }

            // Recursively push down to both children
            Operator optimizedOuterChild = pushProjectionDown(joinOp.getOuterChild(), leftColumns);
            Operator optimizedInnerChild = pushProjectionDown(joinOp.getChild(), rightColumns);

            joinOp.setOuterChild(optimizedOuterChild);
            joinOp.setChild(optimizedInnerChild);

            return joinOp;
        }
        else if (op instanceof ScanOperator) {
            // For ScanOperator, insert a projection if needed
            ScanOperator scanOp = (ScanOperator) op;
            String tableName = scanOp.propagateTableName();

            // Filter columns for this table only
            List<Column> tableColumns = new ArrayList<>();
            for (Column col : requiredColumns) {
                if (col.getTable().getName().equals(tableName)) {
                    tableColumns.add(col);
                }
            }

            // If we're not selecting all columns, add a projection
            Map<String, Integer> tableSchema = DBCatalog.getInstance().getDBSchemata(tableName);
            if (tableColumns.size() < tableSchema.size()) {
                System.out.println("Adding projection at scan level for table " + tableName +
                        " with columns: " + tableColumns);
                return new ProjectOperator(scanOp, tableColumns);
            }

            return scanOp;
        }
        else if (op instanceof DuplicateEliminationOperator) {
            // For duplicate elimination, all projected columns are required
            DuplicateEliminationOperator distinctOp = (DuplicateEliminationOperator) op;
            Operator optimizedChild = pushProjectionDown(distinctOp.getChild(), requiredColumns);
            distinctOp.setChild(optimizedChild);

            return distinctOp;
        }
        else if (op instanceof SortOperator) {
            // For sort, add sort columns to required columns
            SortOperator sortOp = (SortOperator) op;
            // We need to access sort columns here
            // For simplicity, just push down all required columns

            Operator optimizedChild = pushProjectionDown(sortOp.getChild(), requiredColumns);
            sortOp.setChild(optimizedChild);

            return sortOp;
        }
        else if (op instanceof SumOperator) {
            // For SumOperator, add group by and aggregate columns
            SumOperator sumOp = (SumOperator) op;
            // Since we don't have direct access to the sum columns,
            // we'll just push down all required columns

            Operator optimizedChild = pushProjectionDown(sumOp.getChild(), requiredColumns);
            sumOp.setChild(optimizedChild);

            return sumOp;
        }

        // Default case: if operator has a child, recursively optimize it
        if (op.hasChild()) {
            Operator optimizedChild = pushProjectionDown(op.getChild(), requiredColumns);
            op.setChild(optimizedChild);
        }

        return op;
    }

    /**
     * New method: Collects all columns required by an operator subtree.
     */
    private static Set<Column> collectRequiredColumns(Operator op, Set<Column> parentRequiredColumns) {
        Set<Column> requiredColumns = new HashSet<>(parentRequiredColumns);

        if (op instanceof SelectOperator) {
            // Add columns used in selection condition
            SelectOperator selectOp = (SelectOperator) op;
            Expression condition = selectOp.getCondition();

            ColumnExtractor extractor = new ColumnExtractor();
            condition.accept(extractor);
            requiredColumns.addAll(extractor.getColumns());
        }
        else if (op instanceof JoinOperator) {
            // Add columns used in join condition
            JoinOperator joinOp = (JoinOperator) op;
            Expression joinCondition = joinOp.getJoinCondition();

            if (joinCondition != null) {
                ColumnExtractor extractor = new ColumnExtractor();
                joinCondition.accept(extractor);
                requiredColumns.addAll(extractor.getColumns());
            }
        }
        else if (op instanceof SortOperator) {
            // Need all sort columns
            // For now, we'll just keep all columns from parent
        }
        else if (op instanceof SumOperator) {
            // Need all group by and aggregation columns
            // For now, we'll just keep all columns from parent
        }

        return requiredColumns;
    }

}