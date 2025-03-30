package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.FileReader;
import java.util.*;

public class QueryPlanner {

    public static Operator parseStatement(String filename) {
        Operator rootOp = null;
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));

            if (statement != null) {
                Select select = (Select) statement;
                System.out.println("Statement: " + select);
                System.out.println("SELECT items: " + select.getPlainSelect().getSelectItems());
                System.out.println("WHERE expression: " + select.getPlainSelect().getWhere());
                Table firstTable = (Table) select.getPlainSelect().getFromItem();
                System.out.println("From Item: " + firstTable.getName());
                if (existSortOp(select)) {
                    System.out.println("Order by: " + (select.getPlainSelect().getOrderByElements()).get(0).getExpression());
                }
                System.out.println("Distinct: " + select.getPlainSelect().getDistinct());
                if (existGroupByOp(select)) {
                    System.out.println("Group by: " + select.getPlainSelect().getGroupBy().getGroupByExpressions());
                }

                rootOp = new ScanOperator(firstTable.getName());

                if (existJoinOp(select)) {
                        ExpressionPreprocessor preprocessor = new ExpressionPreprocessor();

                        Expression whereExpression = select.getPlainSelect().getWhere();

                        List<Expression> joinExpressions;
                        List<Expression> selectExpressions;

                        if (whereExpression != null) {
                            preprocessor.evaluate(select.getPlainSelect().getWhere());
                            joinExpressions = preprocessor.getJoinExpressions();
                            selectExpressions = preprocessor.getSelectExpressions();
                        } else {
                            joinExpressions = new ArrayList<>();
                            selectExpressions = new ArrayList<>();
                        }


                        List<Table> tables = getTablesInOrder(select);
                        Set<String> joinedTableNames = new HashSet<>();
                        joinedTableNames.add(firstTable.getName()); // the first table in the from clause

                        for (Table table : tables) {
                            Expression joinCondition = findJoinCondition(joinExpressions, joinedTableNames, table);

                            Operator rightOp = new ScanOperator(table.getName());
                            rootOp = new JoinOperator(rootOp, rightOp, joinCondition);

                            joinedTableNames.add(table.getName());

                            System.out.println("++ Join plan created with tables: " + joinedTableNames);
                            System.out.println("   Join condition: " + joinCondition);
                            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                        }

                        if (!selectExpressions.isEmpty()) {
                            rootOp = new SelectOperator(rootOp, combineExpression(selectExpressions));
                            System.out.println("++ Selection needed.");
                            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                        }

                } else if (existSelectOp(select)) {
                    // For queries without joins, this branch remains unchanged
                    rootOp = new SelectOperator(rootOp, select.getPlainSelect().getWhere());
                    System.out.println("++ No joins, but select operator found.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }


                // Handle GROUP BY and SUM before projection
                if (existGroupByOp(select)) {
                    List<Column> groupByColumns = extractGroupByColumns(select);
                    List<Expression> sumExpressions = extractSumExpressions(select);
                    List<Column> outputColumns = extractNonAggregateColumns(select);

                    Set<Column> requiredColumns = getRequiredColumnsForSum(groupByColumns, sumExpressions, select);

                    Operator childOp = rootOp;
                    rootOp = new ProjectOperator(childOp, new ArrayList<>(requiredColumns));
                    System.out.println("++ Project operator added for group by.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());

                    rootOp = new SumOperator(rootOp, groupByColumns, sumExpressions, outputColumns);
                    System.out.println("++ Group by operator with SUM aggregation added.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }
                // Handle SUM without GROUP BY
                else if (existSumAggregate(select)) {
                    List<Column> groupByColumns = new ArrayList<>(); // Empty for no grouping
                    List<Expression> sumExpressions = extractSumExpressions(select);
                    List<Column> outputColumns = new ArrayList<>(); // Empty for no grouping

                    Set<Column> requiredColumns = getRequiredColumnsForSum(groupByColumns, sumExpressions, select);
                    Operator childOp = rootOp;
                    rootOp = new ProjectOperator(childOp, new ArrayList<>(requiredColumns));
                    System.out.println("++ Project operator added for sum.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());


                    rootOp = new SumOperator(rootOp, groupByColumns, sumExpressions, outputColumns);
                    System.out.println("++ SUM aggregation operator added (no grouping).");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }
                // Handle regular projection (only if no GROUP BY or SUM or if SELECT *)
                else if (existProjectOp(select)) {
                    rootOp = new ProjectOperator(rootOp, getProjectCols(select));
                    System.out.println("++ Project operator found.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }

                if (existDistinctOp(select)) {
                    rootOp = new DuplicateEliminationOperator(rootOp);
                    System.out.println("++ Duplicate elimination operator added for DISTINCT.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }

                if (existSortOp(select)) {
                    rootOp = new SortOperator(rootOp, getSortCols(select));
                    System.out.println("++ Sort operator found.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }


        ensureAllSchemasRegistered(rootOp);

        if (Constants.useQueryOptimization) {  // Add this constant
            System.out.println("Applying query plan optimization...");
            System.out.println("Original plan:");
            printQueryPlan(rootOp, 0);

            rootOp = QueryPlanOptimizer.optimize(rootOp);

            System.out.println("Optimized plan:");
            printQueryPlan(rootOp, 0);
        }


        return rootOp;
    }

    private static boolean existGroupByOp(Select select) {
        return select.getPlainSelect().getGroupBy() != null &&
                !select.getPlainSelect().getGroupBy().getGroupByExpressions().isEmpty();
    }

    private static boolean existSumAggregate(Select select) {
        List<SelectItem<?>> selectItems = select.getPlainSelect().getSelectItems();
        for (SelectItem<?> item : selectItems) {
            Expression expr = item.getExpression();
            if (expr instanceof Function) {
                Function function = (Function) expr;
                if ("SUM".equalsIgnoreCase(function.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<Column> extractGroupByColumns(Select select) {
        List<Column> groupByColumns = new ArrayList<>();
        List<Expression> groupByExpressions = select.getPlainSelect().getGroupBy().getGroupByExpressions();

        System.out.println("DEBUG PLANNER: Extracted GROUP BY expressions: " + groupByExpressions);

        for (Expression expr : groupByExpressions) {
            if (expr instanceof Column) {
                Column column = (Column) expr;
                groupByColumns.add(column);
                System.out.println("DEBUG PLANNER: Added GROUP BY column: " +
                        column.getTable().getName() + "." + column.getColumnName());
            } else {
                throw new UnsupportedOperationException("Only column references are supported in GROUP BY");
            }
        }

        return groupByColumns;
    }

    private static List<Expression> extractSumExpressions(Select select) {
        List<Expression> sumExpressions = new ArrayList<>();
        List<SelectItem<?>> selectItems = select.getPlainSelect().getSelectItems();

        for (SelectItem<?> item : selectItems) {
            Expression expr = item.getExpression();
            if (expr instanceof Function) {
                Function function = (Function) expr;
                if ("SUM".equalsIgnoreCase(function.getName())) {
                    sumExpressions.add(function);
                }
            }
        }

        return sumExpressions;
    }

    private static List<Column> extractNonAggregateColumns(Select select) {
        List<Column> nonAggregateColumns = new ArrayList<>();
        List<SelectItem<?>> selectItems = select.getPlainSelect().getSelectItems();

        for (SelectItem<?> item : selectItems) {
            Expression expr = item.getExpression();
            if (expr instanceof Column) {
                nonAggregateColumns.add((Column) expr);
            }
        }

        return nonAggregateColumns;
    }

    private static boolean existDistinctOp(Select select) {
        return select.getPlainSelect().getDistinct() != null;
    }

    private static boolean existSortOp(Select select) {
        return (select.getPlainSelect().getOrderByElements() != null
        && !select.getPlainSelect().getOrderByElements().isEmpty());
    }

    private static boolean existJoinOp(Select select) {
        return (select.getPlainSelect().getJoins() != null && !select.getPlainSelect().getJoins().isEmpty());
    }

    private static boolean existProjectOp(Select select) {
        List<?> selectItems = select.getPlainSelect().getSelectItems();
        boolean exist = true;
        for (Object item : selectItems) {
            Expression exp = ((SelectItem<?>) item).getExpression();
            if (exp instanceof AllColumns) {
                exist = false;
                break;
            }
        }
        return exist;
    }

    private static boolean existSelectOp(Select select) {
        return (select.getPlainSelect().getWhere() != null);
    }

    private static List<Column> getSortCols(Select select) {
        List<OrderByElement> orderByElements = select.getPlainSelect().getOrderByElements();
        List<Column> sortCols = new ArrayList<>();

        for (OrderByElement orderByElement : orderByElements) {
            Expression exp = orderByElement.getExpression();
            if (exp instanceof Column) {

                Column column = (Column) exp;

                sortCols.add(column);
            } else {
                throw new Error("Unexpected item: " + orderByElement + " of type " + orderByElements.getClass());
            }
        }
        return sortCols;
    }

    // maybe merge the two functions since similar
    private static List<Column> getProjectCols(Select select) {
        List<?> selectItems = select.getPlainSelect().getSelectItems();
        List<Column> projectCols = new ArrayList<>();


        for (Object item : selectItems) {
            Expression exp = ((SelectItem<?>) item).getExpression();
            if (exp instanceof Column) {

                Column column = (Column) exp;

                projectCols.add(column);
            } else {
                throw new Error("Unexpected item: " + item + " of type " + item.getClass());
            }
        }
        return projectCols;
    }

    private static Expression findJoinCondition(List<Expression> joinExpressions, Set<String> joinedTableNames, Table rightTable) {
        if (joinExpressions == null || joinExpressions.isEmpty()) {
            return null;
        }

        List<Expression> relevantExpressions = new ArrayList<>();

        for (Expression exp : joinExpressions) {
            Set<String> tableNames = extractTableNames(exp);

            if (tableNames.contains(rightTable.getName())) {
                for (String tableName : tableNames) {
                    if (!tableName.equals(rightTable.getName()) && joinedTableNames.contains(tableName)) {
                        relevantExpressions.add(exp);
                        break;
                    }
                }
            }
        }

        return combineExpression(relevantExpressions);
    }

    private static List<Table> getTablesInOrder(Select select) {
        List<Table> tables = new ArrayList<>();
//        tables.add((Table) select.getPlainSelect().getFromItem()); uncommet to include the first table in from
        for (Join join : select.getPlainSelect().getJoins()) {
            if (!(join.getRightItem() instanceof Table)) {
                throw new UnsupportedOperationException("suppose to be a table");
            }
            Table joinTable = (Table) join.getRightItem();
            tables.add(joinTable);
        }
        return tables;
    }

    private static Set<String> extractTableNames(Expression expression) {
        final Set<String> tableNames = new HashSet<>();

        if (expression instanceof BinaryExpression) {
            Expression left = ((BinaryExpression) expression).getLeftExpression();
            Expression right = ((BinaryExpression) expression).getRightExpression();

            tableNames.addAll(extractTableNames(left));
            tableNames.addAll(extractTableNames(right));
        } else if (expression instanceof Column) {
            Column column = (Column) expression;
            if (column.getTable() != null && column.getTable().getName() != null) {
                tableNames.add(column.getTable().getName());
            }
        }
        // Add other expression types as needed

        return tableNames;
    }

    private static Expression combineExpression(List<Expression> expressions) {
        if (expressions == null || expressions.isEmpty()) {
            return null;
        }

        Expression result = expressions.get(0);

        for (int i = 1; i < expressions.size(); i++) {
            result = new AndExpression(result, expressions.get(i));
        }

        return result;
    }

    private static Map<Set<String>, List<Expression>> categorizeSelectionsByTables(List<Expression> selectExpressions) {
        Map<Set<String>, List<Expression>> categorizedSelections = new HashMap<>();

        for (Expression expr : selectExpressions) {
            Set<String> tables = extractTableNames(expr);
            categorizedSelections.computeIfAbsent(tables, k -> new ArrayList<>()).add(expr);
        }

        return categorizedSelections;
    }

    /**
     * Prints a query plan tree for debugging purposes.
     * @param op The root operator of the plan
     * @param indent The indentation level
     */
    private static void printQueryPlan(Operator op, int indent) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            sb.append("  ");
        }
        sb.append("- ").append(op.getClass().getSimpleName());

        // Add operator-specific details
        if (op instanceof ScanOperator) {
            // Cast and get table name
        } else if (op instanceof SelectOperator) {
            // Cast and get condition
        } // etc.

        System.out.println(sb.toString());

        // Print child operators
        if (op.hasChild()) {
            printQueryPlan(op.getChild(), indent + 1);
        }

        // Handle JoinOperator's outer child
        if (op instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) op;
            printQueryPlan(joinOp.getOuterChild(), indent + 1);
        }
    }



    private static void ensureAllSchemasRegistered(Operator op) {
        if (op == null) return;

        // Force schema registration
        op.ensureSchemaRegistered();

        // Process children
        if (op.hasChild()) {
            ensureAllSchemasRegistered(op.getChild());
        }

        // Special case for JoinOperator
        if (op instanceof JoinOperator) {
            ensureAllSchemasRegistered(((JoinOperator) op).getOuterChild());
        }
    }

    private static Set<Column> getRequiredColumnsForSum(
            List<Column> groupByColumns,
            List<Expression> sumExpressions,
            Select select) {

        Set<Column> requiredColumns = new HashSet<>(groupByColumns);

        // Add columns used in SUM expressions
        for (Expression expr : sumExpressions) {
            if (expr instanceof Function) {
                Function function = (Function) expr;
                if ("SUM".equalsIgnoreCase(function.getName())) {
                    Expression innerExpr = (Expression) function.getParameters().get(0);

                    // Extract columns from the SUM expression
                    ColumnExtractor extractor = new ColumnExtractor();
                    innerExpr.accept(extractor);
                    requiredColumns.addAll(extractor.getColumns());
                }
            }
        }

        // Add columns from the WHERE clause (for join/selection conditions)
        Expression whereExpr = select.getPlainSelect().getWhere();
        if (whereExpr != null) {
            ColumnExtractor extractor = new ColumnExtractor();
            whereExpr.accept(extractor);
            requiredColumns.addAll(extractor.getColumns());
        }

        return requiredColumns;
    }

}
