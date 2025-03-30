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

/**
 * The QueryPlanner class translates SQL queries into executable query plans.
 * It parses SQL statements, constructs appropriate operator trees, and applies optimizations.
 * This class is responsible for:
 * 1. Parsing SQL statements from input files
 * 2. Building operator trees with scan, selection, projection, join, and aggregation operators
 * 3. Ensuring proper schema propagation throughout the operator tree
 * 4. Applying query optimizations when enabled
 * The planner follows a bottom-up approach, starting with scan operators for base tables,
 * then adding join, selection, aggregation, and projection operators as needed.
 * It constructs left-deep join trees in accordance with the order of tables in the FROM clause.
 * @see Operator The abstract base class for all query operators
 * @see DBCatalog The catalog with schema information for column resolution
 * @see QueryPlanOptimizer The optimizer for improving constructed query plans
 */
public class QueryPlanner {

    /**
     * Parses an SQL statement from a file and constructs a query plan.
     * This is the main entry point for query processing in BlazeDB.
     * @param filename The path to a file containing a valid SQL query
     * @return The root operator of the constructed query plan, or null if parsing fails
     */
    public static Operator parseStatement(String filename) {
        Operator rootOp = null;
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));

            if (statement != null) {
                Select select = (Select) statement;
                logQueryDetails(select);

                // Create scan operator for the first table
                rootOp = createScanOperator(select);

                // Process joins if they exist
                if (existJoinOp(select)) {
                    rootOp = processJoins(rootOp, select);
                } else if (existSelectOp(select)) {
                    // For queries without joins, add selection directly
                    rootOp = new SelectOperator(rootOp, select.getPlainSelect().getWhere());
//                    System.out.println("++ No joins, but select operator found.");
//                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }

                // Process GROUP BY and aggregation
                rootOp = processGroupByAndAggregation(rootOp, select);

                // Process projection (if needed)
                rootOp = processProjection(rootOp, select);

                // Process DISTINCT and ORDER BY
                rootOp = processDistinctAndOrderBy(rootOp, select);
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }

        // Ensure schemas are properly registered
        ensureAllSchemasRegistered(rootOp);

        // Apply query optimization if enabled
        if (Constants.useQueryOptimization) {
            System.out.println("Applying query plan optimization...");
            System.out.println("Original plan:");
            printQueryPlan(rootOp, 0);

            rootOp = QueryPlanOptimizer.optimize(rootOp);

            System.out.println("Optimized plan:");
            printQueryPlan(rootOp, 0);
        }

        return rootOp;
    }

    /**
     * Logs details about the parsed SQL query for debugging purposes.
     * @param select The SQL SELECT statement to log
     */
    private static void logQueryDetails(Select select) {
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
    }

    /**
     * Creates a scan operator for the first table in the FROM clause.
     * @param select The SQL SELECT statement
     * @return A ScanOperator for the first table
     */
    private static Operator createScanOperator(Select select) {
        Table firstTable = (Table) select.getPlainSelect().getFromItem();
        return new ScanOperator(firstTable.getName());
    }

    /**
     * Processes JOIN operations for a query.
     * This method constructs a left-deep join tree following the order of tables
     * in the FROM clause. It extracts join conditions from the WHERE clause
     * and applies them appropriately to the JOIN operators.
     * @param rootOp The initial operator (scan of the first table)
     * @param select The SELECT statement being processed
     * @return The root operator of the join tree
     */
    private static Operator processJoins(Operator rootOp, Select select) {
        ExpressionPreprocessor preprocessor = new ExpressionPreprocessor();

        Expression whereExpression = select.getPlainSelect().getWhere();

        List<Expression> joinExpressions;
        List<Expression> selectExpressions;

        if (whereExpression != null) {
            preprocessor.evaluate(select.getPlainSelect().getWhere());
            joinExpressions = preprocessor.getJoinExpressions();
            selectExpressions = preprocessor.getSelectExpressions();
        } else {
            // these lists need to be constructed but preprocessor cant take null expression
            // therefore assign empty lists.
            joinExpressions = new ArrayList<>();
            selectExpressions = new ArrayList<>();
        }

        List<Table> tables = getTablesInOrder(select);
        Set<String> joinedTableNames = new HashSet<>();
        joinedTableNames.add(((Table) select.getPlainSelect().getFromItem()).getName()); // the first table in the from clause

        // Build joins in the order specified in the FROM clause
        for (Table table : tables) {
            Expression joinCondition = findJoinCondition(joinExpressions, joinedTableNames, table);

            Operator rightOp = new ScanOperator(table.getName());
            rootOp = new JoinOperator(rootOp, rightOp, joinCondition);

            joinedTableNames.add(table.getName());

//            System.out.println("++ Join plan created with tables: " + joinedTableNames);
//            System.out.println("   Join condition: " + joinCondition);
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
        }

        // Add any remaining selection conditions after joins
        if (!selectExpressions.isEmpty()) {
            rootOp = new SelectOperator(rootOp, combineExpression(selectExpressions));
//            System.out.println("++ Selection needed.");
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
        }

        return rootOp;
    }

    /**
     * Processes GROUP BY and aggregation operations.
     * Handles both queries with explicit GROUP BY and those with SUM aggregates only.
     * @param rootOp The operator tree so far
     * @param select The SELECT statement being processed
     * @return Updated operator tree with aggregation
     */
    private static Operator processGroupByAndAggregation(Operator rootOp, Select select) {
        // Handle GROUP BY with SUM
        if (existGroupByOp(select)) {
            List<Column> groupByColumns = extractGroupByColumns(select);
            List<Expression> sumExpressions = extractSumExpressions(select);
            List<Column> outputColumns = extractNonAggregateColumns(select);

            Set<Column> requiredColumns = getRequiredColumnsForSum(groupByColumns, sumExpressions, select);

            // Project only required columns before aggregation
            Operator childOp = rootOp;
            rootOp = new ProjectOperator(childOp, new ArrayList<>(requiredColumns));
//            System.out.println("++ Project operator added for group by.");
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());

            // Add the SUM operator
            rootOp = new SumOperator(rootOp, groupByColumns, sumExpressions, outputColumns);
//            System.out.println("++ Group by operator with SUM aggregation added.");
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
        }
        // Handle SUM without GROUP BY
        else if (existSumAggregate(select)) {
            List<Column> groupByColumns = new ArrayList<>(); // Empty for no grouping
            List<Expression> sumExpressions = extractSumExpressions(select);
            List<Column> outputColumns = new ArrayList<>(); // Empty for no grouping

            Set<Column> requiredColumns = getRequiredColumnsForSum(groupByColumns, sumExpressions, select);
            Operator childOp = rootOp;
            rootOp = new ProjectOperator(childOp, new ArrayList<>(requiredColumns));
//            System.out.println("++ Project operator added for sum.");
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());

            rootOp = new SumOperator(rootOp, groupByColumns, sumExpressions, outputColumns);
//            System.out.println("++ SUM aggregation operator added (no grouping).");
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
        }

        return rootOp;
    }

    /**
     * Processes projection operations.
     * Adds a ProjectOperator if the query selects specific columns.
     * @param rootOp The operator tree so far
     * @param select The SELECT statement being processed
     * @return Updated operator tree with projection
     */
    private static Operator processProjection(Operator rootOp, Select select) {
        // Add projection if needed (only if not GROUP BY or SUM or if SELECT *)
        if (existProjectOp(select) && !existSumAggregate(select) && !existGroupByOp(select)) {
            rootOp = new ProjectOperator(rootOp, getProjectCols(select));
//            System.out.println("++ Project operator found.");
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
        }

        return rootOp;
    }

    /**
     * Processes DISTINCT and ORDER BY operations.
     * Adds DuplicateEliminationOperator and/or SortOperator as needed.
     * @param rootOp The operator tree so far
     * @param select The SELECT statement being processed
     * @return Updated operator tree with DISTINCT and/or ORDER BY
     */
    private static Operator processDistinctAndOrderBy(Operator rootOp, Select select) {
        // Add DISTINCT if needed
        if (existDistinctOp(select)) {
            rootOp = new DuplicateEliminationOperator(rootOp);
//            System.out.println("++ Duplicate elimination operator added for DISTINCT.");
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
        }

        // Add ORDER BY if needed
        if (existSortOp(select)) {
            rootOp = new SortOperator(rootOp, getSortCols(select));
//            System.out.println("++ Sort operator found.");
//            System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
        }

        return rootOp;
    }

    /**
     * Checks if a GROUP BY clause exists in the query.
     * @param select The SELECT statement to check
     * @return true if a GROUP BY clause exists, false otherwise
     */
    private static boolean existGroupByOp(Select select) {
        return select.getPlainSelect().getGroupBy() != null &&
                select.getPlainSelect().getGroupBy().getGroupByExpressionList() != null &&
                !select.getPlainSelect().getGroupBy().getGroupByExpressionList().isEmpty();
    }

    /**
     * Checks if any SUM aggregates exist in the query.
     * @param select The SELECT statement to check
     * @return true if any SUM aggregates exist, false otherwise
     */
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

    /**
     * Extracts GROUP BY columns from a query.
     * @param select The SELECT statement to extract from
     * @return A list of Column objects representing the GROUP BY columns
     */
    private static List<Column> extractGroupByColumns(Select select) {
        List<Column> groupByColumns = new ArrayList<>();
        List<?> groupByExpressions = select.getPlainSelect().getGroupBy().getGroupByExpressionList();

//        System.out.println("DEBUG PLANNER: Extracted GROUP BY expressions: " + groupByExpressions);

        for (Object obj : groupByExpressions) {
            Expression expr = (Expression) obj;
            if (expr instanceof Column) {
                Column column = (Column) expr;
                groupByColumns.add(column);
//                System.out.println("DEBUG PLANNER: Added GROUP BY column: " +
//                        column.getTable().getName() + "." + column.getColumnName());
            } else {
                throw new UnsupportedOperationException("Only column references are supported in GROUP BY");
            }
        }

        return groupByColumns;
    }

    /**
     * Extracts SUM expressions from a query.
     * @param select The SELECT statement to extract from
     * @return A list of Expression objects representing SUM aggregates
     */
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

    /**
     * Extracts non-aggregate columns from a SELECT list.
     * @param select The SELECT statement to extract from
     * @return A list of Column objects representing non-aggregate columns
     */
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

    /**
     * Checks if a DISTINCT clause exists in the query.
     * @param select The SELECT statement to check
     * @return true if a DISTINCT clause exists, false otherwise
     */
    private static boolean existDistinctOp(Select select) {
        return select.getPlainSelect().getDistinct() != null;
    }

    /**
     * Checks if an ORDER BY clause exists in the query.
     * @param select The SELECT statement to check
     * @return true if an ORDER BY clause exists, false otherwise
     */
    private static boolean existSortOp(Select select) {
        return (select.getPlainSelect().getOrderByElements() != null
                && !select.getPlainSelect().getOrderByElements().isEmpty());
    }

    /**
     * Checks if a JOIN operation is needed in the query.
     * @param select The SELECT statement to check
     * @return true if a JOIN operation is needed, false otherwise
     */
    private static boolean existJoinOp(Select select) {
        return (select.getPlainSelect().getJoins() != null && !select.getPlainSelect().getJoins().isEmpty());
    }

    /**
     * Checks if a projection operation is needed in the query.
     * @param select The SELECT statement to check
     * @return true if a projection is needed, false otherwise
     */
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

    /**
     * Checks if a selection operation is needed in the query.
     * @param select The SELECT statement to check
     * @return true if a selection is needed, false otherwise
     */
    private static boolean existSelectOp(Select select) {
        return (select.getPlainSelect().getWhere() != null);
    }

    /**
     * Extracts ORDER BY columns from a query.
     * @param select The SELECT statement to extract from
     * @return A list of Column objects for sorting
     */
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

    /**
     * Extracts projection columns from a query.
     * @param select The SELECT statement to extract from
     * @return A list of Column objects for projection
     */
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

    /**
     * Finds appropriate join conditions for a table being joined.
     * @param joinExpressions List of all possible join expressions
     * @param joinedTableNames Set of tables already in the join tree
     * @param rightTable The table being joined
     * @return An expression representing the join condition, or null for cross product
     */
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

    /**
     * Gets tables in the order they appear in the FROM clause.
     * @param select The SELECT statement to extract from
     * @return A list of Table objects in FROM clause order
     */
    private static List<Table> getTablesInOrder(Select select) {
        List<Table> tables = new ArrayList<>();
        for (Join join : select.getPlainSelect().getJoins()) {
            if (!(join.getRightItem() instanceof Table)) {
                throw new UnsupportedOperationException("All joined items must be tables");
            }
            Table joinTable = (Table) join.getRightItem();
            tables.add(joinTable);
        }
        return tables;
    }

    /**
     * Extracts table names referenced in an expression.
     * @param expression The expression to analyze
     * @return A set of table names referenced in the expression
     */
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

        return tableNames;
    }

    /**
     * Combines multiple expressions into a single AND expression.
     * @param expressions List of expressions to combine
     * @return A single combined expression, or null if the list is empty
     */
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

    /**
     * Ensures all schemas are properly registered throughout the operator tree.
     * @param op The root operator to check
     */
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

    /**
     * Determines which columns are required for SUM aggregation.
     * Includes group-by columns, columns in SUM expressions, and columns from WHERE clause.
     * @param groupByColumns Columns in the GROUP BY clause
     * @param sumExpressions SUM aggregate expressions
     * @param select The SELECT statement being processed
     * @return A set of all required columns
     */
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
