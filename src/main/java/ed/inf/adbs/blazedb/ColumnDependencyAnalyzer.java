package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

public class ColumnDependencyAnalyzer {

    // Collects all columns referenced in an expression (for WHERE conditions)
    public static Set<Column> getReferencedColumns(Expression expression) {
        if (expression == null) {
            return new HashSet<>();
        }

        ColumnCollector collector = new ColumnCollector();
        expression.accept(collector);
        return collector.getColumns();
    }

    // Gets columns needed for final output
    public static Map<String, Set<Column>> getOutputColumns(PlainSelect plainSelect) {
        Map<String, Set<Column>> tableColumns = new HashMap<>();

        // Handle SELECT *
        if (hasSelectStar(plainSelect)) {
            // For SELECT *, we need all columns from all tables
            Set<String> tableNames = getAllTableNames(plainSelect);
            for (String tableName : tableNames) {
                tableColumns.put(tableName, null); // null means all columns
            }
            return tableColumns;
        }

        // Process explicit column selections
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            if (item.getExpression() instanceof Column) {
                Column col = (Column) item.getExpression();
                String tableName = col.getTable().getName();

                tableColumns.computeIfAbsent(tableName, k -> new HashSet<>())
                        .add(col);
            } else if (item.getExpression() instanceof Function) {
                // Handle SUM expressions
                Function function = (Function) item.getExpression();
                if ("SUM".equalsIgnoreCase(function.getName())) {
                    if (function.getParameters() != null) {
                        for (Object expr : function.getParameters()) {
                            Set<Column> sumColumns = getReferencedColumns((Expression) expr);
                            for (Column col : sumColumns) {
                                String tableName = col.getTable().getName();
                                tableColumns.computeIfAbsent(tableName, k -> new HashSet<>())
                                        .add(col);
                            }
                        }
                    }
                }
            }
        }

        return tableColumns;
    }

    // Gets columns needed for ORDER BY
    public static Map<String, Set<Column>> getOrderByColumns(PlainSelect plainSelect) {
        Map<String, Set<Column>> tableColumns = new HashMap<>();

        if (plainSelect.getOrderByElements() != null) {
            for (OrderByElement element : plainSelect.getOrderByElements()) {
                if (element.getExpression() instanceof Column) {
                    Column col = (Column) element.getExpression();
                    String tableName = col.getTable().getName();

                    tableColumns.computeIfAbsent(tableName, k -> new HashSet<>())
                            .add(col);
                }
            }
        }

        return tableColumns;
    }

    // Gets columns needed for GROUP BY
    public static Map<String, Set<Column>> getGroupByColumns(PlainSelect plainSelect) {
        Map<String, Set<Column>> tableColumns = new HashMap<>();

        if (plainSelect.getGroupBy() != null) {
            for (Object expr : plainSelect.getGroupBy().getGroupByExpressions()) {
                if (expr instanceof Column) {
                    Column col = (Column) expr;
                    String tableName = col.getTable().getName();

                    tableColumns.computeIfAbsent(tableName, k -> new HashSet<>())
                            .add(col);
                }
            }
        }

        return tableColumns;
    }

    // Gets columns needed for join conditions
    public static Map<String, Set<Column>> getJoinColumns(PlainSelect plainSelect) {
        Map<String, Set<Column>> tableColumns = new HashMap<>();
        Set<Column> joinColumns = new HashSet<>();

        // Process the WHERE clause for join conditions
        if (plainSelect.getWhere() != null) {
            ExpressionPreprocessor preprocessor = new ExpressionPreprocessor();
            preprocessor.evaluate(plainSelect.getWhere());
            List<Expression> joinExpressions = preprocessor.getJoinExpressions();

            for (Expression expr : joinExpressions) {
                joinColumns.addAll(getReferencedColumns(expr));
            }
        }

        // Group join columns by table
        for (Column col : joinColumns) {
            String tableName = col.getTable().getName();
            tableColumns.computeIfAbsent(tableName, k -> new HashSet<>())
                    .add(col);
        }

        return tableColumns;
    }

    // Gets columns needed for selection conditions
    public static Map<String, Set<Column>> getSelectionColumns(PlainSelect plainSelect) {
        Map<String, Set<Column>> tableColumns = new HashMap<>();
        Set<Column> selectionColumns = new HashSet<>();

        // Process the WHERE clause for selection conditions
        if (plainSelect.getWhere() != null) {
            ExpressionPreprocessor preprocessor = new ExpressionPreprocessor();
            preprocessor.evaluate(plainSelect.getWhere());
            List<Expression> selectExpressions = preprocessor.getSelectExpressions();

            for (Expression expr : selectExpressions) {
                selectionColumns.addAll(getReferencedColumns(expr));
            }
        }

        // Group selection columns by table
        for (Column col : selectionColumns) {
            String tableName = col.getTable().getName();
            tableColumns.computeIfAbsent(tableName, k -> new HashSet<>())
                    .add(col);
        }

        return tableColumns;
    }

    // Merges multiple column requirements together
    public static Map<String, Set<Column>> mergeColumnRequirements(List<Map<String, Set<Column>>> requirements) {
        Map<String, Set<Column>> merged = new HashMap<>();

        for (Map<String, Set<Column>> req : requirements) {
            for (Map.Entry<String, Set<Column>> entry : req.entrySet()) {
                String tableName = entry.getKey();
                Set<Column> columns = entry.getValue();

                if (columns == null) {
                    // If any requirement needs all columns, mark this table as needing all columns
                    merged.put(tableName, null);
                } else if (merged.get(tableName) != null) {
                    // Merge the column requirements
                    merged.computeIfAbsent(tableName, k -> new HashSet<>())
                            .addAll(columns);
                }
            }
        }

        return merged;
    }

    private static boolean hasSelectStar(PlainSelect plainSelect) {
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            if (item.getExpression() instanceof AllColumns) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> getAllTableNames(PlainSelect plainSelect) {
        Set<String> tableNames = new HashSet<>();

        // Add the main FROM table
        tableNames.add(((Table) plainSelect.getFromItem()).getName());

        // Add any JOIN tables
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                if (join.getRightItem() instanceof Table) {
                    tableNames.add(((Table) join.getRightItem()).getName());
                }
            }
        }

        return tableNames;
    }

    // Helper class to collect column references from expressions
    private static class ColumnCollector extends ExpressionVisitorAdapter {
        private final Set<Column> columns = new HashSet<>();

        @Override
        public void visit(Column column) {
            columns.add(column);
        }

        public Set<Column> getColumns() {
            return columns;
        }
    }
}
