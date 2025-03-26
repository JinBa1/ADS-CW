package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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


                rootOp = new ScanOperator(firstTable.getName());

                if (existJoinOp(select)) {

                    ExpressionPreprocessor preprocessor = new ExpressionPreprocessor();
                    preprocessor.evaluate(select.getPlainSelect().getWhere());
                    List<Expression> joinExpressions = preprocessor.getJoinExpressions();
                    List<Expression> selectExpressions = preprocessor.getSelectExpressions();

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
                    rootOp = new SelectOperator(rootOp, select.getPlainSelect().getWhere());
                    System.out.println("++ No joins, but select operator found.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }

                if (existProjectOp(select)) {
                    rootOp = new ProjectOperator(rootOp, getProjectCols(select));
                    System.out.println("++ Project operator found.");
                    System.out.println("   Root operator type: " + rootOp.getClass().getSimpleName());
                }
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }


        return rootOp;
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

    private static List<Integer> getProjectCols(Select select) {
        List<?> selectItems = select.getPlainSelect().getSelectItems();
        List<Integer> projectCols = new ArrayList<Integer>();

        for (Object item : selectItems) {
            Expression exp = ((SelectItem<?>) item).getExpression();
            if (exp instanceof Column) {

                Column column = (Column) exp;
                String columnName = column.getColumnName();
                String tableName = column.getTable().getName();

                int colIdx = DBCatalog.getInstance().getDBColumnName(tableName, columnName);
                projectCols.add(colIdx);
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
        Set<String> tableNames = new HashSet<>();

        if (expression instanceof BinaryExpression) {
            Expression[] leftNRight = new Expression[2];
            leftNRight[0] = ((BinaryExpression) expression).getLeftExpression();
            leftNRight[1] = ((BinaryExpression) expression).getRightExpression();

            for (Expression value : leftNRight) {
                if (value instanceof Column) {
                    tableNames.add(((Column) value).getTable().getName());
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported binary expression: " + expression.getClass().getName());
        }

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

}
