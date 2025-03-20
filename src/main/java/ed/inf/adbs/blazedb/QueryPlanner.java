package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.operator.ProjectOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

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
                Table table = (Table) select.getPlainSelect().getFromItem();
                System.out.println("From Item: " + table.getName());


                rootOp = new ScanOperator(table.getName());

                if (existSelectOp(select)) {
                    rootOp = new SelectOperator(rootOp, select.getPlainSelect().getWhere());
                } else if (existProjectOp(select)) {
                    rootOp = new ProjectOperator(rootOp, getProjectCols(select));
                }
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }

        return rootOp;
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
}
