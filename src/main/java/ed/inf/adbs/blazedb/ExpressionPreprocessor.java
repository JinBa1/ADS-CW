package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.LinkedList;
import java.util.Stack;
import java.util.List;

public class ExpressionPreprocessor extends ExpressionVisitorAdapter {

    private Stack<String> tableStack;
    private List<Expression> joinExpressions;
    private List<Expression> selectExpressions;
    private boolean ready;

    public ExpressionPreprocessor() {
        tableStack = new Stack<>();
        joinExpressions = new LinkedList<>();
        selectExpressions = new LinkedList<>();
        ready = false;
    }

    public void evaluate(Expression expression) {
        if (ready) {
            throw new IllegalStateException("JoinsFinder has already evaluated");
        }
        tableStack.clear();
        joinExpressions.clear();
        selectExpressions.clear();

        expression.accept(this);

        ready = true;
    }

    public List<Expression> getJoinExpressions() {
        if (!ready) {
            throw new IllegalStateException("JoinsFinder has not been evaluated");
        }
        return joinExpressions;
    }

    public List<Expression> getSelectExpressions() {
        if (!ready) {
            throw new IllegalStateException("JoinsFinder has not been evaluated");
        }
        return selectExpressions;
    }

    @Override
    public void visit(AndExpression andExpression) {
        // AND expression itself cannot be a single join expression
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(Column column) {
        String tableName = column.getTable().getName();
        if (DBCatalog.getInstance().tableExists(tableName)) {
            String columnName = column.getColumnName();
            if (DBCatalog.getInstance().columnExists(tableName, columnName)) {
                tableStack.push(tableName);
            } else {
                throw new UnsupportedOperationException(String.format("Table %s does not have column %s", tableName, columnName));
            }
        } else {
            throw new UnsupportedOperationException(String.format("Table %s does not exist", tableName));
        }
    }

    @Override
    public void visit(LongValue longValue) {
        tableStack.push(null);
    }

    @Override
    public void visitBinaryExpression(BinaryExpression expression) {
        expression.getLeftExpression().accept(this);
        expression.getRightExpression().accept(this);

        String rightTable = tableStack.pop();
        String leftTable = tableStack.pop();

        if (expression instanceof EqualsTo || expression instanceof NotEqualsTo
        || expression instanceof GreaterThanEquals || expression instanceof GreaterThan
        || expression instanceof MinorThan || expression instanceof MinorThanEquals) {
            if ((rightTable != null && leftTable != null) && (!rightTable.equals(leftTable))) {
                joinExpressions.add(expression);
            } else {
                selectExpressions.add(expression);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported binary expression: " + expression.getClass().getName());
        }

    }

}
