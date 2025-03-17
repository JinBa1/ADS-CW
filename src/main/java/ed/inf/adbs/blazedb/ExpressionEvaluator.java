package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.Stack;

public class ExpressionEvaluator extends ExpressionVisitorAdapter {

    private Tuple currentTuple;
    private Stack<Boolean> resultStack;
    private Stack<Integer> valueStack;

    public ExpressionEvaluator() {
        resultStack = new Stack<>();
        valueStack = new Stack<>();
    }

    public boolean evaluate(Expression expression, Tuple tuple) {
        this.currentTuple = tuple;
        this.resultStack.clear();
        this.valueStack.clear();

        expression.accept(this);

        if (resultStack.isEmpty()) {
            throw new RuntimeException("Expression evaluation did not produce a result");
        }

        return resultStack.pop();
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean leftResult = resultStack.pop();

        if (!leftResult) {
            resultStack.push(false);
            return;
        }

        andExpression.getRightExpression().accept(this);
        boolean rightResult = resultStack.pop();

        resultStack.push(rightResult); // left is always true at this point
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
        String columnName = column.getColumnName();
        int colIdx = DBCatalog.getInstance().getDBColumnName(tableName, columnName);
        valueStack.push(currentTuple.getAttribute(colIdx));
    }

    @Override
    public void visit(LongValue longValue) {
        valueStack.push(Long.valueOf(longValue.getValue()).intValue());
    }

    @Override
    public void visitBinaryExpression(BinaryExpression expression) {
        expression.getLeftExpression().accept(this);
        expression.getRightExpression().accept(this);

        // reverse order for LIFO
        int rightVal = valueStack.pop();
        int leftVal = valueStack.pop();

        boolean result; // default value?

        if (expression instanceof EqualsTo) {
            result = leftVal == rightVal;
        } else if (expression instanceof NotEqualsTo) {
            result = leftVal != rightVal;
        } else if (expression instanceof GreaterThan) {
            result = leftVal > rightVal;
        } else if (expression instanceof GreaterThanEquals) {
            result = leftVal >= rightVal;
        } else if (expression instanceof MinorThan) {
            result = leftVal < rightVal;
        } else if (expression instanceof MinorThanEquals) {
            result = leftVal <= rightVal;
        } else {
            throw new UnsupportedOperationException("Unsupported binary expression: " + expression.getClass().getName());
        }

        resultStack.push(result);
    }


}
