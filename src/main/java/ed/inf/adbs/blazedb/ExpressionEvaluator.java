package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.Map;
import java.util.Stack;

public class ExpressionEvaluator extends ExpressionVisitorAdapter {

    private Tuple currentTuple;
    private Stack<Boolean> resultStack;
    private Stack<Integer> valueStack;

    private String schemaId;

    public ExpressionEvaluator() {
        this(null);
    }

    public ExpressionEvaluator(String schemaId) {
        resultStack = new Stack<>();
        valueStack = new Stack<>();
        this.schemaId = schemaId;
    }

    public boolean evaluate(Expression expression, Tuple tuple) {
        System.out.println("Evaluating expression with schema ID: " + this.schemaId);
        System.out.println("Actual tuple: " + tuple);

        this.currentTuple = tuple;
        this.resultStack.clear();
        this.valueStack.clear();

        expression.accept(this);

        if (resultStack.isEmpty()) {
            throw new RuntimeException("Expression evaluation did not produce a result");
        }

        boolean result = resultStack.peek();
//        System.out.println("expression: " + expression + "evaluated to: " + result + "\n");

        return resultStack.pop();
    }


    public Integer evaluateValue(Expression expression, Tuple tuple) {
        // may need to check for expression type, but with assignment assumption should be safe
        // what if a logical expressin is given? may want to guard that
        this.currentTuple = tuple;
        this.resultStack.clear();
        this.valueStack.clear();

        expression.accept(this);

        if (valueStack.isEmpty()) {
            throw new RuntimeException("Expression evaluation did not produce a value");
        }

        return valueStack.pop();
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

        Integer colIdx = DBCatalog.smartResolveColumnIndex(schemaId, tableName, columnName);


        System.out.println("DEBUG EVAL: Looking up column " + tableName + "." + columnName +
                " in schema " + schemaId + ", resolved to index: " + colIdx);
        System.out.println("DEBUG EVAL: Current tuple size: " + currentTuple.getTuple().size() +
                ", tuple: " + currentTuple);

        if (colIdx == null) {
            // If not found, try looking for the column by its name only
            // Get all keys in the schema
            Map<String, Integer> schema = getSchemaMap(schemaId);
            if (schema != null) {
                // Try to find any key that ends with ".columnName"
                String columnNameLower = columnName.toLowerCase();
                for (Map.Entry<String, Integer> entry : schema.entrySet()) {
                    String key = entry.getKey();
                    if (key.endsWith("." + columnNameLower)) {
                        colIdx = entry.getValue();
                        break;
                    }
                }
            }
        }

        if (colIdx == null) {
            throw new RuntimeException("Column '" + tableName + "." + columnName +
                    " not found in schema " + schemaId);
        }

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

        if (expression instanceof EqualsTo) {
            boolean result = leftVal == rightVal;
            resultStack.push(result);
        } else if (expression instanceof NotEqualsTo) {
            boolean result = leftVal != rightVal;
            resultStack.push(result);
        } else if (expression instanceof GreaterThan) {
            boolean result = leftVal > rightVal;
            resultStack.push(result);
        } else if (expression instanceof GreaterThanEquals) {
            boolean result = leftVal >= rightVal;
            resultStack.push(result);
        } else if (expression instanceof MinorThan) {
            boolean result = leftVal < rightVal;
            resultStack.push(result);
        } else if (expression instanceof MinorThanEquals) {
            boolean result = leftVal <= rightVal;
            resultStack.push(result);
        } else if (expression instanceof Multiplication) {
            // For multiplication, we calculate the product and push it to valueStack
            // (not resultStack since it's not a boolean result)
            valueStack.push(leftVal * rightVal);
        } else {
            throw new UnsupportedOperationException("Unsupported binary expression: " + expression.getClass().getName());
        }
    }

    private Map<String, Integer> getSchemaMap(String schemaId) {
        if (schemaId == null) return null;
        if (schemaId.startsWith("temp_")) {
            return DBCatalog.getInstance().getIntermediateSchema(schemaId);
        } else {
            return DBCatalog.getInstance().getDBSchemata(schemaId);
        }
    }

}
