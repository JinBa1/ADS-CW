package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {

    private Expression expression;
    private ExpressionEvaluator evaluator;

    public SelectOperator(Operator child, Expression expression) {
        this.child = child;
        this.expression = expression;
        this.evaluator = new ExpressionEvaluator(propagateSchemaId());
    }

    @Override
    public Tuple getNextTuple() {
        while (true) {
            Tuple nextTuple = child.getNextTuple();
            if (nextTuple == null) {
                break; //reached end of table because returned null
            }
            if (evaluator.evaluate(expression, nextTuple)) {
                tupleCounter ++;
                return nextTuple; //expression hold, return this tuple
            }
        }
        return null; //should only invoke when reaching end of table
    }

    @Override
    public void reset() {
        child.reset(); // what else?
    }

    @Override
    public String propagateTableName() {
        return child.propagateTableName();
    }

    @Override
    public String propagateSchemaId() {
        return child.propagateSchemaId();
    }
}
