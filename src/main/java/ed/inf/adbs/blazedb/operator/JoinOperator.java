package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class JoinOperator extends Operator {
    private Operator outerChild;
    private Expression expression;
    private ExpressionEvaluator evaluator;


    public JoinOperator(Operator outerChild, Operator innerChild, Expression expression) {
        this.child = innerChild;
        this.outerChild = outerChild;
        this.expression = expression;
        initEvaluator();
    }


    @Override
    public Tuple getNextTuple() {
        Tuple outerTuple = outerChild.getNextTuple();
        while (outerTuple != null) {
            Tuple innerTuple = child.getNextTuple();
            while (innerTuple != null) {
                Tuple combined = combineTuples(outerTuple, innerTuple);
                if (evaluator.evaluate(expression, combined)) {
                    return combined;
                }
                innerTuple = child.getNextTuple();
            }
            outerTuple = child.getNextTuple();
            child.reset();
        }
        return null;
    }

    @Override
    public void reset() {
        outerChild.reset();
        child.reset();
    }

    @Override
    public String propagateTableName() {
        return child.propagateTableName();
    }

    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        ArrayList<Integer> combinedAttributes = new ArrayList<>();
        combinedAttributes.addAll(leftTuple.getTuple());
        combinedAttributes.addAll(rightTuple.getTuple());
        return new Tuple(combinedAttributes);
    }

    private void initEvaluator() {
        // sample the inner tuple info, then reset
        Tuple innerTuple = child.getNextTuple();
        int tupleSize = innerTuple.getTuple().size();
        String tableName = child.propagateTableName();
        this.evaluator = new ExpressionEvaluator(tupleSize, tableName);
        child.reset();
    }
}
