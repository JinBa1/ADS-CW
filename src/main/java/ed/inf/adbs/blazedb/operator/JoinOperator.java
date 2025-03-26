package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class JoinOperator extends Operator {
    private Operator outerChild;
    private Expression expression;
    private ExpressionEvaluator evaluator;

    private Tuple currentOuterTuple;



    public JoinOperator(Operator outerChild, Operator innerChild, Expression expression) {
        this.child = innerChild;
        this.outerChild = outerChild;
        this.expression = expression;
        currentOuterTuple = null;
        initEvaluator();
    }


    @Override
    public Tuple getNextTuple() {
        if (currentOuterTuple == null) {
            currentOuterTuple = outerChild.getNextTuple();
            if (currentOuterTuple == null) {
                return null;
            }
            child.reset();
        }

        while (true) {
            Tuple innerTuple = child.getNextTuple();
            if (innerTuple == null) {
                currentOuterTuple = outerChild.getNextTuple();
                if (currentOuterTuple == null) {
                    return null;
                }
                child.reset();
                continue;
            }
            Tuple combined = combineTuples(currentOuterTuple, innerTuple);

            if (expression == null || evaluator.evaluate(expression, combined)) {
                return combined;
            }
        }

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
