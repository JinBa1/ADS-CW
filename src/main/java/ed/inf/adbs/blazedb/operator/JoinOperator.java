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
            System.out.println("JoinOperator: got outer tuple: " + (currentOuterTuple== null ? "null" : currentOuterTuple));
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
            System.out.println("JoinOperator: evaluating: " + combined);

            if (evaluator.evaluate(expression, combined)) {
                System.out.println("JoinOperator: returning combined tuple: " + combined);
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
        Tuple outerTuple = outerChild.getNextTuple();
        int tupleSize = outerTuple.getTuple().size();
        String tableName = child.propagateTableName();
        System.out.println("tuple Size: " + tupleSize);
        System.out.println("offset table name: " + tableName);
        this.evaluator = new ExpressionEvaluator(tupleSize, tableName);
        child.reset();
        outerChild.reset();
    }
}
