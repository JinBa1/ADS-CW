package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class JoinOperator extends Operator {
    private Operator outerChild;
    private Expression expression;
    private ExpressionEvaluator evaluator;

    private Tuple currentOuterTuple;

    private String intermediateSchemaId;



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
//            System.out.println("JoinOperator: got outer tuple: " + (currentOuterTuple== null ? "null" : currentOuterTuple));
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
//            System.out.println("JoinOperator: evaluating: " + combined);

            if (expression == null  || evaluator.evaluate(expression, combined)) {
//                System.out.println("JoinOperator: returning combined tuple: " + combined);
                tupleCounter ++;
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

    @Override
    public String propagateSchemaId() {
        return intermediateSchemaId;
    }

    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        ArrayList<Integer> combinedAttributes = new ArrayList<>();
        combinedAttributes.addAll(leftTuple.getTuple());
        combinedAttributes.addAll(rightTuple.getTuple());
        return new Tuple(combinedAttributes);
    }

    private void initEvaluator() {

        String leftSchemaId = outerChild.propagateSchemaId();
        String rightSchemaId = child.propagateSchemaId();
        String rightTableName = child.propagateTableName();

        DBCatalog catalog = DBCatalog.getInstance();
        this.intermediateSchemaId = catalog.registerJoinSchema(leftSchemaId, rightSchemaId, rightTableName);

        if (intermediateSchemaId == null) {
            throw new RuntimeException("Could not register join schema for " + leftSchemaId + " and " + rightSchemaId);
        }

        this.evaluator = new ExpressionEvaluator(intermediateSchemaId);

        child.reset();
        outerChild.reset();

    }

    public Operator getOuterChild() {
        return outerChild;
    }
}
