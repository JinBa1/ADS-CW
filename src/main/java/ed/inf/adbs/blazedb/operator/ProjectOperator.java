package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator {

//    private List<Expression> columns;
    private List<Integer> columns;
//    private ExpressionEvaluator evaluator;

//    public ProjectOperator(Operator child, List<Expression> columns) {
//        this.child = child;
//        this.columns = columns;
//        this.evaluator = new ExpressionEvaluator();
//    }

    public ProjectOperator(Operator child, List<Integer> columns) {
        this.child = child;
        this.columns = columns;
    }

    @Override
    public Tuple getNextTuple() {
        Tuple nextTuple = child.getNextTuple();
        if (nextTuple == null) {
            return null;
        }

        ArrayList<Integer> projectedColumns = new ArrayList<>();

        // Below is for version where column values are evulated on demand
//        for (Expression column : columns) {
//            Integer attribute = evaluator.evaluateValue(column,nextTuple);
//            projectedColumns.add(attribute);
//        }

        // Below is for version where stored column indices
        for (Integer column : columns) {
            projectedColumns.add(nextTuple.getAttribute(column));
        }

        return new Tuple(projectedColumns);
    }

    @Override
    public void reset() {
        child.reset();
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
