package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator {

//    private List<Expression> columns;
    private List<Column> columns;
    private List<Integer> resolvedIndices;
    private boolean indicesResolved;
//    private ExpressionEvaluator evaluator;

//    public ProjectOperator(Operator child, List<Expression> columns) {
//        this.child = child;
//        this.columns = columns;
//        this.evaluator = new ExpressionEvaluator();
//    }

    public ProjectOperator(Operator child, List<Column> columns) {
        this.child = child;
        this.columns = columns;
        indicesResolved = false;
    }

    @Override
    public Tuple getNextTuple() {
        if (!indicesResolved) {
            resolveColumnIndices();
        }

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
        for (Integer index : resolvedIndices) {
            projectedColumns.add(nextTuple.getAttribute(index));
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

    private void resolveColumnIndices() {
        if (indicesResolved) {
            return;
        }

        String schemaId = propagateSchemaId();
        resolvedIndices = new ArrayList<>();

        for (Column column : columns) {
            // maybe create the static method somewhere for the below duplcates steps
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            Integer index;
            if (schemaId.startsWith("temp_")) {
                index = DBCatalog.getInstance().getIntermediateColumnName(schemaId, tableName, columnName);
            } else {
                index = DBCatalog.getInstance().getDBColumnName(tableName, columnName);
            }

            if (index == null) {
                throw new RuntimeException("Column " + tableName + ", " + columnName + " not found in schema " + schemaId);
            }

            resolvedIndices.add(index);
        }
        indicesResolved = true;
    }

}
