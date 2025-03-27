package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.TupleComparator;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class SortOperator extends Operator {

    private List<Column> sortColumns;
    private List<Integer> resolvedIndices;
    private List<Tuple> tupleBuffer;
    private boolean buffered;
    private boolean indicesResolved;
    private int currentTupleIndex;
    private TupleComparator comparator;

    public SortOperator(Operator child, List<Column> sortColumns) {
        this.sortColumns = sortColumns;
        this.child = child;
        this.tupleBuffer = new ArrayList<>();
        currentTupleIndex = 0;
        buffered = false;
        indicesResolved = false;
    }

    @Override
    public Tuple getNextTuple() {
        if (!buffered) {
            bufferTuple();
        }

        if (currentTupleIndex >= tupleBuffer.size()) {
            return null;
        }

        Tuple currentTuple = tupleBuffer.get(currentTupleIndex);
        currentTupleIndex += 1;
        return currentTuple;
    }

    @Override
    public void reset() {
        currentTupleIndex = 0;
    }

    @Override
    public String propagateTableName() {
        return child.propagateTableName();
    }

    @Override
    public String propagateSchemaId() {
        return child.propagateSchemaId();
    }

    private void bufferTuple() {
        if (!buffered) {
            Tuple tuple = child.getNextTuple();

            tupleBuffer.add(tuple);

            while (tuple != null) {
                tuple = child.getNextTuple();
                tupleBuffer.add(tuple);
            }

            if (!indicesResolved) {
                resolveColumnIndices();
            }

            comparator = new TupleComparator(resolvedIndices);

            tupleBuffer.sort(comparator);


            buffered = true;
        }
    }

    private void resolveColumnIndices() {
        if (indicesResolved) {
            return;
        }

        String schemaId = propagateSchemaId();
        resolvedIndices = new ArrayList<>();

        for (Column column : sortColumns) {
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
