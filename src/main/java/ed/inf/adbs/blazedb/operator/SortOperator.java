package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.SchemaTransformationType;
import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.TupleComparator;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        // Ensure child schema is registered
        this.child.ensureSchemaRegistered();

        // Register our schema
        registerSchema();
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
        tupleCounter++;
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
        ensureSchemaRegistered();
        return intermediateSchemaId;
    }

    private void bufferTuple() {
        if (!buffered) {
            Tuple tuple = child.getNextTuple();

            while (tuple != null) {
                tupleBuffer.add(tuple);  // Only add non-null tuples
                tuple = child.getNextTuple();
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

            Integer index = DBCatalog.smartResolveColumnIndex(schemaId, tableName, columnName);
            if (index == null) {
                throw new RuntimeException("Column " + tableName + ", " + columnName + " not found in schema " + schemaId);
            }

            resolvedIndices.add(index);
        }

        indicesResolved = true;

    }

    @Override
    protected void registerSchema() {
        if (schemaRegistered) return;

        // Sort doesn't change schema structure
        String childSchemaId = child.propagateSchemaId();
        Map<String, Integer> childSchema;

        if (childSchemaId.startsWith("temp_")) {
            childSchema = DBCatalog.getInstance().getIntermediateSchema(childSchemaId);
        } else {
            childSchema = DBCatalog.getInstance().getDBSchemata(childSchemaId);
        }

        // Create identical schema structure
        Map<String, Integer> sortSchema = new HashMap<>(childSchema);

        // Add details about sort columns
        Map<String, String> transformationDetails = new HashMap<>();
        for (int i = 0; i < sortColumns.size(); i++) {
            Column col = sortColumns.get(i);
            transformationDetails.put("sort_" + i, col.getTable().getName() +
                    "." + col.getColumnName().toLowerCase());
        }

        // Register with transformation details
        intermediateSchemaId = DBCatalog.getInstance().registerSchemaWithTransformation(
                sortSchema,
                childSchemaId,
                SchemaTransformationType.OTHER,  // Or a new SORT type
                transformationDetails
        );

        schemaRegistered = true;
    }
}
