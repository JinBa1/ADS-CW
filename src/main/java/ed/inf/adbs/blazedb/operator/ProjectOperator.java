package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.SchemaTransformationType;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectOperator extends Operator {

    private List<Column> columns;
    private List<Integer> resolvedIndices;
    private boolean indicesResolved;



    public ProjectOperator(Operator child, List<Column> columns) {
        this.child = child;
        this.columns = columns;

        this.child.ensureSchemaRegistered();

        registerSchema();

    }

    @Override
    public Tuple getNextTuple() {
        Tuple nextTuple = child.getNextTuple();
        if (nextTuple == null) {
            return null;
        }

        ArrayList<Integer> projectedColumns = new ArrayList<>();
        for (Integer index : resolvedIndices) {
            projectedColumns.add(nextTuple.getAttribute(index));
        }

        tupleCounter++;
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
        ensureSchemaRegistered();
        return intermediateSchemaId;
    }

    private void resolveColumnIndices() {
        if (indicesResolved) return;

        // Use child.propagateSchemaId() directly to avoid recursion
        String schemaId = child.propagateSchemaId();
        resolvedIndices = new ArrayList<>();

        System.out.println("DEBUG PROJECT: Using schema ID: " + schemaId);
        System.out.println("DEBUG PROJECT: Resolving columns: " + columns);

        for (Column column : columns) {
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            Integer index = DBCatalog.smartResolveColumnIndex(schemaId, tableName, columnName);
            if (index == null) {
                throw new RuntimeException("Column " + tableName + "." + columnName +
                        " not found in schema " + schemaId);
            }

            resolvedIndices.add(index);
        }

        indicesResolved = true;
    }

    // New method to register the projected schema
    @Override
    protected void registerSchema() {
        if (schemaRegistered) return;

        // First, resolve column indices if not already done
        if (!indicesResolved) {
            resolveColumnIndices();
        }

        // Create schema for projection result
        Map<String, Integer> projectedSchema = new HashMap<>();
        Map<String, String> transformationDetails = new HashMap<>();

        // For each column in our projected output
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName().toLowerCase();
            String key = tableName + "." + columnName;

            // In our projected output, this column will be at position i
            projectedSchema.put(key, i);

            // Record the source -> target mapping for this column
            String sourceSchemaId = child.propagateSchemaId();
            Integer sourceIndex = resolvedIndices.get(i);
            transformationDetails.put(key, sourceIndex.toString());
        }

        // Register this schema with DBCatalog, including transformation details
        intermediateSchemaId = DBCatalog.getInstance().registerSchemaWithTransformation(
                projectedSchema,
                child.propagateSchemaId(),
                SchemaTransformationType.PROJECTION,
                transformationDetails
        );

        schemaRegistered = true;
    }

    public List<Column> getColumns() {
        return columns;
    }
}