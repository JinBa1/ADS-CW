package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
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
    private String intermediateSchemaId; // Add this field
    private boolean schemaRegistered = false; // Add this field

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
        // Only return our intermediate schema if it's been registered
        if (schemaRegistered) {
            return intermediateSchemaId;
        }
        // Otherwise, return the child's schema (used by resolveColumnIndices)
        return child.propagateSchemaId();
    }

    private void resolveColumnIndices() {
        if (indicesResolved) {
            return;
        }

        // Important: Use child.propagateSchemaId() directly here to avoid recursion
        String schemaId = child.propagateSchemaId();
        resolvedIndices = new ArrayList<>();

        for (Column column : columns) {
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            // Debug print to see what we're looking for
            System.out.println("ProjectOperator resolving: " + tableName + "." + columnName +
                    " in schema " + schemaId);

            Integer index = DBCatalog.resolveColumnIndex(schemaId, tableName, columnName);
            if (index == null) {
                // Print available columns to help debugging
                Map<String, Integer> schemaMap = DBCatalog.getInstance().getDBSchemata(tableName);
                System.out.println("Available columns in schema " + tableName + ": " +
                        (schemaMap != null ? schemaMap.keySet() : "schema not found"));

                throw new RuntimeException("Column " + tableName + "." + columnName +
                        " not found in schema " + schemaId);
            }

            resolvedIndices.add(index);
        }

        indicesResolved = true;

        // Once indices are resolved, register our projected schema
        registerProjectSchema();
    }

    // New method to register the projected schema
    private void registerProjectSchema() {
        if (schemaRegistered) {
            return;
        }

        // Create a new mapping for the projected columns
        Map<String, Integer> projectedSchema = new HashMap<>();

        // For each column in our projected output
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName().toLowerCase();
            String key = tableName + "." + columnName;

            // In our projected output, this column will be at position i
            projectedSchema.put(key, i);
        }

        // Register this schema with DBCatalog
        intermediateSchemaId = DBCatalog.getInstance().registerIntermediateSchema(projectedSchema);
        schemaRegistered = true;
    }
}