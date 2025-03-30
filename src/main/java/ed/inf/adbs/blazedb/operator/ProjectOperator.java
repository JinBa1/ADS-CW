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

/**
 * This operator performs projection (π) in relational algebra,
 * returning only a selected subset of columns from each tuple.
 * This operator does not eliminate duplicates (bag semantics).
 * @see Operator
 */
public class ProjectOperator extends Operator {

    private final List<Column> columns; // projected columns
    private List<Integer> resolvedIndices; // indices for the columns
    private boolean indicesResolved;


    /**
     * Construct a project operator that projects selected columns
     * from its child operator.
     * @param child The child operator to read from.
     * @param columns The list of columns to retain.
     */
    public ProjectOperator(Operator child, List<Column> columns) {
        this.child = child;
        this.columns = columns;

        this.child.ensureSchemaRegistered();

        registerSchema();

    }

    /**
     * Retrieves the next tuple from the child, projected to contain only the selected columns.
     * @return A new tuple constructed with only the retaining columns, or null if no such tuple exists.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple nextTuple = child.getNextTuple();
        if (nextTuple == null) {
            return null;
        }

        ArrayList<Integer> projectedColumns = new ArrayList<>(); // construct new tuple from this filtered list.
        for (Integer index : resolvedIndices) {
//            System.out.println("DEBUG PROJ: Using index " + index + " for tuple of size " + nextTuple.getTuple().size());
            projectedColumns.add(nextTuple.getAttribute(index));
        }

        //tupleCounter++;
        return new Tuple(projectedColumns);
    }

    /**
     * Reset the operator adn its child.
     */
    @Override
    public void reset() {
        child.reset();
    }


    /**
     * Returns the schema ID for the projected output, registered as an intermediate schema.
     * @return unique identifier for the intermediate schema created by this operator.
     */
    @Override
    public String propagateSchemaId() {
        ensureSchemaRegistered();
        return intermediateSchemaId;
    }

    /**
     * Resolve the column references into integer indices.
     * Use the integer indices to locate the correct columns being projected
     * since the tuple from the child may not have the original schema.
     */
    private void resolveColumnIndices() {
        if (indicesResolved) return;

        // Use child.propagateSchemaId() directly to avoid recursion
        String schemaId = child.propagateSchemaId();
        resolvedIndices = new ArrayList<>();

//        System.out.println("DEBUG PROJECT: Using schema ID: " + schemaId);
//        System.out.println("DEBUG PROJECT: Resolving columns: " + columns);

        for (Column column : columns) {
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            Integer index = DBCatalog.getInstance().resolveColumnWithOrigins(schemaId, tableName, columnName);
            if (index == null) {
                throw new RuntimeException("Column " + tableName + "." + columnName +
                        " not found in schema " + schemaId);
            }

            resolvedIndices.add(index);
        }

        indicesResolved = true;
    }

    /**
     * Update the projection's schema transformation mapping
     * Help the optimiser to make decisions and other operators to locate the columns.
     * @see DBCatalog for details on schema tracking.
     */
    @Override
    protected void registerSchema() {
        if (schemaRegistered) return;

        // First, resolve column indices if not already done
        resolveColumnIndices();

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

    /**
     * Get the list of columns this project operator retains.
     * @return The list of columns to retain.
     */
    public List<Column> getColumns() {
        return columns;
    }

    /**
     * Recursively update schema from bottom up
     * For project operator, it is crucial to reset the flag.
     */
    @Override
    public void updateSchema() {
//        System.out.println("DEBUG: ProjectOperator updating schema. Current indices: " + resolvedIndices);

        this.indicesResolved = false; // This is crucial!

//        this.schemaRegistered = false;
//        System.out.println("DEBUG: ProjectOperator updated schema. New indices: " + resolvedIndices);
//
//        // Propagate to child
//        if (this.hasChild()) {
//            this.child.updateSchema();
//        }
//
//        registerSchema();
        super.updateSchema();
    }
}