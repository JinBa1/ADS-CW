package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Constants;
import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.SchemaTransformationType;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract class representing a relational operator in BlazeDB, following the iterator model.
 * This class defines the core functionality every operator must provide.
 * Every specific relational operator (e.g., Scan, Select, Project, Join, Sort, Aggregate)
 * extends this abstract class and implements the methods defined here. The relational operators
 * process tuples in a pipeline fashion, allowing results to flow from the leaf operators up to
 * the root operator.
 */
public abstract class Operator {

    // Child operator
    protected Operator child;

    // Counting tuple processed, used for performance evaluation
//    protected int tupleCounter = 0;

    // Schema information
    protected String intermediateSchemaId = null;
    protected boolean schemaRegistered = false;

    /**
     * Retrieves the next tuple from the iterator.
     * @return A Tuple object representing the row of data, or NULL if EOF reached.
     */
    public abstract Tuple getNextTuple();

    /**
     * Resets the iterator to the start.
     */
    public abstract void reset();


    /**
     * Propagates and retrieves the schema ID used to track schema transformations.
     * This method ensures schema consistency throughout the query evaluation. Each operator must appropriately propagate
     * schema identifiers, particularly after transformations like projections and joins.
     *
     * @return A Unique identifier representing the current operator's output schema.
     */
    public abstract String propagateSchemaId();


//    public int getTupleCounter() {
//        return tupleCounter;
//    }
//
//    public void resetTupleCounter() {
//        tupleCounter = 0;
//    }

    /**
     * Get the child of this operator.
     * @return An operator instance that is the child of the current operator.
     */
    public final Operator getChild() {
        return child;
    }

    /**
     * Checks if this operator has a child operator.
     * @return True if this operator has a child, false otherwise.
     */
    public final boolean hasChild() {
        return child != null;
    }

    /**
     * Set the child of this operator.
     * @see ed.inf.adbs.blazedb.QueryPlanOptimizer used during optimisations.
     * @param child An operator instance to be the new child of this operator.
     */
    public void setChild(Operator child) {
        this.child = child;
    }

    /**
     * Register this operator's schema transformation.
     * @see ed.inf.adbs.blazedb.DBCatalog update the information in this class.
     */
    protected abstract void registerSchema() ;


    /**
     * Check if this operator has registered its schema.
     */
    public final void ensureSchemaRegistered() {
        if (!schemaRegistered) {
            registerSchema();
        }
    }

    /**
     * Recursively update the Schema from bottom up for the query plan.
     * To match the changes in query plan after optimisations like projection push down.
     * @see ed.inf.adbs.blazedb.QueryPlanOptimizer used for query optimisations.
     */
    public void updateSchema() {
        // Base implementation: re-register schema and propagate to child

        // Propagate to child if exists
        if (this.hasChild()) {
            this.child.updateSchema();
        }

        this.schemaRegistered = false;

        registerSchema();
    }

    /**
     * Resolves column references to their corresponding indices in the schema.
     * This utility method takes a list of columns and resolves each column to its
     * index position in the specified schema. It can either create a new list of indices
     * or populate an existing list with the resolved indices.
     * @param columns The list of column references to resolve
     * @param schemaId The schema identifier to resolve against
     * @param targetList Optional existing list to populate with resolved indices (will be cleared if not null)
     * @return A list of resolved column indices, either the provided targetList or a new ArrayList
     * @throws RuntimeException If any column cannot be resolved in the specified schema
     */
    protected static List<Integer> resolveColumnIndices(List<Column> columns, String schemaId,
                                                        List<Integer> targetList) {
        List<Integer> indices = targetList != null ? targetList : new ArrayList<>();
        if (targetList != null) {
            targetList.clear();
        }

        for (Column column : columns) {
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            Integer index = DBCatalog.getInstance().resolveColumnWithOrigins(schemaId, tableName, columnName);
            if (index == null) {
                throw new RuntimeException("Column " + tableName + "." + columnName +
                        " not found in schema " + schemaId);
            }

            indices.add(index);
        }

        return indices;
    }

    /**
     * Helper method for operations that preserve schema structure but add details.
     * @param child                 The child operator
     * @param transformationDetails The details of the transformation
     * @return The ID of the registered schema
     */
    protected String registerPassthroughSchema(
            Operator child,
            Map<String, String> transformationDetails) {

        String childSchemaId = child.propagateSchemaId();
        Map<String, Integer> childSchema;

        if (childSchemaId.startsWith(Constants.INTERMEDIATE_SCHEMA_PREFIX)) {
            childSchema = DBCatalog.getInstance().getIntermediateSchema(childSchemaId);
        } else {
            childSchema = DBCatalog.getInstance().getDBSchemata(childSchemaId);
        }

        // Create identical schema structure
        Map<String, Integer> newSchema = new HashMap<>(childSchema);

        // Register with transformation details
        return DBCatalog.getInstance().registerSchemaWithTransformation(
                newSchema,
                childSchemaId,
                SchemaTransformationType.OTHER,
                transformationDetails
        );
    }
}