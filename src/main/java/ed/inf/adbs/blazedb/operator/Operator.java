package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

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
}