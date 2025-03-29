package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

/**
 * The abstract Operator class for the iterator model.
 *
 * Feel free to modify this class, but must keep getNextTuple() and reset()
 */
public abstract class Operator {

    protected Operator child;

    protected int tupleCounter = 0;

    // Add common schema fields
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

    public abstract String propagateTableName();

    public abstract String propagateSchemaId();

    public int getTupleCounter() {
        return tupleCounter;
    }

    public void resetTupleCounter() {
        tupleCounter = 0;
    }

    public Operator getChild() {
        return child;
    }

    public boolean hasChild() {
        return child != null;
    }

    public void setChild(Operator child) {
        this.child = child;
    }

    // NEW: Register schema during plan construction
    protected void registerSchema() {
        // Default implementation does nothing
        // Override in operators that transform schemas
    }

    // Helper method to ensure schema is registered
    public final void ensureSchemaRegistered() {
        if (!schemaRegistered) {
            registerSchema();
        }
    }
}