package ed.inf.adbs.blazedb;

import java.util.Map;

/**
 * Stores details about a schema transformation that occurs during query processing.
 * This class maintains information about how an operator transforms its input schema
 * into its output schema. It records both the type of transformation (projection,
 * join, aggregation, or other) and specific details about column mappings and
 * transformations. This information is used by the query processor to track column
 * origins and enable correct column resolution during query execution.
 * @see DBCatalog for usage.
 */
public class SchemaTransformation {

    private final SchemaTransformationType type;
    private final Map<String, String> details;

    /**
     * Construct a SchemaTransformation with specified type and details.
     * @param type The type of schema transformation (PROJECTION, JOIN, AGGREGATION, or OTHER).
     * @param details A map containing transformation-specific details, typically mapping
     *                output column keys to their source information.
     */
    public SchemaTransformation(SchemaTransformationType type, Map<String, String> details) {
        this.type = type;
        this.details = details;
    }


    /**
     * Returns the type of this schema transformation.
     * @return The SchemaTransformationType representing the kind of transformation.
     */
    public SchemaTransformationType getType() {
        return type;
    }

    /**
     * Returns the detailed information about this schema transformation.
     * Depending on the transformation type, the details map typically contains:
     * - For PROJECTION: Column-to-index mappings where keys are output column names
     *   and values are source column indices.
     * - For JOIN: Source schema and index information for each output column.
     * - For AGGREGATION: Group-by column references and aggregate expression details.
     * - For OTHER: Operator-specific information such as selection conditions.
     * @return A map containing transformation-specific details.
     */
    public Map<String, String> getDetails() {
        return details;
    }
}