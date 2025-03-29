package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.SchemaTransformationType;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * The SumOperator implements the GROUP BY operation with SUM aggregation in SQL.
 * It is a blocking operator that reads all tuples from its child, groups them
 * by the specified columns, and calculates SUM aggregates for each group.
 */
public class SumOperator extends Operator {

    // Grouping columns and their resolved indices
    private List<Column> groupByColumns;
    private List<Integer> groupByIndices;

    // SUM aggregate expressions and their evaluators
    private List<Expression> sumExpressions;
    private List<ExpressionEvaluator> evaluators;

    // Output columns (group by columns that should be in the output)
    private List<Column> outputColumns;
    private List<Integer> outputIndices;

    // Map of groups and their aggregate values
    private Map<List<Integer>, List<Integer>> groupAggregates;

    // Iterator for returning grouped results
    private Iterator<Map.Entry<List<Integer>, List<Integer>>> resultIterator;

    // Flag to track if all input has been processed
    private boolean processed;

    private String intermediateSchemaId;
    private boolean schemaRegistered = false;

    /**
     * Constructs a SumOperator with the specified child operator, grouping columns,
     * SUM expressions, and output columns.
     *
     * @param child The child operator from which to read tuples
     * @param groupByColumns The columns to group by
     * @param sumExpressions The SUM aggregate expressions
     * @param outputColumns The columns to include in the output (subset of groupByColumns)
     */
    public SumOperator(Operator child, List<Column> groupByColumns, List<Expression> sumExpressions, List<Column> outputColumns) {
        this.child = child;
        this.groupByColumns = groupByColumns;
        this.sumExpressions = sumExpressions;
        this.outputColumns = outputColumns;

        this.groupByIndices = new ArrayList<>();
        this.outputIndices = new ArrayList<>();
        this.evaluators = new ArrayList<>();
        this.groupAggregates = new HashMap<>();
        this.processed = false;

        // Initialize evaluators for each SUM expression
        String schemaId = child.propagateSchemaId();
        for (int i = 0; i < sumExpressions.size(); i++) {
            this.evaluators.add(new ExpressionEvaluator(schemaId));
        }

        // Resolve column indices
        resolveColumnIndices();

        // Register schema during construction
        registerResultSchema();
    }

    /**
     * Resolves the indices of group by columns and output columns.
     */
    private void resolveColumnIndices() {
        String schemaId = propagateSchemaId();

        // Resolve group by column indices
        for (Column column : groupByColumns) {
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            Integer index = DBCatalog.resolveColumnIndex(schemaId, tableName, columnName);
            if (index == null) {
                throw new RuntimeException("Column " + tableName + "." + columnName +
                        " not found in schema " + schemaId);
            }

            groupByIndices.add(index);
        }

        // Resolve output column indices
        for (Column column : outputColumns) {
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            Integer index = DBCatalog.resolveColumnIndex(schemaId, tableName, columnName);
            if (index == null) {
                throw new RuntimeException("Column " + tableName + "." + columnName +
                        " not found in schema " + schemaId);
            }

            outputIndices.add(index);
        }
    }

    /**
     * Returns the next tuple from the grouped and aggregated results.
     * On first call, reads all tuples from the child and computes the aggregates.
     *
     * @return The next result tuple, or null if no more results
     */
    @Override
    public Tuple getNextTuple() {
        // Process all tuples from child on first call
        if (!processed) {
            processChildTuples();
        }

        // Return the next result if available
        if (resultIterator.hasNext()) {
            Map.Entry<List<Integer>, List<Integer>> entry = resultIterator.next();
            List<Integer> groupKeys = entry.getKey();
            List<Integer> aggregateValues = entry.getValue();

            // Construct the result tuple from output columns and aggregate values
            ArrayList<Integer> resultAttributes = new ArrayList<>();

            // If no GROUP BY, just return aggregate values
            if (groupByColumns.isEmpty()) {
                resultAttributes.addAll(aggregateValues);
                tupleCounter++;
                return new Tuple(resultAttributes);
            }

            // Add selected group by columns to the result
            for (Integer outputIndex : outputIndices) {
                int groupKeyIndex = groupByIndices.indexOf(outputIndex);
                if (groupKeyIndex != -1) {
                    resultAttributes.add(groupKeys.get(groupKeyIndex));
                } else {
                    throw new RuntimeException("Output column not found in group by columns");
                }
            }

            // Add aggregate values to the result
            resultAttributes.addAll(aggregateValues);

            tupleCounter++;
            return new Tuple(resultAttributes);
        }

        return null;
    }

    /**
     * Processes all tuples from the child operator, groups them, and computes aggregates.
     */
    private void processChildTuples() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            // Extract group key values (empty list if no grouping)
            List<Integer> groupKey = new ArrayList<>();
            for (Integer index : groupByIndices) {
                groupKey.add(tuple.getAttribute(index));
            }

            // Get or create aggregates for this group
            List<Integer> aggregates = groupAggregates.getOrDefault(groupKey, null);
            if (aggregates == null) {
                aggregates = new ArrayList<>(Collections.nCopies(sumExpressions.size(), 0));
                groupAggregates.put(groupKey, aggregates);
            }

            // Update aggregate values for this group
            for (int i = 0; i < sumExpressions.size(); i++) {
                Expression sumExpr = sumExpressions.get(i);
                ExpressionEvaluator evaluator = evaluators.get(i);

                if (sumExpr instanceof Function) {
                    Function function = (Function) sumExpr;
                    if ("SUM".equalsIgnoreCase(function.getName())) {
                        Expression innerExpr = (Expression) function.getParameters().get(0);
                        // Evaluate the expression for this tuple
                        int value = evaluator.evaluateValue(innerExpr, tuple);
                        // Add to the current aggregate value
                        aggregates.set(i, aggregates.get(i) + value);
                    }
                }
            }
        }

        // Initialize iterator for returning results
        resultIterator = groupAggregates.entrySet().iterator();
        processed = true;

        // Register our schema for downstream operators
        registerResultSchema();
    }

    /**
     * Resets the operator to its initial state.
     * Clears all processed aggregates and resets the child operator.
     */
    @Override
    public void reset() {
        child.reset();
        groupAggregates.clear();
        processed = false;
    }

    /**
     * Propagates the table name from the child operator.
     *
     * @return The table name from the child operator
     */
    @Override
    public String propagateTableName() {
        return child.propagateTableName();
    }

    /**
     * Propagates the schema ID from the child operator.
     *
     * @return The schema ID from the child operator
     */
    @Override
    public String propagateSchemaId() {
        // If we've registered our schema, return its ID
        if (schemaRegistered) {
            return intermediateSchemaId;
        }
        // Otherwise, return the child's schema ID
        return child.propagateSchemaId();
    }


    // New method to register the result schema

    private void registerResultSchema() {
        if (schemaRegistered) {
            return;
        }

        // Create a new schema mapping that describes our output structure
        Map<String, Integer> resultSchema = new HashMap<>();
        Map<String, String> transformationDetails = new HashMap<>();

        int colIndex = 0;

        // First, add the output columns from GROUP BY
        if (!groupByColumns.isEmpty()) {
            for (Column col : outputColumns) {
                String tableName = col.getTable().getName();
                String columnName = col.getColumnName().toLowerCase();
                String key = tableName + "." + columnName;

                resultSchema.put(key, colIndex);

                // Record source information
                String sourceSchemaId = child.propagateSchemaId();
                Integer sourceIndex = DBCatalog.resolveColumnIndex(sourceSchemaId, tableName, columnName);
                transformationDetails.put(key, "group_by:" + sourceIndex);

                colIndex++;
            }
        }

        // Then add positions for the SUM aggregates
        for (int i = 0; i < sumExpressions.size(); i++) {
            Expression sumExpr = sumExpressions.get(i);
            String aggregateKey;

            if (sumExpr instanceof Function) {
                Function function = (Function) sumExpr;
                Expression innerExpr = (Expression) function.getParameters().get(0);

                // Use a naming convention that can be matched in ORDER BY
                if (innerExpr instanceof Column) {
                    Column col = (Column) innerExpr;
                    String tableName = col.getTable().getName();
                    String columnName = col.getColumnName().toLowerCase();
                    aggregateKey = "SUM(" + tableName + "." + columnName + ")";
                } else {
                    aggregateKey = "SUM(" + i + ")";
                }

                resultSchema.put(aggregateKey, colIndex);
                transformationDetails.put(aggregateKey, "aggregate:" + i);
                colIndex++;
            }
        }

        // Register this schema with DBCatalog, including transformation details
        intermediateSchemaId = DBCatalog.getInstance().registerSchemaWithTransformation(
                resultSchema,
                child.propagateSchemaId(),
                SchemaTransformationType.AGGREGATION,
                transformationDetails
        );

        schemaRegistered = true;
    }
}