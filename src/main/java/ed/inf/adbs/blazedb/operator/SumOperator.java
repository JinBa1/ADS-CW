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


        // Ensure child schema is registered
        this.child.ensureSchemaRegistered();

        // Register our schema
        registerSchema();

        // Initialize evaluators for each SUM expression
        String schemaId = child.propagateSchemaId();
        for (int i = 0; i < sumExpressions.size(); i++) {
            this.evaluators.add(new ExpressionEvaluator(schemaId));
        }

        // Resolve column indices
        resolveColumnIndices();

    }

    /**
     * Resolves the indices of group by columns and output columns.
     */
    private void resolveColumnIndices() {

        String schemaId = child.propagateSchemaId();
        groupByIndices.clear();
        outputIndices.clear();

        // Debug output to verify column resolution
        System.out.println("DEBUG SUM: Resolving group by columns: " + groupByColumns);

        // Resolve group by column indices
        for (Column column : groupByColumns) {
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            Integer index = DBCatalog.smartResolveColumnIndex(schemaId, tableName, columnName);
            if (index == null) {
                throw new RuntimeException("Column " + tableName + "." + columnName +
                        " not found in schema " + schemaId);
            }

            System.out.println("DEBUG SUM: Group by column " + tableName + "." +
                    columnName + " resolved to index " + index);
            groupByIndices.add(index);
        }


        for (Column column : outputColumns) {
            String tableName = column.getTable().getName();
            String columnName = column.getColumnName();

            Integer index = DBCatalog.smartResolveColumnIndex(schemaId, tableName, columnName);
            if (index == null) {
                throw new RuntimeException("Column " + tableName + "." + columnName +
                        " not found in schema " + schemaId);
            }

            System.out.println("DEBUG SUM: Group by column " + tableName + "." +
                    columnName + " resolved to index " + index);
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
            List<Integer> aggregates = groupAggregates.computeIfAbsent(groupKey, k ->
                    new ArrayList<>(Collections.nCopies(sumExpressions.size(), 0)));

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
        ensureSchemaRegistered();
        return intermediateSchemaId;
    }


    // New method to register the result schem


    @Override
    protected void registerSchema() {
        if (schemaRegistered) return;

        // Create schema for aggregation result
        Map<String, Integer> resultSchema = new HashMap<>();
        Map<String, String> transformationDetails = new HashMap<>();

        int colIndex = 0;
        String childSchemaId = child.propagateSchemaId();

        // Add group by columns
        if (!groupByColumns.isEmpty()) {
            for (Column col : outputColumns) {
                String tableName = col.getTable().getName();
                String columnName = col.getColumnName().toLowerCase();
                String key = tableName + "." + columnName;

                resultSchema.put(key, colIndex);

                // Record source column
                Integer sourceIndex = DBCatalog.smartResolveColumnIndex(childSchemaId, tableName, columnName);
                if (sourceIndex == null) {
                    throw new RuntimeException("Column " + tableName + "." + columnName +
                            " not found in schema " + childSchemaId);
                }
                transformationDetails.put(key, "group_by:" + sourceIndex);

                colIndex++;
            }
        }

        // Add aggregate functions
        for (int i = 0; i < sumExpressions.size(); i++) {
            Expression sumExpr = sumExpressions.get(i);
            if (sumExpr instanceof Function) {
                Function function = (Function) sumExpr;
                String functionName = function.getName();
                ExpressionList params = function.getParameters();
                Expression param = (Expression) params.get(0);

                String aggregateKey;
                if (param instanceof Column) {
                    Column col = (Column) param;
                    String tableName = col.getTable().getName();
                    String columnName = col.getColumnName().toLowerCase();
                    aggregateKey = functionName + "(" + tableName + "." + columnName + ")";
                } else {
                    aggregateKey = functionName + "(" + i + ")";
                }

                resultSchema.put(aggregateKey, colIndex);
                transformationDetails.put(aggregateKey, "aggregate:" + i);
                colIndex++;
            }
        }

        // Register schema
        intermediateSchemaId = DBCatalog.getInstance().registerSchemaWithTransformation(
                resultSchema,
                childSchemaId,
                SchemaTransformationType.AGGREGATION,
                transformationDetails
        );

        schemaRegistered = true;
    }
}