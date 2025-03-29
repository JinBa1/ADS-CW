package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.SchemaTransformationType;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class JoinOperator extends Operator {
    private Operator outerChild;
    private Expression expression;
    private ExpressionEvaluator evaluator;

    private Tuple currentOuterTuple;




    public JoinOperator(Operator outerChild, Operator innerChild, Expression expression) {
        this.child = innerChild;
        this.outerChild = outerChild;
        this.expression = expression;
        currentOuterTuple = null;


        this.child.ensureSchemaRegistered();
        this.outerChild.ensureSchemaRegistered();

        registerSchema();

        this.evaluator = new ExpressionEvaluator(intermediateSchemaId);
    }


    @Override
    public Tuple getNextTuple() {

        if (currentOuterTuple == null) {
            currentOuterTuple = outerChild.getNextTuple();
//            System.out.println("JoinOperator: got outer tuple: " + (currentOuterTuple== null ? "null" : currentOuterTuple));
            if (currentOuterTuple == null) {
                return null;
            }
            child.reset();
        }


        while (true) {
            Tuple innerTuple = child.getNextTuple();
            System.out.println("DEBUG: Inner tuple: " + (innerTuple == null ? "null" : innerTuple) +
                    ", columns: " + (innerTuple == null ? "n/a" : innerTuple.getTuple().size()));


            if (innerTuple == null) {
                currentOuterTuple = outerChild.getNextTuple();
                if (currentOuterTuple == null) {
                    return null;
                }
                child.reset();
                continue;
            }
            Tuple combined = combineTuples(currentOuterTuple, innerTuple);
//            System.out.println("JoinOperator: evaluating: " + combined);

            System.out.println("DEBUG: Combined tuple: " + combined +
                    ", columns: " + combined.getTuple().size());

            if (expression == null  || evaluator.evaluate(expression, combined)) {
//                System.out.println("JoinOperator: returning combined tuple: " + combined);
                tupleCounter ++;
                return combined;
            }
        }

    }

    @Override
    public void reset() {
        outerChild.reset();
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

    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        ArrayList<Integer> combinedAttributes = new ArrayList<>();
        combinedAttributes.addAll(leftTuple.getTuple());
        combinedAttributes.addAll(rightTuple.getTuple());
        return new Tuple(combinedAttributes);
    }

    public Operator getOuterChild() {
        return outerChild;
    }

    public void setOuterChild(Operator outerChild) {
        this.outerChild = outerChild;
        updateJoinCondition();
    }

    public void setChild(Operator innerChild) {
        this.child = innerChild;
        updateJoinCondition();
    }

    private void updateJoinCondition() {
        if (this.expression == null) return;

        // Update schema ID for the evaluator
        if (this.intermediateSchemaId != null) {
            this.evaluator = new ExpressionEvaluator(intermediateSchemaId);
        }
    }

    @Override
    protected void registerSchema() {
        if (schemaRegistered) return;

        String leftSchemaId = outerChild.propagateSchemaId();
        String rightSchemaId = child.propagateSchemaId();

        // Get source schemas
        Map<String, Integer> leftSchemaMap = getSchemaMap(leftSchemaId);
        Map<String, Integer> rightSchemaMap = getSchemaMap(rightSchemaId);

        System.out.println("DEBUG JOIN: Left schema ID = " + leftSchemaId + ", Right schema ID = " + rightSchemaId);
        System.out.println("DEBUG JOIN: Left schema map: " + leftSchemaMap);
        System.out.println("DEBUG JOIN: Right schema map: " + rightSchemaMap);

        if (leftSchemaMap == null || rightSchemaMap == null) {
            throw new RuntimeException("Could not retrieve schemas for join");
        }

        // Create combined schema
        Map<String, Integer> joinedSchema = new HashMap<>();
        Map<String, String> transformationDetails = new HashMap<>();



        // Calculate left tuple size for offset
        int leftTupleSize = 0;
        for (Integer index : leftSchemaMap.values()) {
            leftTupleSize = Math.max(leftTupleSize, index + 1);
        }

        // Add entries from both sources
        addSchemaEntries(joinedSchema, transformationDetails, leftSchemaMap, leftSchemaId, 0);
        addSchemaEntries(joinedSchema, transformationDetails, rightSchemaMap, rightSchemaId, leftTupleSize);

        // Add join condition information
        if (expression != null) {
            transformationDetails.put("join_condition", expression.toString());
        }

        // Register with transformation details
        intermediateSchemaId = DBCatalog.getInstance().registerSchemaWithTransformation(
                joinedSchema,
                null, // No single parent
                SchemaTransformationType.JOIN,
                transformationDetails
        );

        // Register both parents
        DBCatalog catalog = DBCatalog.getInstance();
        catalog.addParentSchema(intermediateSchemaId, leftSchemaId);
        catalog.addParentSchema(intermediateSchemaId, rightSchemaId);

        System.out.println("DEBUG JOIN: Joined schema: " + joinedSchema);
        System.out.println("DEBUG JOIN: Registered schema ID: " + intermediateSchemaId);

        schemaRegistered = true;

        // Remove the redundant initEvaluator method
    }


    private Map<String, Integer> getSchemaMap(String schemaId) {
        if (schemaId.startsWith("temp_")) {
            return DBCatalog.getInstance().getIntermediateSchema(schemaId);
        } else {
            return DBCatalog.getInstance().getDBSchemata(schemaId);
        }
    }





    private void addSchemaEntries(Map<String, Integer> joinedSchema,
                                  Map<String, String> transformationDetails,
                                  Map<String, Integer> sourceSchema,
                                  String sourceSchemaId,
                                  int offset) {

        // First determine if this is a temp schema that represents a base table
        String originalTableName = null;
        if (sourceSchemaId.startsWith("temp_")) {
            // Try to find original table through parent schemas
            String parentId = DBCatalog.getInstance().getParentSchemaId(sourceSchemaId);
            if (parentId != null && !parentId.startsWith("temp_")) {
                originalTableName = parentId;
            }
        } else {
            // Direct base table
            originalTableName = sourceSchemaId;
        }

        for (Map.Entry<String, Integer> entry : sourceSchema.entrySet()) {
            String columnKey = entry.getKey();
            Integer sourceIndex = entry.getValue();
            Integer targetIndex = sourceIndex + offset;

            // Always add with temp schema prefix for consistency
            if (columnKey.contains(".")) {
                // For columns already with table qualification
                joinedSchema.put(columnKey, targetIndex);

                // Extract the column name part
                String columnName = columnKey.substring(columnKey.indexOf('.') + 1);

                // Also add with original table name if available
                if (originalTableName != null) {
                    joinedSchema.put(originalTableName + "." + columnName, targetIndex);
                }
            } else {
                // For unqualified columns
                joinedSchema.put(sourceSchemaId + "." + columnKey, targetIndex);

                // Also add with original table name if available
                if (originalTableName != null) {
                    joinedSchema.put(originalTableName + "." + columnKey, targetIndex);
                }
            }

            // Record mapping in transformation details
            transformationDetails.put(columnKey, sourceSchemaId + ":" + sourceIndex);
        }
    }



    public Expression getJoinCondition() {
        return expression;
    }

    public void setJoinCondition(Expression expression) {
        this.expression = expression;
    }

    @Override
    public void updateSchema() {
        // Reset schema registration flag
        System.out.println("JOIN: Updating schema from " + this.intermediateSchemaId + " for expression " + this.expression);

        this.schemaRegistered = false;
        // Update children first
        if (this.outerChild != null) {
            this.outerChild.updateSchema();
        }
        if (this.child != null) {
            this.child.updateSchema();
        }

        // Re-register schema based on updated children
        registerSchema();

        // MISSING STEP: Update join condition expressions to match new schema


        // Create a new evaluator with the updated schema
        this.evaluator = new ExpressionEvaluator(intermediateSchemaId);

        System.out.println("JOIN: Updated to schema " + this.intermediateSchemaId);
    }

    // Helper method to find original table name for a schema ID
    private String getOriginalTableForSchema(String schemaId) {
        // Look up original table for this schema
        if (schemaId.startsWith("temp_")) {
            // Could track this during optimization, or infer from parent schemas
            // For simplicity, let's handle common cases
            String parentId = DBCatalog.getInstance().getParentSchemaId(schemaId);
            if (parentId != null && !parentId.startsWith("temp_")) {
                return parentId; // The base table name
            }
        }
        return null;
    }

}
