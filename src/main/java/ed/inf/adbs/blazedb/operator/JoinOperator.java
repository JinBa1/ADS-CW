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

    private String intermediateSchemaId;



    public JoinOperator(Operator outerChild, Operator innerChild, Expression expression) {
        this.child = innerChild;
        this.outerChild = outerChild;
        this.expression = expression;
        currentOuterTuple = null;

        registerJoinSchema();
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
        return intermediateSchemaId;
    }

    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        ArrayList<Integer> combinedAttributes = new ArrayList<>();
        combinedAttributes.addAll(leftTuple.getTuple());
        combinedAttributes.addAll(rightTuple.getTuple());
        return new Tuple(combinedAttributes);
    }

    private void initEvaluator() {

        String leftSchemaId = outerChild.propagateSchemaId();
        String rightSchemaId = child.propagateSchemaId();
        String rightTableName = child.propagateTableName();

        DBCatalog catalog = DBCatalog.getInstance();
        this.intermediateSchemaId = catalog.registerJoinSchema(leftSchemaId, rightSchemaId, rightTableName);

        if (intermediateSchemaId == null) {
            throw new RuntimeException("Could not register join schema for " + leftSchemaId + " and " + rightSchemaId);
        }

        this.evaluator = new ExpressionEvaluator(intermediateSchemaId);

        child.reset();
        outerChild.reset();

    }

    public Operator getOuterChild() {
        return outerChild;
    }

    public void setOuterChild(Operator outerChild) {
        this.outerChild = outerChild;
    }


    private void registerJoinSchema() {
        String leftSchemaId = outerChild.propagateSchemaId();
        String rightSchemaId = child.propagateSchemaId();
        String rightTableName = child.propagateTableName();

        // Get the source schemas
        Map<String, Integer> leftSchemaMap = getSchemaMap(leftSchemaId);
        Map<String, Integer> rightSchemaMap = getSchemaMap(rightSchemaId);

        if (leftSchemaMap == null || rightSchemaMap == null) {
            throw new RuntimeException("Could not retrieve schemas for join");
        }

        // Create combined schema (similar to existing code)
        Map<String, Integer> joinedSchema = new HashMap<>();
        Map<String, String> transformationDetails = new HashMap<>();

        // Calculate left tuple size
        int leftTupleSize = calculateTupleSize(leftSchemaMap);

        // Add left schema entries
        addSchemaEntries(joinedSchema, transformationDetails, leftSchemaMap, leftSchemaId, 0);

        // Add right schema entries with offset
        addSchemaEntries(joinedSchema, transformationDetails, rightSchemaMap, rightSchemaId, leftTupleSize);

        // Register with transformation details
        intermediateSchemaId = DBCatalog.getInstance().registerSchemaWithTransformation(
                joinedSchema,
                null, // No single parent for joins
                SchemaTransformationType.JOIN,
                transformationDetails
        );

        // Also register the left and right schemas as "parent" schemas
        DBCatalog catalog = DBCatalog.getInstance();
        catalog.addParentSchema(intermediateSchemaId, leftSchemaId);
        catalog.addParentSchema(intermediateSchemaId, rightSchemaId);
    }

    private Map<String, Integer> getSchemaMap(String schemaId) {
        if (schemaId.startsWith("temp_")) {
            return DBCatalog.getInstance().getIntermediateSchema(schemaId);
        } else {
            return DBCatalog.getInstance().getDBSchemata(schemaId);
        }
    }

    private int calculateTupleSize(Map<String, Integer> schema) {
        int maxIndex = -1;
        for (Integer index : schema.values()) {
            maxIndex = Math.max(maxIndex, index);
        }
        return maxIndex + 1;
    }

    private void addSchemaEntries(Map<String, Integer> joinedSchema,
                                  Map<String, String> transformationDetails,
                                  Map<String, Integer> sourceSchema,
                                  String sourceSchemaId,
                                  int offset) {
        for (Map.Entry<String, Integer> entry : sourceSchema.entrySet()) {
            String key = entry.getKey();
            Integer sourceIndex = entry.getValue();
            Integer targetIndex = sourceIndex + offset;

            joinedSchema.put(key, targetIndex);

            // Record source information
            transformationDetails.put(key, sourceSchemaId + ":" + sourceIndex);
        }
    }
}
