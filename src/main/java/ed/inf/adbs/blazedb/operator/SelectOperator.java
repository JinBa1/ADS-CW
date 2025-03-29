package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DBCatalog;
import ed.inf.adbs.blazedb.ExpressionEvaluator;
import ed.inf.adbs.blazedb.SchemaTransformationType;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.HashMap;
import java.util.Map;

public class SelectOperator extends Operator {

    private Expression expression;
    private ExpressionEvaluator evaluator;

    public SelectOperator(Operator child, Expression expression) {
        this.child = child;
        this.expression = expression;

        this.child.ensureSchemaRegistered();

        registerSchema();

        this.evaluator = new ExpressionEvaluator(intermediateSchemaId);
    }

    @Override
    public Tuple getNextTuple() {
        while (true) {
            Tuple nextTuple = child.getNextTuple();
            if (nextTuple == null) {
                break; //reached end of table because returned null
            }
            if (evaluator.evaluate(expression, nextTuple)) {
                tupleCounter ++;
                return nextTuple; //expression hold, return this tuple
            }
        }
        return null; //should only invoke when reaching end of table
    }

    @Override
    public void reset() {
        child.reset(); // what else?
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

    public Expression getCondition() {
        return expression;
    }

    @Override
    protected void registerSchema() {
        if (schemaRegistered) return;

        // Selection doesn't change schema structure, but tracks condition
        String childSchemaId = child.propagateSchemaId();
        String tableName = child.propagateTableName();

        // Get source schema
        Map<String, Integer> sourceSchema;
        if (childSchemaId.startsWith("temp_")) {
            sourceSchema = DBCatalog.getInstance().getIntermediateSchema(childSchemaId);
        } else {
            sourceSchema = DBCatalog.getInstance().getDBSchemata(childSchemaId);
        }

        // Clone source schema for selection (structure is identical)
        Map<String, Integer> selectionSchema = new HashMap<>(sourceSchema);

        // Create details about the conditions
        Map<String, String> transformationDetails = new HashMap<>();
        transformationDetails.put("condition", expression.toString());

        // Register with transformation details
        intermediateSchemaId = DBCatalog.getInstance().registerSchemaWithTransformation(
                selectionSchema,
                childSchemaId,
                SchemaTransformationType.OTHER,  // Or a new SELECTION type
                transformationDetails
        );

        schemaRegistered = true;
    }
}
