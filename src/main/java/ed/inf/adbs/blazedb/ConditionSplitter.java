package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

public class ConditionSplitter extends ExpressionVisitorAdapter {
    private String outerSchemaId;
    private String innerSchemaId;

    private List<Expression> outerConditions = new ArrayList<>();
    private List<Expression> innerConditions = new ArrayList<>();
    private List<Expression> joinConditions = new ArrayList<>();

    public ConditionSplitter(String outerSchemaId, String innerSchemaId) {
        this.outerSchemaId = outerSchemaId;
        this.innerSchemaId = innerSchemaId;
    }

    @Override
    public void visit(AndExpression andExpression) {
        // Process left and right separately
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        processComparison(equalsTo);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        processComparison(notEqualsTo);
    }

    // Add other comparison operator visits similarly

    private void processComparison(BinaryExpression expr) {
        // Extract tables referenced in left and right
        Set<String> leftTables = getReferencedTables(expr.getLeftExpression());
        Set<String> rightTables = getReferencedTables(expr.getRightExpression());

        // Determine if this is an outer, inner, or join condition
        boolean usesOuterOnly = referencesSchemaOnly(leftTables, rightTables, outerSchemaId);
        boolean usesInnerOnly = referencesSchemaOnly(leftTables, rightTables, innerSchemaId);

        if (usesOuterOnly) {
            outerConditions.add(expr);
        } else if (usesInnerOnly) {
            innerConditions.add(expr);
        } else {
            joinConditions.add(expr);
        }
    }

    private Set<String> getReferencedTables(Expression expr) {
        // Extract table names from an expression
        final Set<String> tables = new HashSet<>();

        if (expr instanceof Column) {
            Column col = (Column) expr;
            if (col.getTable() != null) {
                tables.add(col.getTable().getName());
            }
        } else if (expr instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expr;
            tables.addAll(getReferencedTables(binExpr.getLeftExpression()));
            tables.addAll(getReferencedTables(binExpr.getRightExpression()));
        }

        return tables;
    }

    private boolean referencesSchemaOnly(Set<String> leftTables, Set<String> rightTables, String schemaId) {
        // Check if all tables are from a specific schema
        Set<String> allTables = new HashSet<>(leftTables);
        allTables.addAll(rightTables);

        // Use the schema tracking system to check
        for (String table : allTables) {
            // We need to integrate with schema tracking to check if this table belongs to our schema
            if (!tableInSchema(table, schemaId)) {
                return false;
            }
        }

        return !allTables.isEmpty();
    }

    private boolean tableInSchema(String tableName, String schemaId) {
        // Use the DBCatalog to check if table is in schema
        if (schemaId.startsWith("temp_")) {
            Map<String, Integer> schema = DBCatalog.getInstance().getIntermediateSchema(schemaId);
            if (schema == null) return false;

            // Check if any column key has this table prefix
            for (String key : schema.keySet()) {
                if (key.startsWith(tableName + ".")) {
                    return true;
                }
            }
            return false;
        } else {
            // For base tables, check if the table exists
            return DBCatalog.getInstance().tableExists(tableName);
        }
    }

    // Combine conditions with AND
    private Expression combineConditions(List<Expression> conditions) {
        if (conditions.isEmpty()) return null;

        Expression result = conditions.get(0);
        for (int i = 1; i < conditions.size(); i++) {
            result = new AndExpression(result, conditions.get(i));
        }

        return result;
    }

    public Expression getOuterCondition() {
        return combineConditions(outerConditions);
    }

    public Expression getInnerCondition() {
        return combineConditions(innerConditions);
    }

    public Expression getJoinCondition() {
        return combineConditions(joinConditions);
    }
}