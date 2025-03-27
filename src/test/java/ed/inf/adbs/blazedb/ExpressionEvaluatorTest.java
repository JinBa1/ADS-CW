package ed.inf.adbs.blazedb;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class ExpressionEvaluatorTest {

    private static final String TEST_DB_DIR = "src/test/resources/testdb";
    private static final String SCHEMA_FILE = TEST_DB_DIR + "/schema.txt";
    private static final String DATA_DIR = TEST_DB_DIR + "/data";
    private static final String TEST_TABLE = "TestTable";

    private ExpressionEvaluator evaluator;
    private Tuple testTuple;

    @Before
    public void setUp() throws IOException {
        // Create test database directory structure
        Files.createDirectories(Paths.get(DATA_DIR));

        // Create schema file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(SCHEMA_FILE))) {
            writer.write(TEST_TABLE + " A B C D\n");
        }

        // Create test data file with some sample data
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + TEST_TABLE + ".csv"))) {
            writer.write("1, 10, 100, 1000\n");
            writer.write("2, 20, 200, 2000\n");
            writer.write("3, 30, 300, 3000\n");
        }

        // Initialize the database catalog
        DBCatalog.initDBCatalog(TEST_DB_DIR);

        // Create evaluator instance - using table name as schema ID for base tables
        evaluator = new ExpressionEvaluator(TEST_TABLE);

        // Create a test tuple for evaluations
        testTuple = new Tuple(new ArrayList<>(Arrays.asList(1, 10, 100, 1000)));
    }

    @After
    public void tearDown() throws IOException {
        // Clean up test files
        Files.deleteIfExists(Paths.get(DATA_DIR + "/" + TEST_TABLE + ".csv"));
        Files.deleteIfExists(Paths.get(SCHEMA_FILE));
        Files.deleteIfExists(Paths.get(DATA_DIR));
        Files.deleteIfExists(Paths.get(TEST_DB_DIR));
    }

    @Test
    public void testLiteralComparisons() throws Exception {
        // 5 = 5 should be true
        Expression expr1 = CCJSqlParserUtil.parseExpression("5 = 5");
        assertTrue(evaluator.evaluate(expr1, testTuple));

        // 5 != 5 should be false
        Expression expr2 = CCJSqlParserUtil.parseExpression("5 != 5");
        assertFalse(evaluator.evaluate(expr2, testTuple));

        // 5 < 10 should be true
        Expression expr3 = CCJSqlParserUtil.parseExpression("5 < 10");
        assertTrue(evaluator.evaluate(expr3, testTuple));

        // 10 <= 10 should be true
        Expression expr4 = CCJSqlParserUtil.parseExpression("10 <= 10");
        assertTrue(evaluator.evaluate(expr4, testTuple));

        // 15 > 10 should be true
        Expression expr5 = CCJSqlParserUtil.parseExpression("15 > 10");
        assertTrue(evaluator.evaluate(expr5, testTuple));

        // 10 >= 15 should be false
        Expression expr6 = CCJSqlParserUtil.parseExpression("10 >= 15");
        assertFalse(evaluator.evaluate(expr6, testTuple));
    }

    @Test
    public void testColumnReferences() throws Exception {
        // Create column references for testing
        Table table = new Table();
        table.setName(TEST_TABLE);

        // TestTable.A = 1 should be true for our test tuple
        Column colA = new Column();
        colA.setTable(table);
        colA.setColumnName("A");

        EqualsTo equalsExpr = new EqualsTo();
        equalsExpr.setLeftExpression(colA);
        equalsExpr.setRightExpression(new LongValue(1));

        assertTrue(evaluator.evaluate(equalsExpr, testTuple));

        // TestTable.B > 5 should be true for our test tuple
        Column colB = new Column();
        colB.setTable(table);
        colB.setColumnName("B");

        GreaterThan gtExpr = new GreaterThan();
        gtExpr.setLeftExpression(colB);
        gtExpr.setRightExpression(new LongValue(5));

        assertTrue(evaluator.evaluate(gtExpr, testTuple));

        // TestTable.C != 200 should be true for our test tuple
        Column colC = new Column();
        colC.setTable(table);
        colC.setColumnName("C");

        NotEqualsTo neExpr = new NotEqualsTo();
        neExpr.setLeftExpression(colC);
        neExpr.setRightExpression(new LongValue(200));

        assertTrue(evaluator.evaluate(neExpr, testTuple));
    }

    @Test
    public void testColumnToColumnComparisons() throws Exception {
        // Create table and columns
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column colA = new Column();
        colA.setTable(table);
        colA.setColumnName("A");

        Column colB = new Column();
        colB.setTable(table);
        colB.setColumnName("B");

        // TestTable.A < TestTable.B should be true for our test tuple
        MinorThan ltExpr = new MinorThan();
        ltExpr.setLeftExpression(colA);
        ltExpr.setRightExpression(colB);

        assertTrue(evaluator.evaluate(ltExpr, testTuple));

        // TestTable.B != TestTable.C should be true
        NotEqualsTo neExpr = new NotEqualsTo();
        neExpr.setLeftExpression(colB);
        neExpr.setRightExpression(new Column(table, "C"));

        assertTrue(evaluator.evaluate(neExpr, testTuple));
    }

    @Test
    public void testAndExpressions() throws Exception {
        // Test simple AND: (5 = 5) AND (10 > 5)
        Expression expr1 = CCJSqlParserUtil.parseExpression("5 = 5");
        Expression expr2 = CCJSqlParserUtil.parseExpression("10 > 5");

        AndExpression andExpr = new AndExpression(expr1, expr2);
        assertTrue(evaluator.evaluate(andExpr, testTuple));

        // Test AND with false expression: (5 = 6) AND (10 > 5)
        Expression expr3 = CCJSqlParserUtil.parseExpression("5 = 6");

        AndExpression andExpr2 = new AndExpression(expr3, expr2);
        assertFalse(evaluator.evaluate(andExpr2, testTuple));

        // Test complex AND expression from string
        Expression complexExpr = CCJSqlParserUtil.parseExpression("5 = 5 AND 10 > 5 AND 20 >= 15");
        assertTrue(evaluator.evaluate(complexExpr, testTuple));
    }

    @Test
    public void testComplexExpressions() throws Exception {
        // Create a more complex expression with column references and AND
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column colA = new Column(table, "A");
        Column colB = new Column(table, "B");

        // (TestTable.A = 1) AND (TestTable.B > 5)
        EqualsTo equalsExpr = new EqualsTo();
        equalsExpr.setLeftExpression(colA);
        equalsExpr.setRightExpression(new LongValue(1));

        GreaterThan gtExpr = new GreaterThan();
        gtExpr.setLeftExpression(colB);
        gtExpr.setRightExpression(new LongValue(5));

        AndExpression andExpr = new AndExpression(equalsExpr, gtExpr);

        assertTrue(evaluator.evaluate(andExpr, testTuple));

        // Complex expression: TestTable.A = 1 AND TestTable.B > 5 AND TestTable.C = 100
        Expression complexExpr = CCJSqlParserUtil.parseExpression("TestTable.A = 1 AND TestTable.B > 5 AND TestTable.C = 100");
        assertTrue(evaluator.evaluate(complexExpr, testTuple));

        // Should be false: TestTable.A = 1 AND TestTable.B > 50
        Expression falseExpr = CCJSqlParserUtil.parseExpression("TestTable.A = 1 AND TestTable.B > 50");
        assertFalse(evaluator.evaluate(falseExpr, testTuple));
    }

    @Test
    public void testSelectOperator() throws IOException {
        // Create a scan operator
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        try {
            // Create a selection with TestTable.B > 15
            Expression expr = CCJSqlParserUtil.parseExpression("TestTable.B > 15");
            SelectOperator selectOp = new SelectOperator(scanOp, expr);

            // Should return tuples with B > 15 (the 2nd and 3rd tuples)
            Tuple tuple1 = selectOp.getNextTuple();
            assertNotNull("Should return first matching tuple", tuple1);
            assertEquals("Should be 2nd tuple with B=20", Integer.valueOf(20), tuple1.getAttribute(1));

            Tuple tuple2 = selectOp.getNextTuple();
            assertNotNull("Should return second matching tuple", tuple2);
            assertEquals("Should be 3rd tuple with B=30", Integer.valueOf(30), tuple2.getAttribute(1));

            Tuple tuple3 = selectOp.getNextTuple();
            assertNull("Should have no more matching tuples", tuple3);

            // Test reset
            selectOp.reset();
            Tuple afterReset = selectOp.getNextTuple();
            assertNotNull("Should return first matching tuple after reset", afterReset);
            assertEquals("Should match the first tuple", tuple1.getAttribute(0), afterReset.getAttribute(0));

        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        } finally {
            scanOp.close();
        }
    }

    @Test
    public void testSelectOperatorWithNoMatches() throws IOException {
        // Create a scan operator
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        try {
            // Create a selection with a condition that matches no tuples
            Expression expr = CCJSqlParserUtil.parseExpression("TestTable.A > 100");
            SelectOperator selectOp = new SelectOperator(scanOp, expr);

            // Should return no tuples
            Tuple tuple = selectOp.getNextTuple();
            assertNull("Should not find any matching tuples", tuple);

        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        } finally {
            scanOp.close();
        }
    }

    @Test
    public void testSelectOperatorWithComplexCondition() throws IOException {
        // Create a scan operator
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        try {
            // Create a selection with a complex condition
            Expression expr = CCJSqlParserUtil.parseExpression(
                    "TestTable.A < 3 AND TestTable.B >= 10 AND TestTable.C <= 200");
            SelectOperator selectOp = new SelectOperator(scanOp, expr);

            // Should return tuples that match the complex condition (1st and 2nd tuples)
            Tuple tuple1 = selectOp.getNextTuple();
            assertNotNull("Should return first matching tuple", tuple1);
            assertEquals("Should be 1st tuple with A=1", Integer.valueOf(1), tuple1.getAttribute(0));

            Tuple tuple2 = selectOp.getNextTuple();
            assertNotNull("Should return second matching tuple", tuple2);
            assertEquals("Should be 2nd tuple with A=2", Integer.valueOf(2), tuple2.getAttribute(0));

            Tuple tuple3 = selectOp.getNextTuple();
            assertNull("Should have no more matching tuples", tuple3);

        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        } finally {
            scanOp.close();
        }
    }
}