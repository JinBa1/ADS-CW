package ed.inf.adbs.blazedb;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class SelectOperatorTest {

    private static final String TEST_DB_DIR = "src/test/resources/testdb";
    private static final String SCHEMA_FILE = TEST_DB_DIR + "/schema.txt";
    private static final String DATA_DIR = TEST_DB_DIR + "/data";
    private static final String TEST_TABLE = "Student";

    @Before
    public void setUp() throws IOException {
        // Create test database directory structure
        Files.createDirectories(Paths.get(DATA_DIR));

        // Create schema file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(SCHEMA_FILE))) {
            writer.write(TEST_TABLE + " sid name age gpa\n");
        }

        // Create test data file with varied sample data
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + TEST_TABLE + ".csv"))) {
            writer.write("1, 25, 85, 3\n");     // sid=1, name=25 (numeric stand-in), age=85, gpa=3
            writer.write("2, 30, 22, 4\n");     // sid=2, name=30, age=22, gpa=4
            writer.write("3, 35, 19, 2\n");     // sid=3, name=35, age=19, gpa=2
            writer.write("4, 40, 21, 4\n");     // sid=4, name=40, age=21, gpa=4
            writer.write("5, 45, 65, 3\n");     // sid=5, name=45, age=65, gpa=3
        }

        // Initialize the database catalog
        DBCatalog.initDBCatalog(TEST_DB_DIR);
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
    public void testSelectWithSimpleCondition() throws Exception {
        // Test with simple condition: Student.sid = 3
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);
        Expression expr = CCJSqlParserUtil.parseExpression("Student.sid = 3");
        SelectOperator selectOp = new SelectOperator(scanOp, expr);

        // Should return just one tuple with sid=3
        Tuple tuple = selectOp.getNextTuple();
        assertNotNull("Should return a tuple", tuple);
        assertEquals("Tuple should have sid=3", Integer.valueOf(3), tuple.getAttribute(0));

        // No more tuples should be returned
        assertNull("Should have no more matching tuples", selectOp.getNextTuple());

        scanOp.close();
    }

    @Test
    public void testSelectWithRangeCondition() throws Exception {
        // Test with range condition: Student.age < 30
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);
        Expression expr = CCJSqlParserUtil.parseExpression("Student.age < 30");
        SelectOperator selectOp = new SelectOperator(scanOp, expr);

        // Should return three tuples with age < 30 (students 2, 3, and 4)
        List<Integer> expectedSids = Arrays.asList(2, 3, 4);
        List<Integer> actualSids = new ArrayList<>();

        Tuple tuple;
        while ((tuple = selectOp.getNextTuple()) != null) {
            actualSids.add(tuple.getAttribute(0));
        }

        assertEquals("Should return correct number of tuples", 3, actualSids.size());
        for (Integer sid : expectedSids) {
            assertTrue("Should contain student with sid=" + sid, actualSids.contains(sid));
        }

        scanOp.close();
    }

    @Test
    public void testSelectWithCompoundCondition() throws Exception {
        // Test with compound condition: Student.age < 30 AND Student.gpa >= 3
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);
        Expression expr = CCJSqlParserUtil.parseExpression("Student.age < 30 AND Student.gpa >= 3");
        SelectOperator selectOp = new SelectOperator(scanOp, expr);

        // Should return two tuples (students 2 and 4)
        List<Integer> expectedSids = Arrays.asList(2, 4);
        List<Integer> actualSids = new ArrayList<>();

        Tuple tuple;
        while ((tuple = selectOp.getNextTuple()) != null) {
            actualSids.add(tuple.getAttribute(0));
        }

        assertEquals("Should return correct number of tuples", 2, actualSids.size());
        for (Integer sid : expectedSids) {
            assertTrue("Should contain student with sid=" + sid, actualSids.contains(sid));
        }

        scanOp.close();
    }

    @Test
    public void testSelectWithNoMatches() throws Exception {
        // Test with condition that matches no tuples: Student.sid > 10
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);
        Expression expr = CCJSqlParserUtil.parseExpression("Student.sid > 10");
        SelectOperator selectOp = new SelectOperator(scanOp, expr);

        // Should return no tuples
        assertNull("Should not return any tuples", selectOp.getNextTuple());

        scanOp.close();
    }

    @Test
    public void testSelectWithColumnToColumnComparison() throws Exception {
        // Test with column-to-column comparison: Student.gpa = Student.sid
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);
        Expression expr = CCJSqlParserUtil.parseExpression("Student.gpa = Student.sid");
        SelectOperator selectOp = new SelectOperator(scanOp, expr);

        // Only student with sid=4 has matching gpa=3
        Tuple tuple = selectOp.getNextTuple();
        assertNotNull("Should return another tuple", tuple);
        assertEquals("Tuple should have sid=4", Integer.valueOf(4), tuple.getAttribute(0));
        assertEquals("Tuple should have gpa=4", Integer.valueOf(4), tuple.getAttribute(3));

        // No more tuples should be returned
        assertNull("Should have no more matching tuples", selectOp.getNextTuple());

        scanOp.close();
    }

    @Test
    public void testSelectWithComplexNestedCondition() throws Exception {
        // Test with a complex nested condition (using only AND)
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);
        Expression expr = CCJSqlParserUtil.parseExpression(
                "(Student.age > 20 AND Student.gpa >= 3) AND Student.sid < 5");
        SelectOperator selectOp = new SelectOperator(scanOp, expr);

        // Should return students 1, 2, and 4
        List<Integer> expectedSids = Arrays.asList(1, 2, 4);
        List<Integer> actualSids = new ArrayList<>();

        Tuple tuple;
        while ((tuple = selectOp.getNextTuple()) != null) {
            actualSids.add(tuple.getAttribute(0));
        }

        assertEquals("Should return correct number of tuples", 3, actualSids.size());
        for (Integer sid : expectedSids) {
            assertTrue("Should contain student with sid=" + sid, actualSids.contains(sid));
        }

        scanOp.close();
    }

    @Test
    public void testReset() throws Exception {
        // Test the reset functionality
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);
        Expression expr = CCJSqlParserUtil.parseExpression("Student.gpa = 4");
        SelectOperator selectOp = new SelectOperator(scanOp, expr);

        // Get all matching tuples first time
        List<Integer> firstRunSids = new ArrayList<>();
        Tuple tuple;
        while ((tuple = selectOp.getNextTuple()) != null) {
            firstRunSids.add(tuple.getAttribute(0));
        }

        assertEquals("Should find 2 tuples with gpa=4", 2, firstRunSids.size());

        // Reset and get tuples again
        selectOp.reset();

        List<Integer> secondRunSids = new ArrayList<>();
        while ((tuple = selectOp.getNextTuple()) != null) {
            secondRunSids.add(tuple.getAttribute(0));
        }

        assertEquals("Should find same number of tuples after reset", firstRunSids.size(), secondRunSids.size());
        for (int i = 0; i < firstRunSids.size(); i++) {
            assertEquals("Tuples should be returned in same order after reset",
                    firstRunSids.get(i), secondRunSids.get(i));
        }

        scanOp.close();
    }

    @Test
    public void testSelectWithLiteralComparisons() throws Exception {
        // Test with literal-to-literal comparisons (should always evaluate to same result)
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // True condition: 5 < 10
        Expression trueExpr = CCJSqlParserUtil.parseExpression("5 < 10");
        SelectOperator trueSelectOp = new SelectOperator(scanOp, trueExpr);

        // Should return all 5 tuples since condition is always true
        int count = 0;
        while (trueSelectOp.getNextTuple() != null) {
            count++;
        }
        assertEquals("Should return all 5 tuples for always-true condition", 5, count);

        // Reset scan operator
        scanOp.reset();

        // False condition: 10 < 5
        Expression falseExpr = CCJSqlParserUtil.parseExpression("10 < 5");
        SelectOperator falseSelectOp = new SelectOperator(scanOp, falseExpr);

        // Should return no tuples since condition is always false
        assertNull("Should not return any tuples for always-false condition",
                falseSelectOp.getNextTuple());

        scanOp.close();
    }
}