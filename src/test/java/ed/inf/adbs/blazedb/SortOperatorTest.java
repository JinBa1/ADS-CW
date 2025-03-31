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
import ed.inf.adbs.blazedb.operator.SortOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class SortOperatorTest {

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

        // Create test data file with varied sample data (intentionally not in sorted order)
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + TEST_TABLE + ".csv"))) {
            writer.write("3, 35, 19, 2\n");     // sid=3, name=35, age=19, gpa=2
            writer.write("1, 25, 85, 3\n");     // sid=1, name=25, age=85, gpa=3
            writer.write("5, 45, 65, 3\n");     // sid=5, name=45, age=65, gpa=3
            writer.write("2, 30, 22, 4\n");     // sid=2, name=30, age=22, gpa=4
            writer.write("4, 40, 21, 4\n");     // sid=4, name=40, age=21, gpa=4
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
    public void testSortBySingleColumn() {
        // Test sorting by a single column (sid)
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create Column object for sid
        List<Column> sortColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);
        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        sortColumns.add(sidColumn);

        SortOperator sortOp = new SortOperator(scanOp, sortColumns);

        // Get all tuples and verify they are in the expected order
        List<Integer> expectedSids = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> actualSids = new ArrayList<>();

        Tuple tuple;
        while ((tuple = sortOp.getNextTuple()) != null) {
            actualSids.add(tuple.getAttribute(0));
        }

        assertEquals("Number of tuples after sorting should match", expectedSids.size(), actualSids.size());

        for (int i = 0; i < expectedSids.size(); i++) {
            assertEquals("Tuples should be sorted by sid in ascending order",
                    expectedSids.get(i), actualSids.get(i));
        }

        scanOp.close();
    }

    @Test
    public void testSortByMultipleColumns() {
        // Test sorting by multiple columns (gpa then age)
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create Column objects for gpa and age
        List<Column> sortColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column gpaColumn = new Column();
        gpaColumn.setTable(table);
        gpaColumn.setColumnName("gpa");
        sortColumns.add(gpaColumn);

        Column ageColumn = new Column();
        ageColumn.setTable(table);
        ageColumn.setColumnName("age");
        sortColumns.add(ageColumn);

        SortOperator sortOp = new SortOperator(scanOp, sortColumns);

        // Get all tuples
        List<Tuple> sortedTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sortOp.getNextTuple()) != null) {
            sortedTuples.add(tuple);
        }

        assertEquals("Number of tuples after sorting should be 5", 5, sortedTuples.size());

        // Check sorting by gpa (primary) and age (secondary)
        // Expected order: (sid 3, gpa 2) then (sid 1, gpa 3), (sid 5, gpa 3) then (sid 4, gpa 4), (sid 2, gpa 4)
        // For equal gpa (3), sid 5 (age 65) should come before sid 1 (age 85)
        // For equal gpa (4), sid 4 (age 21) should come before sid 2 (age 22)

        // First tuple should be sid 3 (lowest gpa)
        assertEquals("First tuple should have sid=3 (lowest gpa)", Integer.valueOf(3), sortedTuples.get(0).getAttribute(0));

        // Check the order of tuples with gpa=3
        boolean correctOrderForGpa3 = false;
        for (int i = 1; i < sortedTuples.size() - 2; i++) {
            if (sortedTuples.get(i).getAttribute(3) == 3 &&
                    sortedTuples.get(i+1).getAttribute(3) == 3) {

                int age1 = sortedTuples.get(i).getAttribute(2);
                int age2 = sortedTuples.get(i+1).getAttribute(2);
                if (age1 < age2) {
                    correctOrderForGpa3 = true;
                    break;
                }
            }
        }

        assertTrue("Tuples with same gpa should be sorted by age", correctOrderForGpa3);

        // Check the order of tuples with gpa=4
        boolean correctOrderForGpa4 = false;
        for (int i = 3; i < sortedTuples.size() - 1; i++) {
            if (sortedTuples.get(i).getAttribute(3) == 4 &&
                    sortedTuples.get(i+1).getAttribute(3) == 4) {

                int age1 = sortedTuples.get(i).getAttribute(2);
                int age2 = sortedTuples.get(i+1).getAttribute(2);
                if (age1 < age2) {
                    correctOrderForGpa4 = true;
                    break;
                }
            }
        }

        assertTrue("Tuples with same gpa should be sorted by age", correctOrderForGpa4);

        scanOp.close();
    }

    @Test
    public void testSortWithSelect() {
        // Test sorting after selecting a subset of tuples
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        try {
            // Select only students with gpa >= 3
            Expression selectExpr = CCJSqlParserUtil.parseExpression("Student.gpa >= 3");
            SelectOperator selectOp = new SelectOperator(scanOp, selectExpr);

            // Sort by sid
            List<Column> sortColumns = new ArrayList<>();
            Table table = new Table();
            table.setName(TEST_TABLE);
            Column sidColumn = new Column();
            sidColumn.setTable(table);
            sidColumn.setColumnName("sid");
            sortColumns.add(sidColumn);

            SortOperator sortOp = new SortOperator(selectOp, sortColumns);

            // Expected sids after filtering and sorting: 1, 2, 4, 5
            List<Integer> expectedSids = Arrays.asList(1, 2, 4, 5);
            List<Integer> actualSids = new ArrayList<>();

            Tuple tuple;
            while ((tuple = sortOp.getNextTuple()) != null) {
                actualSids.add(tuple.getAttribute(0));
            }

            assertEquals("Number of tuples after filtering and sorting should match",
                    expectedSids.size(), actualSids.size());

            for (int i = 0; i < expectedSids.size(); i++) {
                assertEquals("Tuples should be sorted by sid in ascending order",
                        expectedSids.get(i), actualSids.get(i));
            }

        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        } finally {
            scanOp.close();
        }
    }

    @Test
    public void testReset() {
        // Test reset functionality
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Sort by sid
        List<Column> sortColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);
        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        sortColumns.add(sidColumn);

        SortOperator sortOp = new SortOperator(scanOp, sortColumns);

        // Read all tuples first time
        List<Tuple> firstRunTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sortOp.getNextTuple()) != null) {
            firstRunTuples.add(tuple);
        }

        assertEquals("First run should return 5 sorted tuples", 5, firstRunTuples.size());

        // Reset and get tuples again
        sortOp.reset();

        List<Tuple> secondRunTuples = new ArrayList<>();
        while ((tuple = sortOp.getNextTuple()) != null) {
            secondRunTuples.add(tuple);
        }

        assertEquals("Should return same number of tuples after reset",
                firstRunTuples.size(), secondRunTuples.size());

        // Compare tuples from both runs
        for (int i = 0; i < firstRunTuples.size(); i++) {
            Tuple firstTuple = firstRunTuples.get(i);
            Tuple secondTuple = secondRunTuples.get(i);

            for (int j = 0; j < firstTuple.getTuple().size(); j++) {
                assertEquals("Attribute values should match after reset",
                        firstTuple.getAttribute(j), secondTuple.getAttribute(j));
            }
        }

        scanOp.close();
    }

    @Test
    public void testSortEmptyResult() {
        // Test sorting when the child operator returns no tuples
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        try {
            // Select students with an impossible condition
            Expression selectExpr = CCJSqlParserUtil.parseExpression("Student.sid > 100");
            SelectOperator selectOp = new SelectOperator(scanOp, selectExpr);

            // Sort by sid
            List<Column> sortColumns = new ArrayList<>();
            Table table = new Table();
            table.setName(TEST_TABLE);
            Column sidColumn = new Column();
            sidColumn.setTable(table);
            sidColumn.setColumnName("sid");
            sortColumns.add(sidColumn);

            SortOperator sortOp = new SortOperator(selectOp, sortColumns);

            // Should return no tuples
            assertNull("Sort operator with empty input should return null", sortOp.getNextTuple());

        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        } finally {
            scanOp.close();
        }
    }

    @Test
    public void testSortBySecondaryColumn() {
        // Test that secondary sort key is used only when primary keys are equal
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create new test data with tied gpa values
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + TEST_TABLE + ".csv"))) {
            writer.write("1, 25, 30, 3\n");     // sid=1, name=25, age=30, gpa=3
            writer.write("2, 30, 20, 3\n");     // sid=2, name=30, age=20, gpa=3
            writer.write("3, 35, 40, 3\n");     // sid=3, name=35, age=40, gpa=3
            writer.write("4, 40, 10, 3\n");     // sid=4, name=40, age=10, gpa=3
            writer.write("5, 45, 50, 3\n");     // sid=5, name=45, age=50, gpa=3
        } catch (IOException e) {
            fail("Failed to write test data: " + e.getMessage());
        }

        // Sort by gpa (primary) and age (secondary)
        List<Column> sortColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column gpaColumn = new Column();
        gpaColumn.setTable(table);
        gpaColumn.setColumnName("gpa");
        sortColumns.add(gpaColumn);

        Column ageColumn = new Column();
        ageColumn.setTable(table);
        ageColumn.setColumnName("age");
        sortColumns.add(ageColumn);

        SortOperator sortOp = new SortOperator(scanOp, sortColumns);

        // Expected order by age: 4, 2, 1, 3, 5 (all have same gpa)
        List<Integer> expectedSids = Arrays.asList(4, 2, 1, 3, 5);
        List<Integer> actualSids = new ArrayList<>();

        Tuple tuple;
        while ((tuple = sortOp.getNextTuple()) != null) {
            actualSids.add(tuple.getAttribute(0));
        }

        assertEquals("Number of tuples after sorting should match",
                expectedSids.size(), actualSids.size());

        for (int i = 0; i < expectedSids.size(); i++) {
            assertEquals("Tuples should be sorted by age when gpa is equal",
                    expectedSids.get(i), actualSids.get(i));
        }

        scanOp.close();
    }

    @Test
    public void testPartialIteration() {
        // Test behavior when only retrieving some tuples
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Sort by sid
        List<Column> sortColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);
        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        sortColumns.add(sidColumn);

        SortOperator sortOp = new SortOperator(scanOp, sortColumns);

        // Get only first 3 tuples
        List<Integer> partialSids = new ArrayList<>();
        Tuple tuple;
        for (int i = 0; i < 3 && (tuple = sortOp.getNextTuple()) != null; i++) {
            partialSids.add(tuple.getAttribute(0));
        }

        assertEquals("Should have retrieved 3 tuples", 3, partialSids.size());
        assertEquals("First tuple should have sid=1", Integer.valueOf(1), partialSids.get(0));
        assertEquals("Second tuple should have sid=2", Integer.valueOf(2), partialSids.get(1));
        assertEquals("Third tuple should have sid=3", Integer.valueOf(3), partialSids.get(2));

        // Reset and get all tuples
        sortOp.reset();

        List<Integer> allSids = new ArrayList<>();
        while ((tuple = sortOp.getNextTuple()) != null) {
            allSids.add(tuple.getAttribute(0));
        }

        assertEquals("Should have retrieved all 5 tuples after reset", 5, allSids.size());

        scanOp.close();
    }

    @Test
    public void testSortDescendingOrderIssue() {
        // This test verifies that the operator sorts in ascending order
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Sort by sid
        List<Column> sortColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);
        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        sortColumns.add(sidColumn);

        SortOperator sortOp = new SortOperator(scanOp, sortColumns);

        List<Integer> sortedSids = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sortOp.getNextTuple()) != null) {
            sortedSids.add(tuple.getAttribute(0));
        }

        // Check that values are in ascending order
        for (int i = 0; i < sortedSids.size() - 1; i++) {
            assertTrue("Tuples should be sorted in ascending order",
                    sortedSids.get(i) < sortedSids.get(i + 1));
        }

        scanOp.close();
    }


}