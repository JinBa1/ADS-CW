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

import ed.inf.adbs.blazedb.operator.DuplicateEliminationOperator;
import ed.inf.adbs.blazedb.operator.ProjectOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class DuplicateEliminationOperatorTest {

    private static final String TEST_DB_DIR = "src/test/resources/testdb";
    private static final String SCHEMA_FILE = TEST_DB_DIR + "/schema.txt";
    private static final String DATA_DIR = TEST_DB_DIR + "/data";
    private static final String TEST_TABLE = "TestTable";

    @Before
    public void setUp() throws IOException {
        // Create test database directory structure
        Files.createDirectories(Paths.get(DATA_DIR));

        // Create schema file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(SCHEMA_FILE))) {
            writer.write(TEST_TABLE + " A B C\n");
        }

        // Create test data file with duplicate values
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + TEST_TABLE + ".csv"))) {
            writer.write("1, 10, 100\n");    // Unique tuple
            writer.write("2, 20, 200\n");    // Unique tuple
            writer.write("1, 10, 100\n");    // Duplicate of first tuple
            writer.write("3, 30, 300\n");    // Unique tuple
            writer.write("2, 20, 200\n");    // Duplicate of second tuple
            writer.write("4, 40, 400\n");    // Unique tuple
            writer.write("1, 10, 100\n");    // Another duplicate of first tuple
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
    public void testDuplicateElimination() {
        // Create a scan operator
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create a DuplicateEliminationOperator with the scan operator as child
        DuplicateEliminationOperator distinctOp = new DuplicateEliminationOperator(scanOp);

        // Get all tuples after duplicate elimination
        List<Tuple> distinctTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = distinctOp.getNextTuple()) != null) {
            distinctTuples.add(tuple);
        }

        // Should only have 4 unique tuples (7 total in the data file)
        assertEquals("Should return 4 distinct tuples", 4, distinctTuples.size());

        // Verify the distinct tuples have the expected values
        List<Integer> expectedFirstValues = Arrays.asList(1, 2, 3, 4);
        for (Tuple t : distinctTuples) {
            Integer firstValue = t.getAttribute(0);
            assertTrue("Distinct tuples should contain value " + firstValue,
                    expectedFirstValues.contains(firstValue));
        }
    }

    @Test
    public void testDuplicateEliminationWithProjection() {
        // Create a scan operator
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create a projection on column A only (will create more duplicates)
        List<Column> projectedColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);
        Column colA = new Column();
        colA.setTable(table);
        colA.setColumnName("A");
        projectedColumns.add(colA);

        ProjectOperator projectOp = new ProjectOperator(scanOp, projectedColumns);

        // Create a DuplicateEliminationOperator after projection
        DuplicateEliminationOperator distinctOp = new DuplicateEliminationOperator(projectOp);

        // Get all tuples after duplicate elimination
        List<Tuple> distinctTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = distinctOp.getNextTuple()) != null) {
            distinctTuples.add(tuple);
        }

        // Should only have 4 unique values of column A
        assertEquals("Should return 4 distinct values for column A", 4, distinctTuples.size());

        // Verify the distinct tuples have only one column (A)
        for (Tuple t : distinctTuples) {
            assertEquals("Projected tuple should have only 1 attribute", 1, t.getTuple().size());
        }

        // Verify the distinct values of A
        List<Integer> expectedValues = Arrays.asList(1, 2, 3, 4);
        for (Tuple t : distinctTuples) {
            Integer value = t.getAttribute(0);
            assertTrue("Distinct values should include " + value,
                    expectedValues.contains(value));

            // Remove the value to ensure no duplicates in our results
            expectedValues.remove(value);
        }

        // All expected values should have been found
        assertTrue("All expected values should be found exactly once", expectedValues.isEmpty());
    }

    @Test
    public void testReset() {
        // Create a scan operator
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create a DuplicateEliminationOperator
        DuplicateEliminationOperator distinctOp = new DuplicateEliminationOperator(scanOp);

        // Get all tuples first time
        List<Tuple> firstRunTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = distinctOp.getNextTuple()) != null) {
            firstRunTuples.add(tuple);
        }

        assertEquals("Should return 4 distinct tuples on first run", 4, firstRunTuples.size());

        // Reset and get tuples again
        distinctOp.reset();

        List<Tuple> secondRunTuples = new ArrayList<>();
        while ((tuple = distinctOp.getNextTuple()) != null) {
            secondRunTuples.add(tuple);
        }

        assertEquals("Should return 4 distinct tuples on second run", 4, secondRunTuples.size());

        // Compare both runs - sizes should match
        assertEquals("Both runs should return same number of tuples",
                firstRunTuples.size(), secondRunTuples.size());

        // Note: We can't guarantee the order of tuples after reset due to hash-based implementation,
        // so we'll just check that both runs contain the same set of tuples
        for (Tuple firstTuple : firstRunTuples) {
            boolean found = false;
            for (Tuple secondTuple : secondRunTuples) {
                if (tuplesEqual(firstTuple, secondTuple)) {
                    found = true;
                    break;
                }
            }
            assertTrue("Every tuple from first run should be in second run", found);
        }
    }

    @Test
    public void testEmptyInput() {
        // Create an empty test file
        try {
            Files.deleteIfExists(Paths.get(DATA_DIR + "/" + TEST_TABLE + ".csv"));
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + TEST_TABLE + ".csv"))) {
                // Intentionally left empty
            }
        } catch (IOException e) {
            fail("Failed to create empty test file: " + e.getMessage());
        }

        // Create a scan operator on empty table
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create a DuplicateEliminationOperator
        DuplicateEliminationOperator distinctOp = new DuplicateEliminationOperator(scanOp);

        // Should not return any tuples
        assertNull("Empty input should produce no tuples", distinctOp.getNextTuple());
    }

    @Test
    public void testAllDuplicates() {
        // Create a file with all duplicate rows
        try {
            Files.deleteIfExists(Paths.get(DATA_DIR + "/" + TEST_TABLE + ".csv"));
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + TEST_TABLE + ".csv"))) {
                writer.write("1, 10, 100\n");
                writer.write("1, 10, 100\n");
                writer.write("1, 10, 100\n");
                writer.write("1, 10, 100\n");
                writer.write("1, 10, 100\n");
            }
        } catch (IOException e) {
            fail("Failed to create test file: " + e.getMessage());
        }

        // Create a scan operator
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create a DuplicateEliminationOperator
        DuplicateEliminationOperator distinctOp = new DuplicateEliminationOperator(scanOp);

        // Count results
        int count = 0;
        Tuple tuple;
        while ((tuple = distinctOp.getNextTuple()) != null) {
            count++;
        }

        // Should only return one tuple
        assertEquals("Should return exactly one distinct tuple", 1, count);
    }

    /**
     * Helper method to check if two tuples have the same values
     */
    private boolean tuplesEqual(Tuple t1, Tuple t2) {
        if (t1.getTuple().size() != t2.getTuple().size()) {
            return false;
        }

        for (int i = 0; i < t1.getTuple().size(); i++) {
            if (!t1.getAttribute(i).equals(t2.getAttribute(i))) {
                return false;
            }
        }

        return true;
    }
}