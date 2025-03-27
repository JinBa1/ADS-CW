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

import ed.inf.adbs.blazedb.operator.ProjectOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class ProjectOperatorTest {

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
    public void testProjectSingleColumn() {
        // Test projecting a single column (sid)
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create Column object for sid
        List<Column> projectedColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);
        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        projectedColumns.add(sidColumn);

        ProjectOperator projectOp = new ProjectOperator(scanOp, projectedColumns);

        // Should keep only the sid column for all tuples
        List<Tuple> projectedTuples = new ArrayList<>();
        Tuple tuple;

        while ((tuple = projectOp.getNextTuple()) != null) {
            projectedTuples.add(tuple);
        }

        assertEquals("Should return 5 projected tuples", 5, projectedTuples.size());

        // Each tuple should have exactly one attribute
        for (Tuple t : projectedTuples) {
            assertEquals("Projected tuple should have 1 attribute", 1, t.getTuple().size());
        }

        // Check some values
        assertEquals("First tuple should have sid=1", Integer.valueOf(1), projectedTuples.get(0).getAttribute(0));
        assertEquals("Last tuple should have sid=5", Integer.valueOf(5), projectedTuples.get(4).getAttribute(0));

        scanOp.close();
    }

    @Test
    public void testProjectMultipleColumns() {
        // Test projecting multiple columns (sid and gpa)
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create Column objects for sid and gpa
        List<Column> projectedColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        projectedColumns.add(sidColumn);

        Column gpaColumn = new Column();
        gpaColumn.setTable(table);
        gpaColumn.setColumnName("gpa");
        projectedColumns.add(gpaColumn);

        ProjectOperator projectOp = new ProjectOperator(scanOp, projectedColumns);

        // Should keep only the sid and gpa columns for all tuples
        Tuple tuple = projectOp.getNextTuple();
        assertNotNull("Should return a projected tuple", tuple);

        // Check the structure and values of the first projected tuple
        assertEquals("Projected tuple should have 2 attributes", 2, tuple.getTuple().size());
        assertEquals("First attribute should be sid=1", Integer.valueOf(1), tuple.getAttribute(0));
        assertEquals("Second attribute should be gpa=3", Integer.valueOf(3), tuple.getAttribute(1));

        // Read all tuples
        int count = 1; // Already read one
        while (projectOp.getNextTuple() != null) {
            count++;
        }

        assertEquals("Should have 5 projected tuples in total", 5, count);

        scanOp.close();
    }

    @Test
    public void testProjectReordering() {
        // Test projecting columns in a different order (gpa then sid)
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create Column objects for gpa and sid (reversed order)
        List<Column> projectedColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column gpaColumn = new Column();
        gpaColumn.setTable(table);
        gpaColumn.setColumnName("gpa");
        projectedColumns.add(gpaColumn);

        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        projectedColumns.add(sidColumn);

        ProjectOperator projectOp = new ProjectOperator(scanOp, projectedColumns);

        Tuple tuple = projectOp.getNextTuple();
        assertNotNull("Should return a projected tuple", tuple);

        // Check the first projected tuple - order should be reversed from the previous test
        assertEquals("Projected tuple should have 2 attributes", 2, tuple.getTuple().size());
        assertEquals("First attribute should be gpa=3", Integer.valueOf(3), tuple.getAttribute(0));
        assertEquals("Second attribute should be sid=1", Integer.valueOf(1), tuple.getAttribute(1));

        scanOp.close();
    }

    @Test
    public void testProjectAfterSelect() {
        // Test project after select to ensure they work together
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        try {
            // Select students with gpa=4
            Expression expr = CCJSqlParserUtil.parseExpression("Student.gpa = 4");
            SelectOperator selectOp = new SelectOperator(scanOp, expr);

            // Project only sid
            List<Column> projectedColumns = new ArrayList<>();
            Table table = new Table();
            table.setName(TEST_TABLE);
            Column sidColumn = new Column();
            sidColumn.setTable(table);
            sidColumn.setColumnName("sid");
            projectedColumns.add(sidColumn);

            ProjectOperator projectOp = new ProjectOperator(selectOp, projectedColumns);

            // Should return two projected tuples (for students 2 and 4)
            List<Integer> projectedSids = new ArrayList<>();
            Tuple tuple;

            while ((tuple = projectOp.getNextTuple()) != null) {
                assertEquals("Projected tuple should have 1 attribute", 1, tuple.getTuple().size());
                projectedSids.add(tuple.getAttribute(0));
            }

            assertEquals("Should have 2 projected tuples after selection", 2, projectedSids.size());
            assertTrue("Should contain student with sid=2", projectedSids.contains(2));
            assertTrue("Should contain student with sid=4", projectedSids.contains(4));

        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        } finally {
            scanOp.close();
        }
    }

    @Test
    public void testProjectDuplicates() {
        // Test projecting the same column multiple times
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create Column objects for sid (twice)
        List<Column> projectedColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column sidColumn1 = new Column();
        sidColumn1.setTable(table);
        sidColumn1.setColumnName("sid");
        projectedColumns.add(sidColumn1);

        Column sidColumn2 = new Column();
        sidColumn2.setTable(table);
        sidColumn2.setColumnName("sid");
        projectedColumns.add(sidColumn2);

        ProjectOperator projectOp = new ProjectOperator(scanOp, projectedColumns);

        Tuple tuple = projectOp.getNextTuple();
        assertNotNull("Should return a projected tuple", tuple);

        // Check the structure and values - should have sid twice
        assertEquals("Projected tuple should have 2 attributes", 2, tuple.getTuple().size());
        assertEquals("First attribute should be sid=1", Integer.valueOf(1), tuple.getAttribute(0));
        assertEquals("Second attribute should also be sid=1", Integer.valueOf(1), tuple.getAttribute(1));

        scanOp.close();
    }

    @Test
    public void testReset() {
        // Test reset functionality
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create Column objects for sid and gpa
        List<Column> projectedColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        projectedColumns.add(sidColumn);

        Column gpaColumn = new Column();
        gpaColumn.setTable(table);
        gpaColumn.setColumnName("gpa");
        projectedColumns.add(gpaColumn);

        ProjectOperator projectOp = new ProjectOperator(scanOp, projectedColumns);

        // Read all tuples first time
        List<Tuple> firstRunTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = projectOp.getNextTuple()) != null) {
            firstRunTuples.add(tuple);
        }

        assertEquals("Should return 5 projected tuples", 5, firstRunTuples.size());

        // Reset and get tuples again
        projectOp.reset();

        List<Tuple> secondRunTuples = new ArrayList<>();
        while ((tuple = projectOp.getNextTuple()) != null) {
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
    public void testProjectNoColumns() {
        // Edge case: projecting no columns
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);
        List<Column> projectedColumns = new ArrayList<>(); // Empty list

        ProjectOperator projectOp = new ProjectOperator(scanOp, projectedColumns);

        Tuple tuple = projectOp.getNextTuple();
        assertNotNull("Should still return tuples", tuple);
        assertEquals("Projected tuple should have 0 attributes", 0, tuple.getTuple().size());

        scanOp.close();
    }

    @Test
    public void testProjectEveryColumn() {
        // Test projecting all columns in original order
        ScanOperator scanOp = new ScanOperator(TEST_TABLE);

        // Create all Column objects
        List<Column> projectedColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(TEST_TABLE);

        Column sidColumn = new Column();
        sidColumn.setTable(table);
        sidColumn.setColumnName("sid");
        projectedColumns.add(sidColumn);

        Column nameColumn = new Column();
        nameColumn.setTable(table);
        nameColumn.setColumnName("name");
        projectedColumns.add(nameColumn);

        Column ageColumn = new Column();
        ageColumn.setTable(table);
        ageColumn.setColumnName("age");
        projectedColumns.add(ageColumn);

        Column gpaColumn = new Column();
        gpaColumn.setTable(table);
        gpaColumn.setColumnName("gpa");
        projectedColumns.add(gpaColumn);

        ProjectOperator projectOp = new ProjectOperator(scanOp, projectedColumns);

        Tuple tuple = projectOp.getNextTuple();
        assertNotNull("Should return a projected tuple", tuple);

        // Check it has all attributes
        assertEquals("Projected tuple should have all 4 attributes", 4, tuple.getTuple().size());
        assertEquals("First attribute should be sid=1", Integer.valueOf(1), tuple.getAttribute(0));
        assertEquals("Second attribute should be name=25", Integer.valueOf(25), tuple.getAttribute(1));
        assertEquals("Third attribute should be age=85", Integer.valueOf(85), tuple.getAttribute(2));
        assertEquals("Fourth attribute should be gpa=3", Integer.valueOf(3), tuple.getAttribute(3));

        scanOp.close();
    }
}