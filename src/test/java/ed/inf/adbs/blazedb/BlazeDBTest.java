package ed.inf.adbs.blazedb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Integration tests for BlazeDB.
 * Tests the program as a whole using the command-line interface.
 *
 * These tests cover all the query types supported by BlazeDB as per the coursework requirements:
 * - Simple SELECT-FROM-WHERE queries
 * - Joins
 * - DISTINCT
 * - ORDER BY
 * - GROUP BY
 * - SUM aggregates
 * - Various combinations of the above
 *
 * Note: All column names in the test database are single letters as per the coursework spec,
 * and all attribute values are integers. The schema is as follows:
 *
 * Student: A B C D (where A is student ID, B is name code, C is age, D is GPA*10)
 * Course: E F G H (where E is course ID, F is course name code, G is credits, H is department ID)
 * Enrolled: I J K (where I is student ID, J is course ID, K is grade)
 * Department: L M N (where L is department ID, M is department name code, N is building code)
 * Faculty: O P Q (where O is faculty ID, P is name code, Q is department ID)
 */
public class BlazeDBTest {

	private static final String TEST_DB_DIR = "src/test/resources/test_integration_db";
	private static final String TEST_QUERIES_DIR = "src/test/resources/test_integration_queries";
	private static final String TEST_OUTPUT_DIR = "src/test/resources/test_integration_output";
	private static final String EXPECTED_OUTPUT_DIR = "src/test/resources/test_integration_expected";

	@Before
	public void setUp() throws IOException {
		// Create directories if they don't exist
		Files.createDirectories(Paths.get(TEST_DB_DIR));
		Files.createDirectories(Paths.get(TEST_DB_DIR, "data"));
		Files.createDirectories(Paths.get(TEST_QUERIES_DIR));
		Files.createDirectories(Paths.get(TEST_OUTPUT_DIR));
		Files.createDirectories(Paths.get(EXPECTED_OUTPUT_DIR));

		// Create schema file
		createSchemaFile();

		// Create data files
		createStudentData();
		createCourseData();
		createEnrolledData();
		createDepartmentData();
		createFacultyData();
	}

	@After
	public void tearDown() throws IOException {
		// Delete test directories and their contents
		// (uncomment if you want cleanup)
        /*
        deleteDirectory(Paths.get(TEST_DB_DIR));
        deleteDirectory(Paths.get(TEST_QUERIES_DIR));
        deleteDirectory(Paths.get(TEST_OUTPUT_DIR));
        deleteDirectory(Paths.get(EXPECTED_OUTPUT_DIR));
        */
	}

	/**
	 * Test a simple SELECT * query with no WHERE clause.
	 */
	@Test
	public void testSimpleSelectAll() throws IOException {
		String queryName = "test_simple_select_all";
		String queryContent = "SELECT * FROM Student;";

		String expectedOutput =
				"1, 25, 85, 30\n" +
						"2, 30, 22, 40\n" +
						"3, 35, 19, 20\n" +
						"4, 40, 21, 40\n" +
						"5, 45, 65, 30\n" +
						"6, 50, 32, 10\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a simple SELECT with specific columns.
	 */
	@Test
	public void testSimpleSelectColumns() throws IOException {
		String queryName = "test_simple_select_columns";
		String queryContent = "SELECT Student.A, Student.D FROM Student;";

		String expectedOutput =
				"1, 30\n" +
						"2, 40\n" +
						"3, 20\n" +
						"4, 40\n" +
						"5, 30\n" +
						"6, 10\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a SELECT with a WHERE clause.
	 */
	@Test
	public void testSelectWithWhere() throws IOException {
		String queryName = "test_select_with_where";
		String queryContent = "SELECT Student.A, Student.B, Student.D FROM Student WHERE Student.D > 30;";

		String expectedOutput =
				"2, 30, 40\n" +
						"4, 40, 40\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a SELECT with a complex WHERE clause using AND.
	 */
	@Test
	public void testSelectWithComplexWhere() throws IOException {
		String queryName = "test_select_with_complex_where";
		String queryContent = "SELECT Student.A, Student.C FROM Student WHERE Student.D >= 30 AND Student.C < 50;";

		String expectedOutput =
				"2, 22\n" +
						"4, 21\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a simple join between two tables.
	 */
	@Test
	public void testSimpleJoin() throws IOException {
		String queryName = "test_simple_join";
		String queryContent = "SELECT Student.A, Student.B, Enrolled.J, Enrolled.K FROM Student, Enrolled WHERE Student.A = Enrolled.I;";

		String expectedOutput =
				"1, 25, 101, 85\n" +
						"1, 25, 102, 92\n" +
						"2, 30, 101, 91\n" +
						"2, 30, 103, 84\n" +
						"3, 35, 102, 78\n" +
						"4, 40, 104, 65\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a join between three tables.
	 */
	@Test
	public void testThreeWayJoin() throws IOException {
		String queryName = "test_three_way_join";
		String queryContent = "SELECT Student.A, Student.B, Course.F, Enrolled.K FROM Student, Enrolled, Course WHERE Student.A = Enrolled.I AND Enrolled.J = Course.E;";

		String expectedOutput =
				"1, 25, 201, 85\n" +
						"1, 25, 202, 92\n" +
						"2, 30, 201, 91\n" +
						"2, 30, 203, 84\n" +
						"3, 35, 202, 78\n" +
						"4, 40, 204, 65\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a join with a complex WHERE clause.
	 */
	@Test
	public void testJoinWithComplexWhere() throws IOException {
		String queryName = "test_join_with_complex_where";
		String queryContent = "SELECT Student.A, Student.B, Course.F, Enrolled.K FROM Student, Enrolled, Course WHERE Student.A = Enrolled.I AND Enrolled.J = Course.E AND Enrolled.K > 80;";

		String expectedOutput =
				"1, 25, 201, 85\n" +
						"1, 25, 202, 92\n" +
						"2, 30, 201, 91\n" +
						"2, 30, 203, 84\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a query with DISTINCT.
	 */
	@Test
	public void testSelectDistinct() throws IOException {
		String queryName = "test_select_distinct";
		String queryContent = "SELECT DISTINCT Student.D FROM Student;";

		String expectedOutput =
				"30\n" +
						"40\n" +
						"20\n" +
						"10\n";

		runTest(queryName, queryContent, expectedOutput, false); // Order might vary
	}

	/**
	 * Test a query with ORDER BY.
	 */
	@Test
	public void testSelectWithOrderBy() throws IOException {
		String queryName = "test_select_with_order_by";
		String queryContent = "SELECT Student.A, Student.D FROM Student ORDER BY Student.D;";

		String expectedOutput =
				"6, 10\n" +
						"3, 20\n" +
						"1, 30\n" +
						"5, 30\n" +
						"2, 40\n" +
						"4, 40\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a query with ORDER BY on multiple columns.
	 */
	@Test
	public void testSelectWithMultipleOrderBy() throws IOException {
		String queryName = "test_select_with_multiple_order_by";
		String queryContent = "SELECT Student.A, Student.D, Student.C FROM Student ORDER BY Student.D, Student.C;";

		String expectedOutput =
				"6, 10, 32\n" +
						"3, 20, 19\n" +
						"1, 30, 85\n" +
						"5, 30, 65\n" +
						"4, 40, 21\n" +
						"2, 40, 22\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a query with GROUP BY and SUM aggregation.
	 */
	@Test
	public void testGroupByWithSum() throws IOException {
		String queryName = "test_group_by_with_sum";
		String queryContent = "SELECT Student.D, SUM(Student.C) FROM Student GROUP BY Student.D;";

		String expectedOutput =
				"10, 32\n" +
						"20, 19\n" +
						"30, 150\n" +
						"40, 43\n";

		runTest(queryName, queryContent, expectedOutput, false); // Order might vary
	}

	/**
	 * Test a query with SUM aggregation but no GROUP BY.
	 */
	@Test
	public void testSumWithoutGroupBy() throws IOException {
		String queryName = "test_sum_without_group_by";
		String queryContent = "SELECT SUM(Student.C) FROM Student;";

		String expectedOutput = "244\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a query with SUM(1) to count rows.
	 */
	@Test
	public void testSumCount() throws IOException {
		String queryName = "test_sum_count";
		String queryContent = "SELECT SUM(1) FROM Student;";

		String expectedOutput = "6\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a query with SUM of product.
	 */
	@Test
	public void testSumProduct() throws IOException {
		String queryName = "test_sum_product";
		String queryContent = "SELECT SUM(Student.C*Student.D) FROM Student;";

		// Calculation:
		// (85*30 + 22*40 + 19*20 + 21*40 + 65*30 + 32*10)
		// = 2550 + 880 + 380 + 840 + 1950 + 320 = 6920
		String expectedOutput = "6920\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a query with GROUP BY, SUM and ORDER BY together.
	 */
	@Test
	public void testGroupBySumOrderBy() throws IOException {
		String queryName = "test_group_by_sum_order_by";
		String queryContent = "SELECT Student.D, SUM(Student.C) FROM Student GROUP BY Student.D ORDER BY Student.D;";

		String expectedOutput =
				"10, 32\n" +
						"20, 19\n" +
						"30, 150\n" +
						"40, 43\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a query with DISTINCT and ORDER BY together.
	 */
	@Test
	public void testDistinctOrderBy() throws IOException {
		String queryName = "test_distinct_order_by";
		String queryContent = "SELECT DISTINCT Student.D FROM Student ORDER BY Student.D;";

		String expectedOutput =
				"10\n" +
						"20\n" +
						"30\n" +
						"40\n";

		runTest(queryName, queryContent, expectedOutput);
	}

	/**
	 * Test a GROUP BY query with a join operation.
	 */
	@Test
	public void testGroupByWithJoin() throws IOException {
		String queryName = "test_group_by_with_join";
		String queryContent = "SELECT Course.H, SUM(Enrolled.K) FROM Course, Enrolled WHERE Course.E = Enrolled.J GROUP";
	}

	// Helper methods

	/**
	 * Runs a test with the given query name, query content, and expected output.
	 * Creates the query file, runs BlazeDB, and verifies the output.
	 */
	private void runTest(String queryName, String queryContent, String expectedOutput) throws IOException {
		runTest(queryName, queryContent, expectedOutput, true);
	}

	/**
	 * Runs a test with the given query name, query content, and expected output.
	 *
	 * @param strictOrder If true, the output order must match exactly. If false,
	 *                    the output is compared regardless of row order.
	 */
	private void runTest(String queryName, String queryContent, String expectedOutput, boolean strictOrder) throws IOException {
		// Create query file
		String queryFilePath = Paths.get(TEST_QUERIES_DIR, queryName + ".sql").toString();
		Files.write(Paths.get(queryFilePath), queryContent.getBytes(StandardCharsets.UTF_8));

		// Create expected output file
		String expectedFilePath = Paths.get(EXPECTED_OUTPUT_DIR, queryName + ".csv").toString();
		Files.write(Paths.get(expectedFilePath), expectedOutput.getBytes(StandardCharsets.UTF_8));

		// Run BlazeDB
		String outputFilePath = Paths.get(TEST_OUTPUT_DIR, queryName + ".csv").toString();
		BlazeDB.main(new String[] { TEST_DB_DIR, queryFilePath, outputFilePath });

		// Verify output
		if (strictOrder) {
			verifyOutputExactOrder(expectedFilePath, outputFilePath);
		} else {
			verifyOutputAnyOrder(expectedFilePath, outputFilePath);
		}
	}

	/**
	 * Verifies that the output file exactly matches the expected file.
	 */
	private void verifyOutputExactOrder(String expectedFilePath, String outputFilePath) throws IOException {
		List<String> expectedLines = Files.readAllLines(Paths.get(expectedFilePath));
		List<String> actualLines = Files.readAllLines(Paths.get(outputFilePath));

		assertEquals("Number of lines in output doesn't match expected", expectedLines.size(), actualLines.size());

		for (int i = 0; i < expectedLines.size(); i++) {
			String expected = expectedLines.get(i).trim();
			String actual = actualLines.get(i).trim();
			assertEquals("Line " + (i+1) + " doesn't match", expected, actual);
		}
	}

	/**
	 * Verifies that the output file contains the same lines as the expected file,
	 * regardless of order.
	 */
	private void verifyOutputAnyOrder(String expectedFilePath, String outputFilePath) throws IOException {
		List<String> expectedLines = Files.readAllLines(Paths.get(expectedFilePath));
		List<String> actualLines = Files.readAllLines(Paths.get(outputFilePath));

		assertEquals("Number of lines in output doesn't match expected", expectedLines.size(), actualLines.size());

		// Normalize and sort the lines for comparison
		List<String> normalizedExpected = new ArrayList<>();
		List<String> normalizedActual = new ArrayList<>();

		for (String line : expectedLines) {
			normalizedExpected.add(line.trim());
		}

		for (String line : actualLines) {
			normalizedActual.add(line.trim());
		}

		Collections.sort(normalizedExpected);
		Collections.sort(normalizedActual);

		for (int i = 0; i < normalizedExpected.size(); i++) {
			assertEquals("Line content doesn't match (after sorting)", normalizedExpected.get(i), normalizedActual.get(i));
		}
	}

	/**
	 * Recursively deletes a directory and its contents.
	 */
	private void deleteDirectory(Path path) throws IOException {
		if (Files.exists(path)) {
			Files.walk(path)
					.sorted(Comparator.reverseOrder())
					.map(Path::toFile)
					.forEach(File::delete);
		}
	}

	/**
	 * Creates the schema file for the test database.
	 */
	private void createSchemaFile() throws IOException {
		String schema =
				"Student A B C D\n" +
						"Course E F G H\n" +
						"Enrolled I J K\n" +
						"Department L M N\n" +
						"Faculty O P Q";

		Files.write(Paths.get(TEST_DB_DIR, "schema.txt"), schema.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Creates sample Student data.
	 * A: student ID, B: code representing name, C: age, D: GPA*10
	 */
	private void createStudentData() throws IOException {
		String data =
				"1, 25, 85, 30\n" +
						"2, 30, 22, 40\n" +
						"3, 35, 19, 20\n" +
						"4, 40, 21, 40\n" +
						"5, 45, 65, 30\n" +
						"6, 50, 32, 10";

		Files.write(Paths.get(TEST_DB_DIR, "data", "Student.csv"), data.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Creates sample Course data.
	 * E: course ID, F: code representing course name, G: credits, H: department ID
	 */
	private void createCourseData() throws IOException {
		String data =
				"101, 201, 3, 1\n" +
						"102, 202, 3, 2\n" +
						"103, 203, 3, 3\n" +
						"104, 204, 4, 3\n" +
						"105, 205, 4, 1\n" +
						"106, 206, 4, 2";

		Files.write(Paths.get(TEST_DB_DIR, "data", "Course.csv"), data.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Creates sample Enrolled data.
	 * I: student ID, J: course ID, K: grade
	 */
	private void createEnrolledData() throws IOException {
		String data =
				"1, 101, 85\n" +
						"1, 102, 92\n" +
						"2, 101, 91\n" +
						"2, 103, 84\n" +
						"3, 102, 78\n" +
						"4, 104, 65";

		Files.write(Paths.get(TEST_DB_DIR, "data", "Enrolled.csv"), data.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Creates sample Department data.
	 * L: department ID, M: code representing name, N: code representing building
	 */
	private void createDepartmentData() throws IOException {
		String data =
				"1, 301, 401\n" +
						"2, 302, 402\n" +
						"3, 303, 403";

		Files.write(Paths.get(TEST_DB_DIR, "data", "Department.csv"), data.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Creates sample Faculty data.
	 * O: faculty ID, P: code representing name, Q: department ID
	 */
	private void createFacultyData() throws IOException {
		String data =
				"1, 501, 1\n" +
						"2, 502, 1\n" +
						"3, 503, 2\n" +
						"4, 504, 2\n" +
						"5, 505, 3\n" +
						"6, 506, 3";

		Files.write(Paths.get(TEST_DB_DIR, "data", "Faculty.csv"), data.getBytes(StandardCharsets.UTF_8));
	}
}