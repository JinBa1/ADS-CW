package ed.inf.adbs.blazedb;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import ed.inf.adbs.blazedb.operator.JoinOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import ed.inf.adbs.blazedb.operator.Operator;

/**
 * Lightweight in-memory database system.
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 */
public class BlazeDB {


	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Just for demonstration, replace this function call with your logic
//		parsingExample(inputFile);
		DBCatalog.resetDBCatalog();
		DBCatalog.initDBCatalog(databaseDir);
		Operator rootOp = QueryPlanner.parseStatement(inputFile);
		execute(rootOp, outputFile);
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement
	 * from a file or a string and prints the SELECT and WHERE clauses to screen.
	 */
	public static void parsingExample(String filename) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
//            Statement statement = CCJSqlParserUtil.parse("SELECT Course.cid, Student.name FROM Course, Student WHERE Student.sid = 3");
			if (statement != null) {
				Select select = (Select) statement;
				System.out.println("Statement: " + select);
				System.out.println("SELECT items: " + select.getPlainSelect().getSelectItems());
				System.out.println("WHERE expression: " + select.getPlainSelect().getWhere());
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}


	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root object of the operator tree. Writes the result to `outputFile`.
	 * @param root       The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
	public static void execute(Operator root, String outputFile) {
		try {

			// Ensure the output directory exists
			File outputFileObj = new File(outputFile);
			File parentDir = outputFileObj.getParentFile();
			if (parentDir != null && !parentDir.exists()) {
				boolean created = parentDir.mkdirs();
				if (!created) {
					System.err.println("Failed to create output directory: " + parentDir.getAbsolutePath());
				}
			}

			// Create a BufferedWriter
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

			// Iterate over the tuples produced by root
			Tuple tuple = root.getNextTuple();

//			System.out.println("first tuple from query plan: " + (tuple == null ? "null" : tuple));

			while (tuple != null) {
				writer.write(tuple.toString());
				writer.newLine();
				tuple = root.getNextTuple();
			}


			// Close the writer
			writer.close();

			System.out.println("Query executed successfully!");
			System.out.println("Output file: " + outputFile);

			// Initialize map to track operator type counts
//			Map<String, Integer> operatorCounts = new HashMap<>();
			// Report and accumulate operator counts
//			reportOperatorCounts(root, operatorCounts);

			// Print total counts by operator type
//			System.out.println("\n--- Total Operator Counts ---");
//			int grandTotal = 0;
//			for (Map.Entry<String, Integer> entry : operatorCounts.entrySet()) {
//				System.out.println(entry.getKey() + ": " + entry.getValue() + " tuples processed");
//				grandTotal += entry.getValue();
//			}
//			System.out.println("Grand total: " + grandTotal + " tuples processed across all operators");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

//	private static void reportOperatorCounts(Operator op, Map<String, Integer> operatorCounts) {
//		if (op == null) return;
//
//		String operatorType = op.getClass().getSimpleName();
//		int tupleCount = op.getTupleCounter();
//
//		System.out.println(operatorType + ": " + tupleCount + " tuples produced.");
//
//		// Accumulate counts by operator type
//		operatorCounts.put(
//				operatorType,
//				operatorCounts.getOrDefault(operatorType, 0) + tupleCount
//		);
//
//		// Recursive call to children if exist
//		if (op.hasChild()) {
//			reportOperatorCounts(op.getChild(), operatorCounts);
//		}
//
//		if (op instanceof JoinOperator) {
//			reportOperatorCounts(((JoinOperator) op).getOuterChild(), operatorCounts);
//		}
//	}
}
