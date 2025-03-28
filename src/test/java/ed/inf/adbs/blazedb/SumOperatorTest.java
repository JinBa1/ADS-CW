package ed.inf.adbs.blazedb;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.SumOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class SumOperatorTest {

    private static final String TEST_DB_DIR = "src/test/resources/testdb";
    private static final String SCHEMA_FILE = TEST_DB_DIR + "/schema.txt";
    private static final String DATA_DIR = TEST_DB_DIR + "/data";
    private static final String SALES_TABLE = "Sales";
    private static final String EMPTY_TABLE = "EmptySales";

    @Before
    public void setUp() throws IOException {
        // Create test database directory structure
        Files.createDirectories(Paths.get(DATA_DIR));

        // Create schema file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(SCHEMA_FILE))) {
            writer.write(SALES_TABLE + " product_id category qty price\n");
            writer.write(EMPTY_TABLE + " product_id category qty price\n");
        }

        // Create test data file with sales data - this is the base data for most tests
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + SALES_TABLE + ".csv"))) {
            writer.write("1, 1, 10, 5\n");    // product_id=1, category=1, qty=10, price=5 (revenue=50)
            writer.write("2, 1, 20, 10\n");   // product_id=2, category=1, qty=20, price=10 (revenue=200)
            writer.write("3, 2, 15, 8\n");    // product_id=3, category=2, qty=15, price=8 (revenue=120)
            writer.write("4, 2, 5, 15\n");    // product_id=4, category=2, qty=5, price=15 (revenue=75)
            writer.write("5, 2, 30, 7\n");    // product_id=5, category=2, qty=30, price=7 (revenue=210)
            // Note: Changed the category for product_id=5 from 3 to 2 to fix the test expectations
        }

        // Create empty table file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + EMPTY_TABLE + ".csv"))) {
            // Intentionally left empty
        }

        // Initialize the database catalog
        DBCatalog.initDBCatalog(TEST_DB_DIR);
    }

    @After
    public void tearDown() throws IOException {
        // Clean up test files
        Files.deleteIfExists(Paths.get(DATA_DIR + "/" + SALES_TABLE + ".csv"));
        Files.deleteIfExists(Paths.get(DATA_DIR + "/" + EMPTY_TABLE + ".csv"));
        Files.deleteIfExists(Paths.get(SCHEMA_FILE));
        Files.deleteIfExists(Paths.get(DATA_DIR));
        Files.deleteIfExists(Paths.get(TEST_DB_DIR));
    }

    @Test
    public void testGroupByWithSumAggregate() throws Exception {
        // Test group by category with sum(qty) aggregate
        ScanOperator scanOp = new ScanOperator(SALES_TABLE);

        // Create column objects for grouping by category
        List<Column> groupByColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(SALES_TABLE);
        Column categoryColumn = new Column();
        categoryColumn.setTable(table);
        categoryColumn.setColumnName("category");
        groupByColumns.add(categoryColumn);

        // Create sum(qty) expression
        List<Expression> sumExpressions = new ArrayList<>();
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        Column qtyColumn = new Column();
        qtyColumn.setTable(table);
        qtyColumn.setColumnName("qty");

        // In JSqlParser 4.7, we create an ExpressionList and set it as parameters
        ExpressionList exprList = new ExpressionList();
        exprList.addExpressions(qtyColumn);
        sumFunction.setParameters(exprList);

        sumExpressions.add(sumFunction);

        // Output columns same as group by columns
        List<Column> outputColumns = new ArrayList<>(groupByColumns);

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Get all result tuples
        List<Tuple> resultTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sumOp.getNextTuple()) != null) {
            resultTuples.add(tuple);
        }

        // Should have 2 groups (category 1 and 2)
        assertEquals("Should have 2 groups", 2, resultTuples.size());

        // Verify the sum of quantities for each category
        for (Tuple t : resultTuples) {
            int category = t.getAttribute(0);
            int sumQty = t.getAttribute(1);

            switch (category) {
                case 1:
                    assertEquals("Category 1 should have sum(qty)=30", 30, sumQty);
                    break;
                case 2:
                    assertEquals("Category 2 should have sum(qty)=50", 50, sumQty);
                    break;
                default:
                    fail("Unexpected category: " + category);
            }
        }
    }

    @Test
    public void testGroupByWithMultipleSumAggregates() throws Exception {
        // Test group by category with sum(qty) and sum(price) aggregates
        ScanOperator scanOp = new ScanOperator(SALES_TABLE);

        // Create column objects for grouping by category
        List<Column> groupByColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(SALES_TABLE);
        Column categoryColumn = new Column();
        categoryColumn.setTable(table);
        categoryColumn.setColumnName("category");
        groupByColumns.add(categoryColumn);

        // Create sum expressions: sum(qty) and sum(price)
        List<Expression> sumExpressions = new ArrayList<>();

        // sum(qty)
        Function sumQtyFunction = new Function();
        sumQtyFunction.setName("SUM");
        Column qtyColumn = new Column();
        qtyColumn.setTable(table);
        qtyColumn.setColumnName("qty");

        ExpressionList qtyExprList = new ExpressionList();
        qtyExprList.addExpressions(qtyColumn);
        sumQtyFunction.setParameters(qtyExprList);

        sumExpressions.add(sumQtyFunction);

        // sum(price)
        Function sumPriceFunction = new Function();
        sumPriceFunction.setName("SUM");
        Column priceColumn = new Column();
        priceColumn.setTable(table);
        priceColumn.setColumnName("price");

        ExpressionList priceExprList = new ExpressionList();
        priceExprList.addExpressions(priceColumn);
        sumPriceFunction.setParameters(priceExprList);

        sumExpressions.add(sumPriceFunction);

        // Output columns same as group by columns
        List<Column> outputColumns = new ArrayList<>(groupByColumns);

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Get all result tuples
        List<Tuple> resultTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sumOp.getNextTuple()) != null) {
            resultTuples.add(tuple);
        }

        // Should have 2 groups (category 1 and 2)
        assertEquals("Should have 2 groups", 2, resultTuples.size());

        // Verify the sums for each category
        for (Tuple t : resultTuples) {
            int category = t.getAttribute(0);
            int sumQty = t.getAttribute(1);
            int sumPrice = t.getAttribute(2);

            switch (category) {
                case 1:
                    assertEquals("Category 1 should have sum(qty)=30", 30, sumQty);
                    assertEquals("Category 1 should have sum(price)=15", 15, sumPrice);
                    break;
                case 2:
                    assertEquals("Category 2 should have sum(qty)=50", 50, sumQty);
                    assertEquals("Category 2 should have sum(price)=30", 30, sumPrice);
                    break;
                default:
                    fail("Unexpected category: " + category);
            }
        }
    }

    @Test
    public void testSumOfMultiplication() throws Exception {
        // Test sum(qty * price) - computing total revenue per category
        ScanOperator scanOp = new ScanOperator(SALES_TABLE);

        // Create column objects for grouping by category
        List<Column> groupByColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(SALES_TABLE);
        Column categoryColumn = new Column();
        categoryColumn.setTable(table);
        categoryColumn.setColumnName("category");
        groupByColumns.add(categoryColumn);

        // Create sum(qty * price) expression
        List<Expression> sumExpressions = new ArrayList<>();
        Function sumFunction = new Function();
        sumFunction.setName("SUM");

        // Create qty * price expression
        Column qtyColumn = new Column();
        qtyColumn.setTable(table);
        qtyColumn.setColumnName("qty");

        Column priceColumn = new Column();
        priceColumn.setTable(table);
        priceColumn.setColumnName("price");

        Multiplication multiplication = new Multiplication();
        multiplication.setLeftExpression(qtyColumn);
        multiplication.setRightExpression(priceColumn);

        ExpressionList multiExprList = new ExpressionList();
        multiExprList.addExpressions(multiplication);
        sumFunction.setParameters(multiExprList);

        sumExpressions.add(sumFunction);

        // Output columns same as group by columns
        List<Column> outputColumns = new ArrayList<>(groupByColumns);

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Get all result tuples
        List<Tuple> resultTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sumOp.getNextTuple()) != null) {
            resultTuples.add(tuple);
        }

        // Should have 2 groups (category 1 and 2)
        assertEquals("Should have 2 groups", 2, resultTuples.size());

        // Verify the total revenue for each category
        for (Tuple t : resultTuples) {
            int category = t.getAttribute(0);
            int totalRevenue = t.getAttribute(1);

            switch (category) {
                case 1:
                    // Category 1: (10*5) + (20*10) = 50 + 200 = 250
                    assertEquals("Category 1 should have revenue=250", 250, totalRevenue);
                    break;
                case 2:
                    // Category 2: (15*8) + (5*15) + (30*7) = 120 + 75 + 210 = 405
                    assertEquals("Category 2 should have revenue=405", 405, totalRevenue);
                    break;
                default:
                    fail("Unexpected category: " + category);
            }
        }
    }

    @Test
    public void testSumOfConstant() throws Exception {
        // Test SUM(1) - counting number of records per category
        ScanOperator scanOp = new ScanOperator(SALES_TABLE);

        // Create column objects for grouping by category
        List<Column> groupByColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(SALES_TABLE);
        Column categoryColumn = new Column();
        categoryColumn.setTable(table);
        categoryColumn.setColumnName("category");
        groupByColumns.add(categoryColumn);

        // Create SUM(1) expression
        List<Expression> sumExpressions = new ArrayList<>();
        Function sumFunction = new Function();
        sumFunction.setName("SUM");

        LongValue constValue = new LongValue(1);
        ExpressionList constExprList = new ExpressionList();
        constExprList.addExpressions(constValue);
        sumFunction.setParameters(constExprList);

        sumExpressions.add(sumFunction);

        // Output columns same as group by columns
        List<Column> outputColumns = new ArrayList<>(groupByColumns);

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Get all result tuples
        List<Tuple> resultTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sumOp.getNextTuple()) != null) {
            resultTuples.add(tuple);
        }

        // Should have 2 groups (category 1 and 2)
        assertEquals("Should have 2 groups", 2, resultTuples.size());

        // Verify the count for each category
        for (Tuple t : resultTuples) {
            int category = t.getAttribute(0);
            int count = t.getAttribute(1);

            switch (category) {
                case 1:
                    // Category 1 has 2 records
                    assertEquals("Category 1 should have count=2", 2, count);
                    break;
                case 2:
                    // Category 2 has 3 records
                    assertEquals("Category 2 should have count=3", 3, count);
                    break;
                default:
                    fail("Unexpected category: " + category);
            }
        }
    }

    @Test
    public void testNoGroupBy() throws Exception {
        // Test SUM without GROUP BY - should return a single row with total
        ScanOperator scanOp = new ScanOperator(SALES_TABLE);

        // Empty group by columns (no grouping)
        List<Column> groupByColumns = new ArrayList<>();

        // Create SUM(qty) expression
        List<Expression> sumExpressions = new ArrayList<>();
        Function sumFunction = new Function();
        sumFunction.setName("SUM");

        Table table = new Table();
        table.setName(SALES_TABLE);
        Column qtyColumn = new Column();
        qtyColumn.setTable(table);
        qtyColumn.setColumnName("qty");

        ExpressionList qtyExprList = new ExpressionList();
        qtyExprList.addExpressions(qtyColumn);
        sumFunction.setParameters(qtyExprList);

        sumExpressions.add(sumFunction);

        // No output columns (no group by)
        List<Column> outputColumns = new ArrayList<>();

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Get the result
        Tuple tuple = sumOp.getNextTuple();
        assertNotNull("Should return a result tuple", tuple);

        // Should have exactly one sum value
        assertEquals("Result tuple should have one attribute", 1, tuple.getTuple().size());

        // Sum of all qty values: 10 + 20 + 15 + 5 + 30 = 80
        assertEquals("Total sum(qty) should be 80", 80, tuple.getAttribute(0).intValue());

        // No more results
        assertNull("Should have no more result tuples", sumOp.getNextTuple());
    }

    @Test
    public void testEmptyTable() throws Exception {
        // Test with an empty table
        ScanOperator scanOp = new ScanOperator(EMPTY_TABLE);

        // Group by category
        List<Column> groupByColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(EMPTY_TABLE);
        Column categoryColumn = new Column();
        categoryColumn.setTable(table);
        categoryColumn.setColumnName("category");
        groupByColumns.add(categoryColumn);

        // Create sum(qty) expression
        List<Expression> sumExpressions = new ArrayList<>();
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        Column qtyColumn = new Column();
        qtyColumn.setTable(table);
        qtyColumn.setColumnName("qty");

        ExpressionList qtyExprList = new ExpressionList();
        qtyExprList.addExpressions(qtyColumn);
        sumFunction.setParameters(qtyExprList);

        sumExpressions.add(sumFunction);

        // Output columns same as group by columns
        List<Column> outputColumns = new ArrayList<>(groupByColumns);

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Should not return any tuples
        assertNull("Empty table should produce no results", sumOp.getNextTuple());
    }

    @Test
    public void testReset() throws Exception {
        // Test reset functionality
        ScanOperator scanOp = new ScanOperator(SALES_TABLE);

        // Group by category
        List<Column> groupByColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(SALES_TABLE);
        Column categoryColumn = new Column();
        categoryColumn.setTable(table);
        categoryColumn.setColumnName("category");
        groupByColumns.add(categoryColumn);

        // Create sum(qty) expression
        List<Expression> sumExpressions = new ArrayList<>();
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        Column qtyColumn = new Column();
        qtyColumn.setTable(table);
        qtyColumn.setColumnName("qty");

        ExpressionList qtyExprList = new ExpressionList();
        qtyExprList.addExpressions(qtyColumn);
        sumFunction.setParameters(qtyExprList);

        sumExpressions.add(sumFunction);

        // Output columns same as group by columns
        List<Column> outputColumns = new ArrayList<>(groupByColumns);

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Count results first time
        int firstRunCount = 0;
        while (sumOp.getNextTuple() != null) {
            firstRunCount++;
        }

        // Reset and count again
        sumOp.reset();

        int secondRunCount = 0;
        while (sumOp.getNextTuple() != null) {
            secondRunCount++;
        }

        // Both runs should return the same number of groups
        assertEquals("Should return same number of groups after reset",
                firstRunCount, secondRunCount);

        // Verify we have 2 groups (categories) as expected
        assertEquals("Should have 2 groups", 2, firstRunCount);
    }

    @Test
    public void testMultipleGroupByColumns() throws Exception {
        // Create test data with multiple attributes to group by
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + SALES_TABLE + ".csv"))) {
            writer.write("1, 1, 10, 5\n");    // product_id=1, category=1, qty=10, price=5
            writer.write("1, 2, 15, 8\n");    // product_id=1, category=2, qty=15, price=8
            writer.write("2, 1, 20, 10\n");   // product_id=2, category=1, qty=20, price=10
            writer.write("2, 2, 5, 15\n");    // product_id=2, category=2, qty=5, price=15
            writer.write("3, 1, 30, 7\n");    // product_id=3, category=1, qty=30, price=7
        }

        ScanOperator scanOp = new ScanOperator(SALES_TABLE);

        // Group by product_id and category
        List<Column> groupByColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(SALES_TABLE);

        Column productColumn = new Column();
        productColumn.setTable(table);
        productColumn.setColumnName("product_id");
        groupByColumns.add(productColumn);

        Column categoryColumn = new Column();
        categoryColumn.setTable(table);
        categoryColumn.setColumnName("category");
        groupByColumns.add(categoryColumn);

        // Create sum(qty) expression
        List<Expression> sumExpressions = new ArrayList<>();
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        Column qtyColumn = new Column();
        qtyColumn.setTable(table);
        qtyColumn.setColumnName("qty");

        ExpressionList qtyExprList = new ExpressionList();
        qtyExprList.addExpressions(qtyColumn);
        sumFunction.setParameters(qtyExprList);

        sumExpressions.add(sumFunction);

        // Output columns same as group by columns
        List<Column> outputColumns = new ArrayList<>(groupByColumns);

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Get all result tuples
        List<Tuple> resultTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sumOp.getNextTuple()) != null) {
            resultTuples.add(tuple);
        }

        // Should have 5 groups (product_id + category combinations)
        assertEquals("Should have 5 groups", 5, resultTuples.size());

        // Verify the sums for each group
        for (Tuple t : resultTuples) {
            int productId = t.getAttribute(0);
            int category = t.getAttribute(1);
            int sumQty = t.getAttribute(2);

            if (productId == 1 && category == 1) {
                assertEquals("Product 1, Category 1 should have sum(qty)=10", 10, sumQty);
            } else if (productId == 1 && category == 2) {
                assertEquals("Product 1, Category 2 should have sum(qty)=15", 15, sumQty);
            } else if (productId == 2 && category == 1) {
                assertEquals("Product 2, Category 1 should have sum(qty)=20", 20, sumQty);
            } else if (productId == 2 && category == 2) {
                assertEquals("Product 2, Category 2 should have sum(qty)=5", 5, sumQty);
            } else if (productId == 3 && category == 1) {
                assertEquals("Product 3, Category 1 should have sum(qty)=30", 30, sumQty);
            } else {
                fail("Unexpected group: product=" + productId + ", category=" + category);
            }
        }
    }

    @Test
    public void testSubsetOfGroupByColumnsAsOutput() throws Exception {
        // Create test data specifically for this test with controlled category values
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/" + SALES_TABLE + ".csv"))) {
            writer.write("1, 1, 10, 5\n");    // product_id=1, category=1, qty=10, price=5
            writer.write("1, 2, 15, 8\n");    // product_id=1, category=2, qty=15, price=8
            writer.write("2, 1, 20, 10\n");   // product_id=2, category=1, qty=20, price=10
            writer.write("2, 2, 5, 15\n");    // product_id=2, category=2, qty=5, price=15
            writer.write("3, 1, 30, 7\n");    // product_id=3, category=1, qty=30, price=7
        }

        ScanOperator scanOp = new ScanOperator(SALES_TABLE);

        // Group by product_id and category
        List<Column> groupByColumns = new ArrayList<>();
        Table table = new Table();
        table.setName(SALES_TABLE);

        Column productColumn = new Column();
        productColumn.setTable(table);
        productColumn.setColumnName("product_id");
        groupByColumns.add(productColumn);

        Column categoryColumn = new Column();
        categoryColumn.setTable(table);
        categoryColumn.setColumnName("category");
        groupByColumns.add(categoryColumn);

        // Create sum(qty) expression
        List<Expression> sumExpressions = new ArrayList<>();
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        Column qtyColumn = new Column();
        qtyColumn.setTable(table);
        qtyColumn.setColumnName("qty");

        ExpressionList qtyExprList = new ExpressionList();
        qtyExprList.addExpressions(qtyColumn);
        sumFunction.setParameters(qtyExprList);

        sumExpressions.add(sumFunction);

        // Output only category (subset of group by columns)
        List<Column> outputColumns = new ArrayList<>();
        outputColumns.add(categoryColumn);

        SumOperator sumOp = new SumOperator(scanOp, groupByColumns, sumExpressions, outputColumns);

        // Get all result tuples
        List<Tuple> resultTuples = new ArrayList<>();
        Tuple tuple;
        while ((tuple = sumOp.getNextTuple()) != null) {
            resultTuples.add(tuple);
        }

        // Should still have 5 groups (product_id + category combinations)
        assertEquals("Should have 5 groups", 5, resultTuples.size());

        // Each tuple should have 2 attributes: category and sum(qty)
        for (Tuple t : resultTuples) {
            assertEquals("Result tuple should have 2 attributes", 2, t.getTuple().size());

            // First attribute should be category (1 or 2)
            int category = t.getAttribute(0);
            assertTrue("Category should be 1 or 2", category == 1 || category == 2);

            // Second attribute should be sum(qty) for that specific product_id + category group
            int sumQty = t.getAttribute(1);
            assertTrue("Sum(qty) should be one of the expected values",
                    sumQty == 10 || sumQty == 15 || sumQty == 20 || sumQty == 5 || sumQty == 30);
        }
    }
}