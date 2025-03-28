package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.DBCatalog;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class ScanOperator extends Operator {

    private final Path tablePath;
    private final String tableName;
    private BufferedReader reader;

    public ScanOperator(String tableName) {
        this.tableName = tableName;
        tablePath = DBCatalog.getInstance().getDBLocation(tableName);
        child = null; // Scan cannot have child operator

        openReader();
    }

    private void openReader() {
        try {
            reader = Files.newBufferedReader(tablePath);
        } catch (IOException e) {
            System.err.println("Failed to open reader for table " + tableName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) { // END OF FILE
                closeReader();
                return null;
            }

            String[] values = line.split(",\\s*");
            ArrayList<Integer> attributes = new ArrayList<>();
            for (String value : values) {
                attributes.add(Integer.parseInt(value.trim()));
            }

            tupleCounter ++;
            return new Tuple(attributes);
        } catch (IOException e) {
            System.err.println("Error reading tuple: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void reset() {
        try {
            if (reader != null) {
                reader.close();
            }
            openReader();
        } catch (IOException e) {
            System.err.println("Error reading tuple: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public String propagateTableName() {
        return tableName;
    }

    @Override
    public String propagateSchemaId() {
        return tableName;
    }

    private void closeReader() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing reader for table " + tableName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void close() {
        closeReader();
    }
}
