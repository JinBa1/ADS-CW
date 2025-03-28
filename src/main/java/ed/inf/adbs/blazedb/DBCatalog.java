package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.schema.Column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class DBCatalog {

    private static DBCatalog instance;

    private final Map<String, Path> dBLocations;
    private final Map<String, Map<String, Integer>> dBSchemata;

    private final Map<String, Map<String, Integer>> intermediateSchemata;

    private DBCatalog() {
        dBLocations = new HashMap<>();
        dBSchemata = new HashMap<>();
        intermediateSchemata = new HashMap<>();
    }

    public static DBCatalog getInstance() {
        if (instance == null) {
            instance = new DBCatalog();
            System.out.println("Created DBCatalog, but haven't load content, use initDBCatalog() instead");
        }
        return instance;
    }

    public static void initDBCatalog(String dBDirectory) {
        if (instance == null) {
            instance = new DBCatalog();
            instance.loadDBCatalog(dBDirectory);
        }
    }

    public static void resetDBCatalog() {
        instance = null;
    }

    private void loadDBCatalog(String dBDirectory) {
        try {
            Path dBPath = Paths.get(dBDirectory);
            Path schemaPath = dBPath.resolve("schema.txt");
            Path dataPath = dBPath.resolve("data");
            try (BufferedReader schemaReader = Files.newBufferedReader(schemaPath)) {
                String line;
                while ((line = schemaReader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    String tableName = parts[0];

                    Map<String, Integer> columnMap = new HashMap<>();
                    for (int i = 1; i < parts.length; i++) {
                        columnMap.put(parts[i].toLowerCase(), i - 1); //to lower case
                    }

                    dBSchemata.put(tableName, columnMap);

                    Path dataFilePath = dataPath.resolve(tableName + ".csv");
                    dBLocations.put(tableName, dataFilePath);
                }
            }

            //Verify data file exists
            if (Files.exists(dataPath) && Files.isDirectory(dataPath)) {
                try (Stream<Path> files = Files.list(dataPath)) {
                    files.forEach(file -> {
                        String fileName = file.getFileName().toString();
                        if (fileName.endsWith(".csv")) {
                            String tableName = fileName.substring(0, fileName.length() - 4);
                            if (!dBSchemata.containsKey(tableName)) {
                                System.err.println("Warning: Found data file " + fileName +
                                        " but no schema definition");
                            }
                        }
                    });
                }
            }



        } catch (Exception e) {
            System.err.println("Error loading database schema: " + e.getMessage());
            e.printStackTrace();
        }

    }

    public Path getDBLocation(String tableName) {
        return dBLocations.get(tableName);
    }

    public Map<String, Integer> getDBSchemata(String tableName) {
        return dBSchemata.get(tableName);
    }

    public Integer getDBColumnName(String tableName, String columnName) {
        // need to capture null schema from get table
        return dBSchemata.get(tableName).get(columnName.toLowerCase());
    }

    public boolean tableExists(String tableName) {
        return (dBLocations.containsKey(tableName) && dBSchemata.containsKey(tableName));
    }

    public boolean columnExists(String tableName, String columnName) {
        if (!tableExists(tableName)) {
            return false;
        }
        return dBSchemata.get(tableName).containsKey(columnName.toLowerCase());
    }


    public String registerIntermediateSchema(Map<String, Integer> schema) {
        String schemaId = "temp_" + UUID.randomUUID().toString().substring(0, 8);
        intermediateSchemata.put(schemaId, schema);
        return schemaId;
    }

    public Integer getIntermediateColumnName(String schemaId, String tableName, String columnName) {
        if (!intermediateSchemata.containsKey(schemaId)) {
            return null;
        }

        Map<String, Integer> schema = intermediateSchemata.get(schemaId);
        String key = tableName + "." + columnName.toLowerCase();
        return schema.get(key);
    }

    public String registerJoinSchema(String leftSchemaId, String rightSchemaId, String rightTableName) {
        // Get the source schemas
        Map<String, Integer> leftSchemaMap = leftSchemaId.startsWith("temp_")
                ? intermediateSchemata.get(leftSchemaId)
                : dBSchemata.get(leftSchemaId);

        Map<String, Integer> rightSchemaMap = rightSchemaId.startsWith("temp_")
                ? intermediateSchemata.get(rightSchemaId)
                : dBSchemata.get(rightSchemaId);

        if (leftSchemaMap == null || rightSchemaMap == null) {
            return null;
        }

        // Determine the size of the left tuple for index adjustment
        int leftTupleSize = 0;
        for (Map.Entry<String, Integer> entry : leftSchemaMap.entrySet()) {
            leftTupleSize = Math.max(leftTupleSize, entry.getValue() + 1);
        }

        // Create a new joined schema
        Map<String, Integer> joinedSchema = new HashMap<>();

        // Add left schema entries with proper formatting
        String leftTableName = leftSchemaId;
        if (leftSchemaId.startsWith("temp_")) {
            // For intermediate schemas, keep original keys
            for (Map.Entry<String, Integer> entry : leftSchemaMap.entrySet()) {
                joinedSchema.put(entry.getKey(), entry.getValue());
            }
        } else {
            // For base tables, format keys as "table.column"
            for (Map.Entry<String, Integer> entry : leftSchemaMap.entrySet()) {
                String columnName = entry.getKey();
                String key = leftSchemaId + "." + columnName.toLowerCase();
                joinedSchema.put(key, entry.getValue());
            }
        }

        // Add right schema entries with proper formatting
        if (rightSchemaId.startsWith("temp_")) {
            // For intermediate schemas, keep original keys but adjust indices
            for (Map.Entry<String, Integer> entry : rightSchemaMap.entrySet()) {
                joinedSchema.put(entry.getKey(), entry.getValue() + leftTupleSize);
            }
        } else {
            // For base tables, format keys as "table.column" and adjust indices
            for (Map.Entry<String, Integer> entry : rightSchemaMap.entrySet()) {
                String columnName = entry.getKey();
                String key = rightSchemaId + "." + columnName.toLowerCase();
                joinedSchema.put(key, entry.getValue() + leftTupleSize);
            }
        }

        return registerIntermediateSchema(joinedSchema);
    }


    public static Integer resolveColumnIndex(String schemaId, String tableName, String columnName) {
        DBCatalog catalog = DBCatalog.getInstance();

        if (schemaId.startsWith("temp_")) {
            return catalog.getIntermediateColumnName(schemaId, tableName, columnName);
        } else {
            return catalog.getDBColumnName(tableName, columnName);
        }
    }
}
