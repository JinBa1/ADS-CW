package ed.inf.adbs.blazedb;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;



public class DBCatalog {

    private static DBCatalog instance;

    private final Map<String, Path> dBLocations;
    private final Map<String, Map<String, Integer>> dBSchemata;

    private final Map<String, Map<String, Integer>> intermediateSchemata;

    // Add schema transformation tracking
    private final Map<String, String> schemaParentMap; // child schema ID -> parent schema ID
    private final Map<String, SchemaTransformation> schemaTransformations; // schema ID -> transformation details

    private final Map<String, List<String>> schemaMultiParentMap;

    private final Map<String, Map<String, String>> columnOriginMap;


    private DBCatalog() {
        dBLocations = new HashMap<>();
        dBSchemata = new HashMap<>();
        intermediateSchemata = new HashMap<>();


        schemaParentMap = new HashMap<>();
        schemaTransformations = new HashMap<>();

        schemaMultiParentMap = new HashMap<>();

        columnOriginMap = new HashMap<>();
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


    public Map<String, Integer> getIntermediateSchema(String schemaId) {
        if (!intermediateSchemata.containsKey(schemaId)) {
            return null;
        }
        return intermediateSchemata.get(schemaId);
    }

    // Register a schema with transformation information
    public String registerSchemaWithTransformation(Map<String, Integer> schema,
                                                   String parentSchemaId,
                                                   SchemaTransformationType type,
                                                   Map<String, String> transformationDetails) {
        String schemaId = registerIntermediateSchema(schema);

        if (schemaId == null) {
            System.err.println("ERROR: Failed to register intermediate schema");
            return null;
        }

        // Record parent relationship
        if (parentSchemaId != null) {
            schemaParentMap.put(schemaId, parentSchemaId);
        }

        // Record transformation details
        schemaTransformations.put(schemaId, new SchemaTransformation(type, transformationDetails));

        // Track column origins for the new schema
        Map<String, String> originMap = new HashMap<>();
        columnOriginMap.put(schemaId, originMap);

        // For each column in the new schema, record its origin
        for (Map.Entry<String, String> detail : transformationDetails.entrySet()) {
            if (detail.getKey().contains(".")) {
                originMap.put(detail.getKey(), detail.getKey()); // Self-reference for existing qualified names
            }
        }

        return schemaId;
    }

    // Get information about schema lineage
    public String getParentSchemaId(String schemaId) {
        return schemaParentMap.get(schemaId);
    }

    public SchemaTransformation getSchemaTransformation(String schemaId) {
        return schemaTransformations.get(schemaId);
    }

    // Get schema derivation path - useful for optimization
    public List<String> getSchemaLineage(String schemaId) {
        List<String> lineage = new ArrayList<>();

        String currentId = schemaId;
        while (currentId != null) {
            lineage.add(currentId);
            currentId = schemaParentMap.get(currentId);
        }

        return lineage;
    }

    public Integer resolveColumnThroughTransformations(String finalSchemaId,
                                                       String tableName,
                                                       String columnName) {
        // First try direct resolution in the final schema
        Integer directIndex = smartResolveColumnIndex(finalSchemaId, tableName, columnName);
        if (directIndex != null) {
            return directIndex;
        }

        // If not found, check the transformation chain
        List<String> lineage = getSchemaLineage(finalSchemaId);
        for (String schemaId : lineage) {
            SchemaTransformation transformation = getSchemaTransformation(schemaId);
            if (transformation == null) continue;

            String columnKey = tableName + "." + columnName.toLowerCase();

            switch (transformation.getType()) {
                case PROJECTION:
                    // In projection, source column is directly mapped
                    String sourceIndexStr = transformation.getDetails().get(columnKey);
                    if (sourceIndexStr != null) {
                        return Integer.parseInt(sourceIndexStr);
                    }
                    break;

                case JOIN:
                    // For joins, need to check which parent schema contains the column
                    Map<String, String> details = transformation.getDetails();
                    String sourceInfo = details.get(columnKey);
                    if (sourceInfo != null && sourceInfo.contains(":")) {
                        String[] parts = sourceInfo.split(":");
                        String sourceSchemaId = parts[0];
                        int sourceIndex = Integer.parseInt(parts[1]);
                        return sourceIndex;
                    }
                    break;

                case AGGREGATION:
                    // For aggregation, check if it's a group by column
                    String groupInfo = transformation.getDetails().get(columnKey);
                    if (groupInfo != null && groupInfo.startsWith("group_by:")) {
                        String sourceIndex = groupInfo.substring("group_by:".length());
                        return Integer.parseInt(sourceIndex);
                    }
                    break;

                case OTHER:
                    // For selections and other transformations that don't change structure
                    // Just delegate to parent schema
                    String parentId = getParentSchemaId(schemaId);
                    if (parentId != null) {
                        return smartResolveColumnIndex(parentId, tableName, columnName);
                    }
                    break;
            }
        }

        // If we get here, we couldn't resolve through transformations
        return null;
    }

    // Add a parent schema (for operators with multiple inputs like JOIN)
    public void addParentSchema(String childSchemaId, String parentSchemaId) {
        schemaMultiParentMap.computeIfAbsent(childSchemaId, k -> new ArrayList<>())
                .add(parentSchemaId);
    }

    // Get all parent schemas (useful for joins)
    public List<String> getAllParentSchemas(String schemaId) {
        return schemaMultiParentMap.getOrDefault(schemaId, Collections.emptyList());
    }

    public static Integer smartResolveColumnIndex(String schemaId, String tableName, String columnName) {
        DBCatalog catalog = DBCatalog.getInstance();
        Map<String, Integer> schema = catalog.getSchema(schemaId);
        System.out.println("DEBUG RESOLVE: Looking for column " + tableName + "." + columnName + " in schema " + schemaId);
        if (schema == null) return null;

        // Try qualified name first
        String qualifiedKey = tableName + "." + columnName.toLowerCase();
        Integer index = schema.get(qualifiedKey);

        // If not found, try just the column name
        if (index == null) {
            index = schema.get(columnName.toLowerCase());
        }

        return index;
    }

    private Map<String, Integer> getSchema(String schemaId) {
        if (schemaId.startsWith("temp_")) {
            return intermediateSchemata.get(schemaId);
        } else {
            return dBSchemata.get(schemaId);
        }
    }

    // Enhanced method to resolve columns considering origins
    public Integer resolveColumnWithOrigins(String schemaId, String tableName, String columnName) {
        // Try direct resolution first
        Integer directIndex = smartResolveColumnIndex(schemaId, tableName, columnName);
        if (directIndex != null) return directIndex;

        // Try resolving through origin tracking
        Map<String, String> originMap = columnOriginMap.get(schemaId);
        if (originMap != null) {
            String lookupKey = tableName + "." + columnName.toLowerCase();

            // Look for any column that maps to this original name
            for (Map.Entry<String, String> entry : originMap.entrySet()) {
                if (entry.getValue().equalsIgnoreCase(lookupKey)) {
                    return getIntermediateSchema(schemaId).get(entry.getKey());
                }
            }
        }

        // Try parent schemas if needed
        List<String> parents = getAllParentSchemas(schemaId);
        for (String parent : parents) {
            Integer parentResult = resolveColumnWithOrigins(parent, tableName, columnName);
            if (parentResult != null) return parentResult;
        }

        return null;
    }
}
