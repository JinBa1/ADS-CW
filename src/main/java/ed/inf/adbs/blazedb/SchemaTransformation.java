package ed.inf.adbs.blazedb;

import java.util.Map;

// Class to store transformation details
public class SchemaTransformation {
    private final SchemaTransformationType type;
    private final Map<String, String> details;

    public SchemaTransformation(SchemaTransformationType type, Map<String, String> details) {
        this.type = type;
        this.details = details;
    }

    // Constructor, getters, etc.

    public SchemaTransformationType getType() {
        return type;
    }

    public Map<String, String> getDetails() {
        return details;
    }
}