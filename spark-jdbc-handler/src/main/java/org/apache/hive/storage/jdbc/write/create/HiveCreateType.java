package org.apache.hive.storage.jdbc.write.create;

public enum HiveCreateType {
    INT("INT"),
    INTEGER("INTEGER"),
    STRING("STRING"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    BIGINT("BIGINT"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    TIMESTAMP("TIMESTAMP"),
    BOOLEAN("BOOLEAN"),
    VARCHAR("VARCHAR"),
    CHAR("CHAR"),
    DATE("DATE"),
    BINARY("BINARY");

    private String type;

    HiveCreateType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }


    public static HiveCreateType getHiveCreateTypeByHiveType(String hiveTypeName) {
        for (HiveCreateType hiveCreateType : values()) {
            if (hiveCreateType.getType().equalsIgnoreCase(hiveTypeName)) {
                return hiveCreateType;
            } else if (hiveTypeName.toLowerCase().contains("decimal") && hiveCreateType.type.equalsIgnoreCase("decimal")) {
                return hiveCreateType;
            } else if (hiveTypeName.toLowerCase().startsWith("char") && hiveCreateType.getType().equalsIgnoreCase("char")) {
                return hiveCreateType;
            } else if (hiveTypeName.toLowerCase().startsWith("varchar") && hiveCreateType.getType().equalsIgnoreCase("varchar")) {
                return hiveCreateType;
            }
        }
        return null;
    }
}
