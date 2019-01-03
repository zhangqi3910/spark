package org.apache.hive.storage.jdbc.read.type;

public enum HiveType {
    INT("int"),
    DOUBLE("double"),
    DECIMAL("decimal"),
    CHAR("char"),
    DATE("date"),
    TIMESTAMP("timestamp"),
    LONG("long"),
    FLOAT("float"),
    VARCHAR("varchar"),
    STRING("string");


    private String type;

    HiveType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static HiveType getHiveTypeByType(String hiveColumnType) {
        for (HiveType hiveType : values()) {
            if (hiveType.getType().equalsIgnoreCase(hiveColumnType)) {
                return hiveType;
            }
        }
        return null;
    }
}
