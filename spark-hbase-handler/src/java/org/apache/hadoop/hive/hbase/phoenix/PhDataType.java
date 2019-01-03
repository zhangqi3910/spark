package org.apache.hadoop.hive.hbase.phoenix;

public enum PhDataType {
    INTEGER("INTEGER"),
    UNSIGNED_INT("UNSIGNED_INT"),
    DEFAULT("DEFAULT"),
    BIGINT("i64"),
    INT("i32"),
    TINYINT("byte"),
    SMALLINT("i16"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    BOOLEAN("BOOL"),
    VARCHAR("VARCHAR"),
    CHAR("CHAR"),
    TIMESTAMP ("TIMESTAMP"),
    BINARY ("BINARY"),
    DATE ("DATE"),
    STRING("string"),
    DECIMAL("decimal");

    private String dataType;

    public String getDataType() {
        return dataType;
    }

    PhDataType(String dataType) {
        this.dataType = dataType;
    }

    public static PhDataType getPhDataType(String dataType) {
    	dataType = dataType.split("\\(")[0];
        for (PhDataType phDataType : PhDataType.values()) {
            if (phDataType.dataType.equalsIgnoreCase(dataType)) {
                return phDataType;
            }
        }
        return DEFAULT;
    }
}
