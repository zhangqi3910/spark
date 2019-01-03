package org.apache.hive.storage.jdbc.write.type.oracle;

public enum OracleJdbcType {
    INT("INT"),
    INTEGER("INTEGER"),
    //oracle
    NUMBER("NUMBER"),
    VARCHAR2("VARCHAR2"),
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    LONG("LONG"),
    FLOAT("FLOAT"),
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    RAW("RAW"),
    BLOB("BLOB"),
    CLOB("CLOB"),
    DECIMAL("DECIMAL");


    private String type;

    OracleJdbcType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static OracleJdbcType getJdbcTypeByTypeName(String jdbcTypeName) {
        for (OracleJdbcType oracleJdbcType : values()) {
            if (oracleJdbcType.getType().equalsIgnoreCase(jdbcTypeName)) {
                return oracleJdbcType;
            } else if (jdbcTypeName.toLowerCase().contains("char") && oracleJdbcType.getType().equalsIgnoreCase("char")) {
                return oracleJdbcType;
            } else if (jdbcTypeName.toLowerCase().contains("varchar2") && oracleJdbcType.getType().equalsIgnoreCase("varchar2")) {
                return oracleJdbcType;
            } else if (jdbcTypeName.toLowerCase().contains("varchar") && oracleJdbcType.getType().equalsIgnoreCase("varchar")) {
                return oracleJdbcType;
            } else if (jdbcTypeName.toLowerCase().contains("raw") && oracleJdbcType.getType().equalsIgnoreCase("raw")) {
                return oracleJdbcType;
            } else if (jdbcTypeName.toLowerCase().contains("decimal") && oracleJdbcType.getType().equalsIgnoreCase("decimal")) {
                return oracleJdbcType;
            }
        }
        return null;
    }
}
