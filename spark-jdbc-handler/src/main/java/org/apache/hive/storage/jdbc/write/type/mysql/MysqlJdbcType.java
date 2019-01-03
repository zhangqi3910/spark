package org.apache.hive.storage.jdbc.write.type.mysql;

public enum MysqlJdbcType {

    INT("INT"),
    VARCHAR("VARCHAR"),
    CHAR("CHAR"),
    TINYINT("TINYINT"),
    DECIMAL("DECIMAL"),
    TEXT("TEXT"),
    SMALLINT("SMALLINT"),
    BIGINT("BIGINT"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    BIT("BIT"),
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    TIMESTAMP("TIMESTAMP"),
    DATETIME("DATETIME"),
    DATE("DATE");


    private String type;

    MysqlJdbcType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static MysqlJdbcType getJdbcTypeByTypeName(String jdbcTypeName) {
        for (MysqlJdbcType mysqlJdbcType : values()) {
            if (mysqlJdbcType.getType().equalsIgnoreCase(jdbcTypeName)) {
                return mysqlJdbcType;
            } else if (jdbcTypeName.toLowerCase().contains("decimal") && mysqlJdbcType.getType().equalsIgnoreCase("decimal")) {
                return mysqlJdbcType;
            } else if (jdbcTypeName.toLowerCase().contains("varchar") && mysqlJdbcType.getType().equalsIgnoreCase("varchar")) {
                return mysqlJdbcType;
            } else if (jdbcTypeName.toLowerCase().contains("char") && mysqlJdbcType.getType().equalsIgnoreCase("char")) {
                return mysqlJdbcType;
            }
        }
        return null;
    }

}
