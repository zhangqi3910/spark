package org.apache.hive.storage.jdbc.read.type;

public class JdbcFieldAttr {
    private String jdbcTypeName;

    private Object value;

    public JdbcFieldAttr() {
    }

    public JdbcFieldAttr(String jdbcTypeName, Object value) {
        this.jdbcTypeName = jdbcTypeName;
        this.value = value;
    }

    public String getJdbcTypeName() {
        return jdbcTypeName;
    }

    public void setJdbcTypeName(String jdbcTypeName) {
        this.jdbcTypeName = jdbcTypeName;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
