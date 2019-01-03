package org.apache.hive.storage.jdbc.write.type;

public class JdbcFieldTypeBean {
    private String field;

    private String columnTypeName;

    private String hiveColumnTypeName;

    public JdbcFieldTypeBean(String field, String columnTypeName, String hiveColumnTypeName) {
        this.field = field;
        this.columnTypeName = columnTypeName;
        this.hiveColumnTypeName = hiveColumnTypeName;
    }

    public String getHiveColumnTypeName() {
        return hiveColumnTypeName;
    }

    public void setHiveColumnTypeName(String hiveColumnTypeName) {
        this.hiveColumnTypeName = hiveColumnTypeName;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getColumnTypeName() {
        return columnTypeName;
    }

    public void setColumnTypeName(String columnTypeName) {
        this.columnTypeName = columnTypeName;
    }
}
