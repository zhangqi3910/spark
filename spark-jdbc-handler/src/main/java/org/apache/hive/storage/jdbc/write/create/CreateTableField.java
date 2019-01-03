package org.apache.hive.storage.jdbc.write.create;

public class CreateTableField {

    private String hiveField;

    private String jdbcField;

    private String hiveTypeName;


    public CreateTableField(String hiveField, String jdbcField) {
        this.hiveField = hiveField;
        this.jdbcField = jdbcField;
    }

    public CreateTableField() {
    }

    public CreateTableField(String hiveField, String jdbcField, String hiveTypeName) {
        this.hiveField = hiveField;
        this.jdbcField = jdbcField;
        this.hiveTypeName = hiveTypeName;
    }

    public String getHiveTypeName() {
        return hiveTypeName;
    }

    public void setHiveTypeName(String hiveTypeName) {
        this.hiveTypeName = hiveTypeName;
    }

    public String getHiveField() {
        return hiveField;
    }

    public void setHiveField(String hiveField) {
        this.hiveField = hiveField;
    }

    public String getJdbcField() {
        return jdbcField;
    }

    public void setJdbcField(String jdbcField) {
        this.jdbcField = jdbcField;
    }
}
