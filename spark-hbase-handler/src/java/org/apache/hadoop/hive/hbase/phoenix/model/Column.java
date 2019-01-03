package org.apache.hadoop.hive.hbase.phoenix.model;

public class Column {
    private String columnName;

    private String dataType;

    private boolean isRowKey = false;

    private String columnFamily;

    public Column() {
    }

    public Column(String columnName, String dataType) {
        this.columnName = columnName;
        this.dataType = dataType;
    }

    public Column(String columnName, String dataType, boolean isRowKey) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.isRowKey = isRowKey;
    }

    public Column(String columnName, String dataType, boolean isRowKey, String columnFamily) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.isRowKey = isRowKey;
        this.columnFamily = columnFamily;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public boolean isRowKey() {
        return isRowKey;
    }

    public void setRowKey(boolean rowKey) {
        isRowKey = rowKey;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}
