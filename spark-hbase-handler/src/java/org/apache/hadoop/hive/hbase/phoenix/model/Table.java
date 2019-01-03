package org.apache.hadoop.hive.hbase.phoenix.model;

import java.util.ArrayList;
import java.util.List;

public class Table {
    private List<Column> columnList;

    private List<Column> keys;

    public Table() {
    }

    public List<Column> getKeys() {
        return keys;
    }

    public void setKeys(List<Column> keys) {
        this.keys = keys;
    }

    public Table(List<Column> columnList, List<Column> keys) {
        this.columnList = columnList;
        this.keys = keys;
    }

    public Table(List<Column> columnList) {
        this.columnList = columnList;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    public Column getRowKeyColumn() {
        for (Column column : columnList) {
            if (column.isRowKey()) {
                return column;
            }
        }
        return null;
    }

    public void setKeys(String keyMaping) {
        keys = new ArrayList<>();
        String[] columnNameArr = keyMaping.split(",");
        for (int i = 0; i < columnNameArr.length; i++) {
            if (i == 0) {
                continue;
            }
            Column column = getColumn(columnNameArr[i]);
            keys.add(column);
        }
    }

    public Column getColumn(String columnName) {
        for (Column column : columnList) {
            if (column.getColumnName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }
}
