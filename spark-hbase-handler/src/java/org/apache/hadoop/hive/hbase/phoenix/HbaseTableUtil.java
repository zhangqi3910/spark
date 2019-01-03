package org.apache.hadoop.hive.hbase.phoenix;

import org.apache.hadoop.hive.hbase.phoenix.model.Column;
import org.apache.hadoop.hive.hbase.phoenix.model.Table;

import java.util.ArrayList;
import java.util.List;

public class HbaseTableUtil {

    public static Table getTable(String ddl, String mapping, String keyMaping) {
        mapping = resetMaping(mapping, keyMaping, ddl);
        Table table = getTable(ddl, mapping);
        table.setKeys(keyMaping);
        return table;
    }

    public static String resetMaping(String maping, String keyMaping, String ddl) {
        StringBuilder stringBuilder = new StringBuilder();
        String[] tmpArr = maping.split(",");
        for (int i = 0; i < tmpArr.length; i++) {
            String columnName = tmpArr[i];
            if (i == 0) {
                stringBuilder.append(columnName).append(",");
                continue;
            }
            String hiveColumnName = getKeyMapingColumnName(keyMaping, ddl, i);
            if (hiveColumnName.isEmpty()) {
                stringBuilder.append(columnName).append(",");
            } else {
                stringBuilder.append(columnName.replace(":key", "0:" + hiveColumnName)).append(",");
            }
        }
        return stringBuilder.toString().substring(0, stringBuilder.length() - 1);
    }

    public static String getKeyMapingColumnName(String keyMaping, String ddl, int i) {
        String hiveColumnName = getColumnName(ddl, i);
        if (keyMaping.toLowerCase().contains(hiveColumnName.toLowerCase())) {
            return hiveColumnName;
        }
        return "";
    }


    public static String getColumnName(String ddl, int i) {
        String columnString = ddl.substring(ddl.indexOf("{") + 1, ddl.indexOf("}")).trim();
        return columnString.split(",")[i].trim().split(" ")[1];
    }

    public static Table getTable(String ddl, String mapping) {
        List<Column> columnList = new ArrayList<>();
        String columnString = ddl.substring(ddl.indexOf("{") + 1, ddl.indexOf("}")).trim();
        for (String columnAndDataType : columnString.split(", ")) {
            String[] columnTmp = columnAndDataType.trim().split(" ");
            String dataType = columnTmp[0];
            String columnName = columnTmp[1];
            Column column = new Column(columnName, dataType);
            String[] arr = getColumnAndFamily(columnName, mapping);
            if (null == arr || (arr.length == 2 && arr[0].isEmpty())) {
                column.setRowKey(true);
            } else {
                column.setColumnFamily(arr[0]);
            }
            columnList.add(column);
        }
        return new Table(columnList);
    }

    private static String[] getColumnAndFamily(String columnName, String mapping) {
        for (String key : mapping.trim().split(",")) {
            String[] arr = key.trim().split(":");
            if (arr[1].replaceAll("#b|#s", "").equalsIgnoreCase(columnName)) {
                return arr;
            }
        }
        return null;
    }
}
