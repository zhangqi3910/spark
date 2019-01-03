package org.apache.hive.storage.jdbc.read.type.mysql;

import org.apache.hive.storage.jdbc.read.type.IJdbcTypeChange;

public class HiveStringMysqlJdbc implements IJdbcTypeChange {

    @Override
    public Object exChangeJdbcType(Object value, String jdbcTypeName) {
        if (value instanceof String) {
            return value;
        }
        return String.valueOf(value);
    }
}
