package org.apache.hive.storage.jdbc.read.type.mysql;

import org.apache.hive.storage.jdbc.read.type.IJdbcTypeChange;

public class HiveTimestampMysqlJdbc implements IJdbcTypeChange {
    @Override
    public Object exChangeJdbcType(Object value, String jdbcTypeName) {
        return null;
    }
}
