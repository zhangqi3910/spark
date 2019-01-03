package org.apache.hive.storage.jdbc.read.type;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

public interface IJdbcTypeChange {
    Object exChangeJdbcType(Object value, String jdbcTypeName) throws HiveJdbcDatabaseAccessException;
}
