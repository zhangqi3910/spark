package org.apache.hive.storage.jdbc.read.type.mysql;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.read.type.IJdbcTypeChange;

import java.math.BigDecimal;

public class HiveIntMysqlJdbc implements IJdbcTypeChange {

    @Override
    public Object exChangeJdbcType(Object value, String jdbcTypeName) throws HiveJdbcDatabaseAccessException {

        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).intValue();
        } else if (value instanceof Integer) {
            return value;
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                throw new HiveJdbcDatabaseAccessException(e);
            }
        }
        return null;
    }
}
