package org.apache.hive.storage.jdbc.write.type.oracle;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

import java.math.BigDecimal;

public class OracleJdbcLONGHive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) throws HiveJdbcDatabaseAccessException {
        if (hiveType.equals(HiveCreateType.BIGINT)) {
            return value;
        } else if (hiveType.equals(HiveCreateType.INTEGER) ||
                hiveType.equals(HiveCreateType.INT)) {
            return Long.parseLong(String.valueOf(value));
        } else if (hiveType.equals(HiveCreateType.DECIMAL)) {
            return ((BigDecimal) value).longValue();
        } else {
            throw new HiveJdbcDatabaseAccessException("hive and jdbc type mapping no support.");
        }
    }
}
