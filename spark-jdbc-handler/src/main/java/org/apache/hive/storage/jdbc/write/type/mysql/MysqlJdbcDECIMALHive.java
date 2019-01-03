package org.apache.hive.storage.jdbc.write.type.mysql;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

import java.math.BigDecimal;

public class MysqlJdbcDECIMALHive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) throws HiveJdbcDatabaseAccessException {
        if (hiveType.equals(HiveCreateType.DOUBLE) ||
                hiveType.equals(HiveCreateType.FLOAT) ||
                hiveType.equals(HiveCreateType.BIGINT) ||
                hiveType.equals(HiveCreateType.DECIMAL)) {
            return new BigDecimal(String.valueOf(value));
        } else {
            throw new HiveJdbcDatabaseAccessException("hive and jdbc type mapping no support.");
        }
    }
}
