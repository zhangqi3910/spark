package org.apache.hive.storage.jdbc.write.type.mysql;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

public class MysqlJdbcBIGINTHive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) throws HiveJdbcDatabaseAccessException {
        if (hiveType.equals(HiveCreateType.BIGINT)) {
            return value;
        } else if (hiveType.equals(HiveCreateType.INTEGER) ||
                hiveType.equals(HiveCreateType.TINYINT) ||
                hiveType.equals(HiveCreateType.SMALLINT)) {
            return Long.parseLong(String.valueOf(value));
        } else {
            throw new HiveJdbcDatabaseAccessException("hive and jdbc type mapping no support.");
        }
    }
}
