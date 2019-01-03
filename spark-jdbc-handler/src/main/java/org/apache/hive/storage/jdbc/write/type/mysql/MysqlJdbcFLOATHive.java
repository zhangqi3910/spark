package org.apache.hive.storage.jdbc.write.type.mysql;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

public class MysqlJdbcFLOATHive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) throws HiveJdbcDatabaseAccessException {
        if (hiveType.equals(HiveCreateType.FLOAT)) {
            return value;
        } else if (hiveType.equals(HiveCreateType.DOUBLE) ||
                hiveType.equals(HiveCreateType.INTEGER) ||
                hiveType.equals(HiveCreateType.DECIMAL) ||
                hiveType.equals(HiveCreateType.INT)) {
            return Float.parseFloat(String.valueOf(value));
        } else {
            throw new HiveJdbcDatabaseAccessException("hive and jdbc type mapping no support.");
        }
    }
}
