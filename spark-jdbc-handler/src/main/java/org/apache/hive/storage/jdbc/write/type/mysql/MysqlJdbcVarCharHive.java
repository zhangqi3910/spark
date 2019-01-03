package org.apache.hive.storage.jdbc.write.type.mysql;

import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

public class MysqlJdbcVarCharHive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) {
        if (value instanceof String) {
            return String.valueOf(value);
        }
        return String.valueOf(value);
    }
}
