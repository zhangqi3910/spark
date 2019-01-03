package org.apache.hive.storage.jdbc.write.type.mysql;

import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

public class MysqlJdbcTEXTHive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) {
        return String.valueOf(value);
    }
}
