package org.apache.hive.storage.jdbc.write.create.mysql;

import org.apache.hive.storage.jdbc.write.create.IHiveCreateChangeJdbcType;

public class MysqlHiveCreateDECIMALJdbc implements IHiveCreateChangeJdbcType {
    @Override
    public String hiveTypeChangeJdbcType(String hiveType) {
        return hiveType;
    }
}
