package org.apache.hive.storage.jdbc.write.create.mysql;

import org.apache.hive.storage.jdbc.write.create.IHiveCreateChangeJdbcType;

public class MysqlHiveCreateTINYINTJdbc implements IHiveCreateChangeJdbcType {
    @Override
    public String hiveTypeChangeJdbcType(String hiveType) {
        return "tinyint";
    }
}
