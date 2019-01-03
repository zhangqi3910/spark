package org.apache.hive.storage.jdbc.write.create.oracle;

import org.apache.hive.storage.jdbc.write.create.IHiveCreateChangeJdbcType;

public class OracleHiveCreateStringJdbc implements IHiveCreateChangeJdbcType {
    @Override
    public String hiveTypeChangeJdbcType(String hiveType) {
        return "VARCHAR(255)";
    }
}
