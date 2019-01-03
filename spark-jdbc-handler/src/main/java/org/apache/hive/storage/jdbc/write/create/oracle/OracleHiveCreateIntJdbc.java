package org.apache.hive.storage.jdbc.write.create.oracle;

import org.apache.hive.storage.jdbc.write.create.IHiveCreateChangeJdbcType;

public class OracleHiveCreateIntJdbc implements IHiveCreateChangeJdbcType {
    @Override
    public String hiveTypeChangeJdbcType(String hiveType) {
        return "NUMBER(10,0)";
    }
}
