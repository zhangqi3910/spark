package org.apache.hive.storage.jdbc.write.create.mysql;

import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.create.IHiveCreateChangeJdbcType;

import java.util.HashMap;
import java.util.Map;

public final class MysqlHiveCreateChangeJdbcType {
    private static Map<HiveCreateType, IHiveCreateChangeJdbcType> changeJdbcTypeMap = new HashMap<>();

    static {
        changeJdbcTypeMap.put(HiveCreateType.INT, new MysqlHiveCreateIntJdbc());
        changeJdbcTypeMap.put(HiveCreateType.STRING, new MysqlHiveCreateStringJdbc());
        changeJdbcTypeMap.put(HiveCreateType.TINYINT, new MysqlHiveCreateTINYINTJdbc());
        changeJdbcTypeMap.put(HiveCreateType.SMALLINT, new MysqlHiveCreateSMALLINTJdbc());
        changeJdbcTypeMap.put(HiveCreateType.INTEGER, new MysqlHiveCreateINTEGERJdbc());
        changeJdbcTypeMap.put(HiveCreateType.BIGINT, new MysqlHiveCreateBIGINTJdbc());
        changeJdbcTypeMap.put(HiveCreateType.FLOAT, new MysqlHiveCreateFLOATJdbc());
        changeJdbcTypeMap.put(HiveCreateType.DOUBLE, new MysqlHiveCreateDOUBLEJdbc());
        changeJdbcTypeMap.put(HiveCreateType.DECIMAL, new MysqlHiveCreateDECIMALJdbc());
        changeJdbcTypeMap.put(HiveCreateType.TIMESTAMP, new MysqlHiveCreateTIMESTAMPJdbc());
        changeJdbcTypeMap.put(HiveCreateType.VARCHAR, new MysqlHiveCreateVARCHARJdbc());
        changeJdbcTypeMap.put(HiveCreateType.CHAR, new MysqlHiveCreateCHARJdbc());
        changeJdbcTypeMap.put(HiveCreateType.DATE, new MysqlHiveCreateDATEJdbc());
        changeJdbcTypeMap.put(HiveCreateType.BINARY, new MysqlHiveCreateBINARYJdbc());
        changeJdbcTypeMap.put(HiveCreateType.BOOLEAN, new MysqlHiveCreateBooleanJdbc());
    }


    public static String getJdbcTypeNameByHiveType(String hiveTypeName) {
        HiveCreateType hiveCreateType = HiveCreateType.getHiveCreateTypeByHiveType(hiveTypeName);
        IHiveCreateChangeJdbcType hiveCreateChangeJdbcType = changeJdbcTypeMap.get(hiveCreateType);
        if (null == hiveCreateType || null == hiveCreateChangeJdbcType) {
            throw new UnsupportedOperationException("unsupport data type.");
        }
        return hiveCreateChangeJdbcType.hiveTypeChangeJdbcType(hiveTypeName);
    }

}
