package org.apache.hive.storage.jdbc.write.create.oracle;

import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.create.IHiveCreateChangeJdbcType;

import java.util.HashMap;
import java.util.Map;

public final class OracleHiveCreateChangeJdbcType {
    private static Map<HiveCreateType, IHiveCreateChangeJdbcType> changeJdbcTypeMap = new HashMap<>();

    static {
        changeJdbcTypeMap.put(HiveCreateType.INT, new OracleHiveCreateIntJdbc());
        changeJdbcTypeMap.put(HiveCreateType.STRING, new OracleHiveCreateStringJdbc());
        changeJdbcTypeMap.put(HiveCreateType.TINYINT, new OracleHiveCreateTINYINTJdbc());
        changeJdbcTypeMap.put(HiveCreateType.SMALLINT, new OracleHiveCreateSMALLINTJdbc());
        changeJdbcTypeMap.put(HiveCreateType.INTEGER, new OracleHiveCreateINTEGERJdbc());
        changeJdbcTypeMap.put(HiveCreateType.FLOAT, new OracleHiveCreateFLOATJdbc());
        changeJdbcTypeMap.put(HiveCreateType.DOUBLE, new OracleHiveCreateDOUBLEJdbc());
        changeJdbcTypeMap.put(HiveCreateType.DECIMAL, new OracleHiveCreateDECIMALJdbc());
        changeJdbcTypeMap.put(HiveCreateType.TIMESTAMP, new OracleHiveCreateTIMESTAMPJdbc());
        changeJdbcTypeMap.put(HiveCreateType.VARCHAR, new OracleHiveCreateVARCHARJdbc());
        changeJdbcTypeMap.put(HiveCreateType.CHAR, new OracleHiveCreateCHARJdbc());
        changeJdbcTypeMap.put(HiveCreateType.DATE, new OracleHiveCreateDATEJdbc());
        changeJdbcTypeMap.put(HiveCreateType.BINARY, new OracleHiveCreateBINARYJdbc());
        changeJdbcTypeMap.put(HiveCreateType.BOOLEAN, new OracleHiveCreateBooleanJdbc());
        changeJdbcTypeMap.put(HiveCreateType.BIGINT, new OracleHiveCreateBIGINTJdbc());
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
