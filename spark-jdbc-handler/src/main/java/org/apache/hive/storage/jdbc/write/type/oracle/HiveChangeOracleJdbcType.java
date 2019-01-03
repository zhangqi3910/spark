package org.apache.hive.storage.jdbc.write.type.oracle;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

import java.util.HashMap;
import java.util.Map;

public final class HiveChangeOracleJdbcType {

    private static Map<OracleJdbcType, IHiveTypeChange> hiveTypeChangeMap = new HashMap<>();

    static {
        hiveTypeChangeMap.put(OracleJdbcType.INT, new OracleJdbcIntHive());
        hiveTypeChangeMap.put(OracleJdbcType.NUMBER, new OracleJdbcNumberHive());
        hiveTypeChangeMap.put(OracleJdbcType.VARCHAR2, new OracleJdbcVarChar2Hive());
        hiveTypeChangeMap.put(OracleJdbcType.CHAR, new OracleJdbcCHARHive());
        hiveTypeChangeMap.put(OracleJdbcType.VARCHAR, new OracleJdbcVARCHARHive());
        hiveTypeChangeMap.put(OracleJdbcType.INTEGER, new OracleJdbcINTEGERHive());
        hiveTypeChangeMap.put(OracleJdbcType.LONG, new OracleJdbcLONGHive());
        hiveTypeChangeMap.put(OracleJdbcType.FLOAT, new OracleJdbcFLOATType());
        hiveTypeChangeMap.put(OracleJdbcType.DATE, new OracleJdbcDATEHive());
        hiveTypeChangeMap.put(OracleJdbcType.TIMESTAMP, new OracleJdbcTIMESTAMPHive());
        hiveTypeChangeMap.put(OracleJdbcType.RAW, new OracleJdbcRAWHive());
        hiveTypeChangeMap.put(OracleJdbcType.BLOB, new OracleJdbcBLOBHive());
        hiveTypeChangeMap.put(OracleJdbcType.CLOB, new OracleJdbcCLOBHive());
        hiveTypeChangeMap.put(OracleJdbcType.DECIMAL, new OracleJdbcDECIMALHive());
    }

    public static Object changeHiveTypeToJdbc(String jdbcColumnTypeName, String hiveTypeName, Object value) throws HiveJdbcDatabaseAccessException {
        OracleJdbcType oracleJdbcType = OracleJdbcType.getJdbcTypeByTypeName(jdbcColumnTypeName);
        IHiveTypeChange hiveTypeChange = hiveTypeChangeMap.get(oracleJdbcType);
        HiveCreateType hiveCreateType = HiveCreateType.getHiveCreateTypeByHiveType(hiveTypeName);
        if (null == oracleJdbcType || null == hiveTypeChange || null == hiveCreateType) {
            throw new UnsupportedOperationException("unsupport  insert  data type." + jdbcColumnTypeName);
        }
        return hiveTypeChange.exChangeHiveType(value, hiveCreateType);
    }
}
