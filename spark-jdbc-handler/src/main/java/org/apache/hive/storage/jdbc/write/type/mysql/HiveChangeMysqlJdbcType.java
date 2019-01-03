package org.apache.hive.storage.jdbc.write.type.mysql;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

import java.util.HashMap;
import java.util.Map;

public final class HiveChangeMysqlJdbcType {

    private static Map<MysqlJdbcType, IHiveTypeChange> hiveTypeChangeMap = new HashMap<>();

    static {
        hiveTypeChangeMap.put(MysqlJdbcType.INT, new MysqlJdbcIntHive());
        hiveTypeChangeMap.put(MysqlJdbcType.VARCHAR, new MysqlJdbcVarCharHive());
        hiveTypeChangeMap.put(MysqlJdbcType.DECIMAL, new MysqlJdbcDECIMALHive());
        hiveTypeChangeMap.put(MysqlJdbcType.TEXT, new MysqlJdbcTEXTHive());
        hiveTypeChangeMap.put(MysqlJdbcType.TINYINT, new MysqlJdbcTINYINTHive());
        hiveTypeChangeMap.put(MysqlJdbcType.SMALLINT, new MysqlJdbcSMALLINTHive());
        hiveTypeChangeMap.put(MysqlJdbcType.BIGINT, new MysqlJdbcBIGINTHive());
        hiveTypeChangeMap.put(MysqlJdbcType.FLOAT, new MysqlJdbcFLOATHive());
        hiveTypeChangeMap.put(MysqlJdbcType.DOUBLE, new MysqlJdbcDOUBLEHive());
        hiveTypeChangeMap.put(MysqlJdbcType.BIT, new MysqlJdbcBITHive());
        hiveTypeChangeMap.put(MysqlJdbcType.BINARY, new MysqlJdbcBINARYHive());
        hiveTypeChangeMap.put(MysqlJdbcType.VARBINARY, new MysqlJdbcVARBINARYHive());
        hiveTypeChangeMap.put(MysqlJdbcType.TIMESTAMP, new MysqllJdbcTIMESTAMPHive());
        hiveTypeChangeMap.put(MysqlJdbcType.DATETIME, new MysqlJdbcDATETIMEHive());
        hiveTypeChangeMap.put(MysqlJdbcType.DATE, new MysqlJdbcDATEHive());
        hiveTypeChangeMap.put(MysqlJdbcType.CHAR, new MysqlJdbcCHARHive());
    }

    public static Object changeHiveTypeToJdbc(String jdbcColumnTypeName, String hiveTypeName, Object value) throws HiveJdbcDatabaseAccessException {
        MysqlJdbcType mysqlJdbcType = MysqlJdbcType.getJdbcTypeByTypeName(jdbcColumnTypeName);
        IHiveTypeChange hiveTypeChange = hiveTypeChangeMap.get(mysqlJdbcType);
        HiveCreateType hiveCreateType = HiveCreateType.getHiveCreateTypeByHiveType(hiveTypeName);
        if (null == mysqlJdbcType || null == hiveTypeChange || null == hiveCreateType) {
            throw new UnsupportedOperationException("unsupport  insert  data type. mysql type name = " + jdbcColumnTypeName);
        }
        return hiveTypeChange.exChangeHiveType(value, hiveCreateType);
    }

}
