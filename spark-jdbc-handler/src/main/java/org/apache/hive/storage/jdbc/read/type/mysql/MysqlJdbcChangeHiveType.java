package org.apache.hive.storage.jdbc.read.type.mysql;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.read.type.HiveType;
import org.apache.hive.storage.jdbc.read.type.IJdbcTypeChange;

import java.util.HashMap;
import java.util.Map;

public final class MysqlJdbcChangeHiveType {
    private static Map<HiveType, IJdbcTypeChange> jdbcTypeChangeMap = new HashMap<>();

    static {
        jdbcTypeChangeMap.put(HiveType.INT, new HiveIntMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.STRING, new HiveStringMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.CHAR, new HiveCharMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.DATE, new HiveDateMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.DECIMAL, new HiveDecimalMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.TIMESTAMP, new HiveTimestampMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.LONG, new HiveLongMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.FLOAT, new HiveFloatMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.VARCHAR, new HiveVarCharMysqlJdbc());
        jdbcTypeChangeMap.put(HiveType.DOUBLE, new HiveDoubleMysqlJdbc());
    }

    public static Object changeJdbcTypeToHive(String hiveColumnType, String jdbcTypeName, Object value) throws HiveJdbcDatabaseAccessException {
        HiveType hiveType = HiveType.getHiveTypeByType(hiveColumnType);
        IJdbcTypeChange jdbcTypeChange = jdbcTypeChangeMap.get(hiveType);
        if (null == hiveColumnType || null == jdbcTypeChange) {
            throw new UnsupportedOperationException("unsupport data type. hive type name = " + hiveColumnType);
        }
        return jdbcTypeChangeMap.get(hiveType).exChangeJdbcType(value, jdbcTypeName);
    }
}
