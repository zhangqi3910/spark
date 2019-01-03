package org.apache.hive.storage.jdbc.write.type.oracle;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

import java.math.BigDecimal;

public class OracleJdbcIntHive implements IHiveTypeChange {

    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) throws HiveJdbcDatabaseAccessException {
        if (hiveType.equals(HiveCreateType.INT) || hiveType.equals(HiveCreateType.INTEGER)) {
            return value;
        } else if (hiveType.equals(HiveCreateType.DECIMAL)) {
            return ((BigDecimal) value).intValue();
        } else {
            throw new HiveJdbcDatabaseAccessException("hive and jdbc type mapping no support.");
        }
    }
}
