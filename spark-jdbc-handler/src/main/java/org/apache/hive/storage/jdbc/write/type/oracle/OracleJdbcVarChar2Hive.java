package org.apache.hive.storage.jdbc.write.type.oracle;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

public class OracleJdbcVarChar2Hive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) throws HiveJdbcDatabaseAccessException {
        if (hiveType.equals(HiveCreateType.VARCHAR) ||
                hiveType.equals(HiveCreateType.CHAR) ||
                hiveType.equals(HiveCreateType.STRING)) {
            return String.valueOf(value);
        } else {
            throw new HiveJdbcDatabaseAccessException("hive and jdbc type mapping no support.");
        }
    }
}
