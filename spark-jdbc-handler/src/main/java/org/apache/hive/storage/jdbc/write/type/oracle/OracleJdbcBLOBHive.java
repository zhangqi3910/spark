package org.apache.hive.storage.jdbc.write.type.oracle;

import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

public class OracleJdbcBLOBHive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) throws HiveJdbcDatabaseAccessException {
        if (hiveType.equals(HiveCreateType.BINARY))
        {
            return value;
        }
        return null;
    }
}
