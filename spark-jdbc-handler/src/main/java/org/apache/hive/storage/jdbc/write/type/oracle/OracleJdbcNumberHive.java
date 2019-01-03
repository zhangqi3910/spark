package org.apache.hive.storage.jdbc.write.type.oracle;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hive.storage.jdbc.write.create.HiveCreateType;
import org.apache.hive.storage.jdbc.write.type.IHiveTypeChange;

public class OracleJdbcNumberHive implements IHiveTypeChange {
    @Override
    public Object exChangeHiveType(Object value, HiveCreateType hiveType) {
        if (hiveType.equals(HiveCreateType.DECIMAL)) {
            return ((HiveDecimal) value).bigDecimalValue();
        }
        return value;
    }
}
