package org.apache.hive.storage.jdbc.write.create;

import org.apache.hive.storage.jdbc.exception.HiveJdbcStorageException;

import java.util.ArrayList;
import java.util.List;

public class TableField {

    private List<CreateTableField> createTableFields;

    private String[] hiveColumnNames;

    public TableField() {
    }

    public TableField(List<CreateTableField> createTableFields, String[] hiveColumnNames) {
        this.createTableFields = createTableFields;
        this.hiveColumnNames = hiveColumnNames;
    }

    public List<CreateTableField> getCreateTableFields() {
        return createTableFields;
    }

    public void setCreateTableFields(List<CreateTableField> createTableFields) {
        this.createTableFields = createTableFields;
    }

    public String[] getHiveColumnNames() {
        return hiveColumnNames;
    }

    public void setHiveColumnNames(String[] hiveColumnNames) {
        this.hiveColumnNames = hiveColumnNames;
    }

    public CreateTableField getJdbcColumnName(String hiveColumnName) {
        for (CreateTableField createTableField : createTableFields) {
            if (createTableField.getHiveField().equalsIgnoreCase(hiveColumnName)) {
                return createTableField;
            }
        }
        return null;
    }


    public List<String> getJdbcColumnNames() throws Exception {
        List<String> jdbcColumnNames = new ArrayList<>();
        for (String hiveColumnName : hiveColumnNames) {
            CreateTableField createTableField = getJdbcColumnName(hiveColumnName.trim());
            if (null == createTableField) {
                throw new HiveJdbcStorageException("hive mapping jdbc field error.");
            }
            jdbcColumnNames.add(createTableField.getJdbcField());
        }
        return jdbcColumnNames;
    }

}
