package org.apache.hive.storage.jdbc.read;

public class ReadJdbcBean {

    private String field;

    private Object value;

    private String fieldTypeName;

    public ReadJdbcBean(String field, Object value, String fieldTypeName) {
        this.field = field;
        this.value = value;
        this.fieldTypeName = fieldTypeName;
    }

    public ReadJdbcBean() {
    }

    public String getFieldTypeName() {
        return fieldTypeName;
    }

    public void setFieldTypeName(String fieldTypeName) {
        this.fieldTypeName = fieldTypeName;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
