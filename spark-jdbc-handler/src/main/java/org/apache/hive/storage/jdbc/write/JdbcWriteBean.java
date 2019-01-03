package org.apache.hive.storage.jdbc.write;

public class JdbcWriteBean {

    private String field;

    private Object value;


    public JdbcWriteBean() {
    }

    public JdbcWriteBean(String field, Object value) {
        this.field = field;
        this.value = value;
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
