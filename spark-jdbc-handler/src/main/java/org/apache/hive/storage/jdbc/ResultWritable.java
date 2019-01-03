package org.apache.hive.storage.jdbc;

import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.read.ReadJdbcBean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class ResultWritable implements Writable {

    private List<ReadJdbcBean> readJdbcBeans;

    public ResultWritable() {
    }

    public ResultWritable(List<ReadJdbcBean> readJdbcBeans) {
        this.readJdbcBeans = readJdbcBeans;
    }

    public List<ReadJdbcBean> getReadJdbcBeans() {
        return readJdbcBeans;
    }

    public Object getJdbcRowValue(String columnName) {
        for (ReadJdbcBean readJdbcBean : readJdbcBeans) {
            if (readJdbcBean.getField().equalsIgnoreCase(columnName)) {
                return readJdbcBean.getValue();
            }
        }
        return null;
    }


    public String getJdbcTypeName(String columnName) {
        for (ReadJdbcBean readJdbcBean : readJdbcBeans) {
            if (readJdbcBean.getField().equalsIgnoreCase(columnName)) {
                return readJdbcBean.getFieldTypeName();
            }
        }
        return null;
    }

    public void setReadJdbcBeans(List<ReadJdbcBean> readJdbcBeans) {
        this.readJdbcBeans = readJdbcBeans;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
