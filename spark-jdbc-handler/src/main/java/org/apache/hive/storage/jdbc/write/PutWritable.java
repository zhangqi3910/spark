package org.apache.hive.storage.jdbc.write;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutWritable implements Writable {

    private List<JdbcWriteBean> jdbcWriteBeans;

    public PutWritable() {
    }

    public PutWritable(List<JdbcWriteBean> jdbcWriteBeans) {
        this.jdbcWriteBeans = jdbcWriteBeans;
    }

    public List<JdbcWriteBean> getJdbcWriteBeans() {
        return jdbcWriteBeans;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    }


    public JdbcWriteBean getFieldValueByColumnName(String columnName) {
        for (JdbcWriteBean jdbcWriteBean : jdbcWriteBeans) {
            if (jdbcWriteBean.getField().equalsIgnoreCase(columnName))
            {
                return jdbcWriteBean;
            }
        }
        return null;
    }


}
