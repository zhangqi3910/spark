package org.apache.hive.storage.jdbc.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyHiveRecordWriter implements FileSinkOperator.RecordWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyHiveRecordWriter.class);

    private DatabaseAccessor dbAccessor;

    private Configuration configuration;

    public MyHiveRecordWriter(DatabaseAccessor dbAccessor, Configuration configuration) {
        this.dbAccessor = dbAccessor;
        this.configuration = configuration;
    }


    @Override
    public void write(Writable w) throws IOException {
        try {
            if (dbAccessor.insertDataJdbc(w, configuration) <= 0) {
                LOGGER.info("insert data failed.");
            }
        } catch (HiveJdbcDatabaseAccessException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close(boolean abort) throws IOException {

    }
}
