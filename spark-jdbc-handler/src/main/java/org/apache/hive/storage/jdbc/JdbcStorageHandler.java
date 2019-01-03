/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdbcStorageHandler extends DefaultStorageHandler implements HiveStorageHandler, HiveMetaHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStorageHandler.class);

    private Configuration conf;


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }


    @Override
    public Configuration getConf() {
        return this.conf;
    }


    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return JdbcInputFormat.class;
    }


    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return JdbcOutputFormat.class;
    }


    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return JdbcSerDe.class;
    }


    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }


    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        JdbcStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
    }


    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        JdbcStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
    }


    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        // Nothing to do here...
    }


    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

    }


    @Override
    public void preCreateTable(Table table) throws MetaException {
        boolean isExternal = MetaStoreUtils.isExternalTable(table);
        table.getSd().setLocation(null);
        // We'd like to move this to HiveMetaStore for any non-native table, but
        // first we need to support storing NULL for location on a table
        if (table.getSd().getLocation() != null && table.getSd().getInputFormat() != null && table.getSd().getOutputFormat() != null) {
            throw new MetaException("LOCATION may not be specified for jdbc.");
        }

        String mappings = table.getParameters().get("hive.sql.column.mapping");
        if (null == mappings || mappings.isEmpty()) {
            throw new MetaException("LOCATION may not be specified for jdbc.");
        }


        Properties properties = new Properties();
        table.getParameters().forEach(properties::setProperty);
        Configuration tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(properties);
        String tableName = conf.get("hive.sql.table.name");
        DatabaseAccessor dbAccessor = DatabaseAccessorFactory.getAccessor(tableConfig);

        try {
            if (!dbAccessor.isTableExist(tableConfig)) {
                if (!isExternal) {
                    //create table
                    dbAccessor.createTable(tableConfig, table);
                } else {
                    // an external table
                    throw new MetaException("jdbc table " + tableName +
                            " doesn't exist while the table is declared as an external table.");
                }
            } else {
                if (!isExternal) {
                    throw new MetaException("Table " + tableName + " already exists"
                            + " within jdbc; use CREATE EXTERNAL TABLE instead to"
                            + " register it in Hive.");
                }
                List<String> jdbcColumnNames = dbAccessor.getColumnNames(tableConfig);
                if (mappings.split(",").length != jdbcColumnNames.size()) {
                    throw new MetaException("Table " + tableName + " already exists"
                            + " within jdbc; use CREATE EXTERNAL TABLE instead to"
                            + " register it in Hive.");
                }
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new MetaException("create table failed for jdbc.");
        }
    }


    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        boolean isExternal = MetaStoreUtils.isExternalTable(table);
        Properties properties = new Properties();
        table.getParameters().forEach(properties::setProperty);
        Configuration tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(properties);
        DatabaseAccessor dbAccessor = DatabaseAccessorFactory.getAccessor(tableConfig);

        try {
            if (!isExternal && dbAccessor.isTableExist(tableConfig)) {
                dbAccessor.dropTable(tableConfig);
            }
        } catch (Exception e) {
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {

    }

    @Override
    public void preDropTable(Table table) throws MetaException {

    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {

    }

    @Override
    public void commitDropTable(Table table, boolean deleteData) throws MetaException {
        Properties properties = new Properties();
        table.getParameters().forEach(properties::setProperty);
        Configuration tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(properties);
        DatabaseAccessor dbAccessor = DatabaseAccessorFactory.getAccessor(tableConfig);

        try {
            boolean isExternal = MetaStoreUtils.isExternalTable(table);
            if (deleteData && !isExternal) {
                if (dbAccessor.isTableExist(tableConfig)) {
                    dbAccessor.dropTable(tableConfig);
                }
            }
        } catch (Exception e) {
            throw new MetaException(StringUtils.stringifyException(e));
        }
    }
}
