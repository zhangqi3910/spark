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
package org.apache.hive.storage.jdbc.dao;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.conf.AES128Encrypt;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.PutWritable;
import org.apache.hive.storage.jdbc.write.create.CreateTableField;
import org.apache.hive.storage.jdbc.write.create.TableField;
import org.apache.hive.storage.jdbc.write.type.JdbcFieldTypeBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 */
public class GenericJdbcDatabaseAccessor implements DatabaseAccessor {

    protected static final String DBCP_CONFIG_PREFIX = JdbcStorageConfigManager.CONFIG_PREFIX + ".dbcp";
    protected static final int DEFAULT_FETCH_SIZE = 1000;
    protected static final Logger LOGGER = LoggerFactory.getLogger(GenericJdbcDatabaseAccessor.class);
    protected DataSource dbcpDataSource = null;
    protected AES128Encrypt aes128Encrypt = new AES128Encrypt();


    public GenericJdbcDatabaseAccessor() {
    }


    @Override
    public List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            initializeDatabaseConnection(conf);
            String metadataQuery = getMetaDataQuery(conf);
            LOGGER.debug("Query to execute is [{}]", metadataQuery);

            conn = dbcpDataSource.getConnection();
            ps = conn.prepareStatement(metadataQuery);
            rs = ps.executeQuery();

            ResultSetMetaData metadata = rs.getMetaData();
            int numColumns = metadata.getColumnCount();
            List<String> columnNames = new ArrayList<String>(numColumns);
            for (int i = 0; i < numColumns; i++) {
                columnNames.add(metadata.getColumnName(i + 1));
            }
            return columnNames;
        } catch (Exception e) {
            String columnMapping = conf.get("hive.sql.column.mapping");
            if (null == columnMapping || columnMapping.isEmpty()) {
                throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
            }
            List<CreateTableField> createTableFields = new ArrayList<>();
            for (String mapping : columnMapping.split(",")) {
                if (!mapping.contains("=")) {
                    throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
                }
                String[] mappings = mapping.split("=");
                if (mappings.length != 2) {
                    throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
                }
                createTableFields.add(new CreateTableField(mappings[0].trim(), mappings[1].trim()));
            }
            String hiveColumnTypes = conf.get(serdeConstants.LIST_COLUMNS);
            if (null == hiveColumnTypes || hiveColumnTypes.isEmpty()) {
                throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e.getMessage(), e);
            }
            String[] hiveColumnTypeNameArr = hiveColumnTypes.split(",");
            TableField tableField = new TableField(createTableFields, hiveColumnTypeNameArr);
            try {
                return tableField.getJdbcColumnNames();
            } catch (Exception e1) {
                throw new HiveJdbcDatabaseAccessException("Error while trying to get column names: " + e1.getMessage(), e1);
            }
        } finally {
            cleanupResources(conn, ps, rs);
        }
    }

    protected String getMetaDataQuery(Configuration conf) {
        String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
        return addLimitToQuery(sql, 1);
    }


    @Override
    public int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            initializeDatabaseConnection(conf);
            String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
            String countQuery = "SELECT COUNT(*) FROM (" + sql + ") tmptable";
            LOGGER.debug("Query to execute is [{}]", countQuery);

            conn = dbcpDataSource.getConnection();
            ps = conn.prepareStatement(countQuery);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                LOGGER.warn("The count query did not return any results.", countQuery);
                throw new HiveJdbcDatabaseAccessException("Count query did not return any results.");
            }
        } catch (HiveJdbcDatabaseAccessException he) {
            throw he;
        } catch (Exception e) {
            LOGGER.error("Caught exception while trying to get the number of records", e);
            throw new HiveJdbcDatabaseAccessException(e);
        } finally {
            cleanupResources(conn, ps, rs);
        }
    }


    @Override
    public JdbcRecordIterator
    getRecordIterator(Configuration conf, int limit, int offset) throws HiveJdbcDatabaseAccessException {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            initializeDatabaseConnection(conf);
            String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
            String limitQuery = addLimitAndOffsetToQuery(sql, limit, offset);
            LOGGER.debug("Query to execute is [{}]", limitQuery);

            conn = dbcpDataSource.getConnection();
            ps = conn.prepareStatement(limitQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(getFetchSize(conf));
            rs = ps.executeQuery();

            return new JdbcRecordIterator(conn, ps, rs);
        } catch (Exception e) {
            LOGGER.error("Caught exception while trying to execute query", e);
            cleanupResources(conn, ps, rs);
            throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query", e);
        }
    }

    @Override
    public int insertDataJdbc(Writable w, Configuration conf) throws HiveJdbcDatabaseAccessException {
        List<JdbcFieldTypeBean> columnTypeNames = getColumnTypeNames(conf);
        PutWritable putWritable = (PutWritable) w;
        String tableName = conf.get("hive.sql.table.name");
        StringBuilder sql = new StringBuilder().append("INSERT INTO ");
        sql.append(tableName).append(" ").append("( ");
        StringBuilder columnNameMkString = new StringBuilder();
        StringBuilder valueMkString = new StringBuilder();
        for (JdbcFieldTypeBean columnTypeName : columnTypeNames) {
            columnNameMkString.append(columnTypeName.getField()).append(",");
            valueMkString.append("?").append(",");
        }
        sql.append(columnNameMkString.toString(), 0, columnNameMkString.length() - 1).append(") VALUES(");
        sql.append(valueMkString.toString(), 0, valueMkString.length() - 1).append(")");
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            initializeDatabaseConnection(conf);
            connection = dbcpDataSource.getConnection();
            pstmt = connection.prepareStatement(sql.toString());
            //对数据类型进行转换
            for (int i = 0; i < columnTypeNames.size(); i++) {
                String jdbcTypeName = columnTypeNames.get(i).getColumnTypeName();
                String hiveTypeName = columnTypeNames.get(i).getHiveColumnTypeName();
                Object value = putWritable.getFieldValueByColumnName(columnTypeNames.get(i).getField()).getValue();
                pstmt.setObject(i + 1, changeHiveToJdbc(jdbcTypeName, hiveTypeName, value));
            }
            int res = pstmt.executeUpdate();//执行sql语句
            if (res > 0) {
                LOGGER.info("数据录入成功");
            } else {
                throw new HiveJdbcDatabaseAccessException("insert table failed.");
            }
            return res;
        } catch (Exception e) {
            throw new HiveJdbcDatabaseAccessException(e);
        } finally {
            cleanupResources(connection, pstmt, null);
        }
    }

    protected Object changeHiveToJdbc(String jdbcTypeName, String hiveTypeName, Object value) throws HiveJdbcDatabaseAccessException {
        throw new HiveJdbcDatabaseAccessException("hive and jdbc type mapping no support.");
    }

    @Override
    public List<JdbcFieldTypeBean> getColumnTypeNames(Configuration conf) throws HiveJdbcDatabaseAccessException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            initializeDatabaseConnection(conf);
            String metadataQuery = getMetaDataQuery(conf);
            LOGGER.debug("Query to execute is [{}]", metadataQuery);

            conn = dbcpDataSource.getConnection();
            ps = conn.prepareStatement(metadataQuery);
            rs = ps.executeQuery();

            ResultSetMetaData metadata = rs.getMetaData();
            int numColumns = metadata.getColumnCount();
            List<JdbcFieldTypeBean> columnTypeNames = new ArrayList<JdbcFieldTypeBean>(numColumns);
            //获取hive的column属性
            String hiveColumnTypes = conf.get(serdeConstants.LIST_COLUMN_TYPES);
            if (null == hiveColumnTypes || hiveColumnTypes.isEmpty()) {
                throw new HiveJdbcDatabaseAccessException("hive no type ,error insert.");
            }
            String[] hiveColumnTypeNameArr = hiveColumnTypes.split(":");
            for (int i = 0; i < numColumns; i++) {
                columnTypeNames.add(new JdbcFieldTypeBean(metadata.getColumnName(i + 1), metadata.getColumnTypeName(i + 1), hiveColumnTypeNameArr[i]));
            }

            return columnTypeNames;
        } catch (Exception e) {
            LOGGER.error("Error while trying to get column type names.", e);
            throw new HiveJdbcDatabaseAccessException("Error while trying to get column type names: " + e.getMessage(), e);
        } finally {
            cleanupResources(conn, ps, rs);
        }
    }

    @Override
    public boolean isTableExist(Configuration conf) throws Exception {
        String tableName = conf.get("hive.sql.table.name");
        Connection conn = null;
        ResultSet resultSet = null;
        try {
            initializeDatabaseConnection(conf);
            conn = dbcpDataSource.getConnection();
            String jdbcTableName = tableName;
            if (tableName.contains(".")) {
                jdbcTableName = tableName.split("\\.")[1];
            }
            resultSet = conn.getMetaData().getTables(null, null, jdbcTableName, null);
            return resultSet.next();
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            cleanupResources(conn, null, resultSet);
        }
    }

    private String getHiveTypeName(List<FieldSchema> fieldSchemas, String hiveColumnName) {
        for (FieldSchema fieldSchema : fieldSchemas) {
            if (fieldSchema.getName().equalsIgnoreCase(hiveColumnName)) {
                return fieldSchema.getType();
            }
        }
        return null;
    }


    @Override
    public int createTable(Configuration conf, Table table) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        String tableName = conf.get("hive.sql.table.name");
        try {
            initializeDatabaseConnection(conf);
            conn = dbcpDataSource.getConnection();
            stmt = conn.createStatement();
            StringBuilder stringBuilder = new StringBuilder().append("CREATE TABLE ");
            stringBuilder.append(tableName).append(" (");

            String columnMapping = conf.get("hive.sql.column.mapping");
            if (null == columnMapping || columnMapping.isEmpty()) {
                throw new HiveJdbcDatabaseAccessException("Error while trying to get column names");
            }
            List<CreateTableField> createTableFields = new ArrayList<>();

            List<FieldSchema> fieldSchemas = table.getSd().getCols();
            for (String mapping : columnMapping.split(",")) {
                if (!mapping.contains("=")) {
                    throw new HiveJdbcDatabaseAccessException("Error while trying to get column names ");
                }
                String[] mappings = mapping.split("=");
                if (mappings.length != 2) {
                    throw new HiveJdbcDatabaseAccessException("Error while trying to get column names");
                }
                String hiveTypeName = getHiveTypeName(fieldSchemas, mappings[0].trim());
                if (null == hiveTypeName) {
                    throw new HiveJdbcDatabaseAccessException("Error while trying to get column names");
                }
                createTableFields.add(new CreateTableField(mappings[0].trim(), mappings[1].trim(), hiveTypeName));
            }

            StringBuilder fieldNameStringBuilder = new StringBuilder();
            for (CreateTableField createTableField : createTableFields) {
                fieldNameStringBuilder.append(createTableField.getJdbcField()).append("  ");
                fieldNameStringBuilder.append(getJdbcTypeName(createTableField.getHiveTypeName())).append(",");
            }
            stringBuilder.append(fieldNameStringBuilder.toString(), 0, fieldNameStringBuilder.length() - 1);

            stringBuilder.append(" )");
            return stmt.executeUpdate(stringBuilder.toString());
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            cleanupResources(conn, stmt, null);
        }
    }

    @Override
    public int dropTable(Configuration conf) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        String tableName = conf.get("hive.sql.table.name");
        try {
            initializeDatabaseConnection(conf);
            conn = dbcpDataSource.getConnection();
            stmt = conn.createStatement();
            return stmt.executeUpdate(getJdbcDropTableSql(tableName));
        } catch (Exception e) {
            throw new Exception(e);
        }
    }


    protected String getJdbcDropTableSql(String tableName) {
        return "DROP TABLE " + tableName;
    }


    protected String getJdbcTypeName(String hiveTypeName) {
        return "VARCHAR2(500)";
    }


    /**
     * Uses generic JDBC escape functions to add a limit and offset clause to a query string
     *
     * @param sql
     * @param limit
     * @param offset
     * @return
     */
    protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
        if (offset == 0) {
            return addLimitToQuery(sql, limit);
        } else {
            return sql + " {LIMIT " + limit + " OFFSET " + offset + "}";
        }
    }


    /*
     * Uses generic JDBC escape functions to add a limit clause to a query string
     */
    protected String addLimitToQuery(String sql, int limit) {
        return sql + " {LIMIT " + limit + "}";
    }


    protected void cleanupResources(Connection conn, Statement ps, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            LOGGER.warn("Caught exception during resultset cleanup.", e);
        }

        try {
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            LOGGER.warn("Caught exception during statement cleanup.", e);
        }

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            LOGGER.warn("Caught exception during connection cleanup.", e);
        }
    }

    protected void initializeDatabaseConnection(Configuration conf) throws Exception {
        if (dbcpDataSource == null) {
            synchronized (this) {
                if (dbcpDataSource == null) {
                    Properties props = getConnectionPoolProperties(conf);
                    dbcpDataSource = BasicDataSourceFactory.createDataSource(props);
                }
            }
        }
    }


    protected Properties getConnectionPoolProperties(Configuration conf) throws HiveJdbcDatabaseAccessException {
        // Create the default properties object
        Properties dbProperties = getDefaultDBCPProperties();

        // override with user defined properties
        Map<String, String> userProperties = conf.getValByRegex(DBCP_CONFIG_PREFIX + "\\.*");
        //password Decrypt
        if (userProperties != null) {
            String password = userProperties.get(DBCP_CONFIG_PREFIX + ".password");
            if (null != password && !password.isEmpty()) {
                try {
                    userProperties.put(DBCP_CONFIG_PREFIX + ".password", aes128Encrypt.decrypt(password));
                } catch (Exception e) {
                    throw new HiveJdbcDatabaseAccessException(e);
                }
            }
        }
        if ((userProperties != null) && (!userProperties.isEmpty())) {
            for (Entry<String, String> entry : userProperties.entrySet()) {
                dbProperties.put(entry.getKey().replaceFirst(DBCP_CONFIG_PREFIX + "\\.", ""), entry.getValue());
            }
        }

        // essential properties that shouldn't be overridden by users
        dbProperties.put("url", conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()));
        dbProperties.put("driverClassName", conf.get(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName()));
        dbProperties.put("type", "javax.sql.DataSource");
        return dbProperties;
    }


    protected Properties getDefaultDBCPProperties() {
        Properties props = new Properties();
        props.put("initialSize", "1");
        props.put("maxActive", "3");
        props.put("maxIdle", "0");
        props.put("maxWait", "10000");
        props.put("timeBetweenEvictionRunsMillis", "30000");
        return props;
    }


    protected int getFetchSize(Configuration conf) {
        return conf.getInt(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
    }
}
