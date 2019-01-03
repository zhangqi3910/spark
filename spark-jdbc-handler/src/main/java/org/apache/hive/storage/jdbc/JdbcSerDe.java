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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.read.type.mysql.MysqlJdbcChangeHiveType;
import org.apache.hive.storage.jdbc.write.JdbcWriteBean;
import org.apache.hive.storage.jdbc.write.PutWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class JdbcSerDe extends AbstractSerDe {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSerDe.class);

    private StructObjectInspector objectInspector;
    private int numColumns;
    private String[] hiveColumnTypeArray;
    private List<String> columnNames;
    private List<Object> row;


    /*
     * This method gets called multiple times by Hive. On some invocations, the properties will be empty.
     * We need to detect when the properties are not empty to initialise the class variables.
     *
     * @see org.apache.hadoop.hive.serde2.Deserializer#initialize(org.apache.hadoop.conf.Configuration, java.util.Properties)
     */
    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        try {
            LOGGER.debug("Initializing the SerDe");

            // Hive cdh-4.3 does not provide the properties object on all calls
            if (tbl.containsKey(JdbcStorageConfig.DATABASE_TYPE.getPropertyName())) {
                Configuration tableConfig = JdbcStorageConfigManager.convertPropertiesToConfiguration(tbl);

                DatabaseAccessor dbAccessor = DatabaseAccessorFactory.getAccessor(tableConfig);
                columnNames = dbAccessor.getColumnNames(tableConfig);
                numColumns = columnNames.size();

                String[] hiveColumnNameArray = parseProperty(tbl.getProperty(serdeConstants.LIST_COLUMNS), ",");
                if (numColumns != hiveColumnNameArray.length) {
                    throw new SerDeException("Expected " + numColumns + " columns. Table definition has "
                            + hiveColumnNameArray.length + " columns");
                }
                List<String> hiveColumnNames = Arrays.asList(hiveColumnNameArray);

                hiveColumnTypeArray = parseProperty(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES), ":");
                if (hiveColumnTypeArray.length == 0) {
                    throw new SerDeException("Received an empty Hive column type definition");
                }

                List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>(numColumns);
                LazySerDeParameters serdeParams = new LazySerDeParameters(conf, tbl, getClass().getName());
                List<TypeInfo> columnTypes = serdeParams.getColumnTypes();
                for (int i = 0; i < numColumns; i++) {
                    fieldInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector((PrimitiveTypeInfo) columnTypes.get(i)));
                }
                objectInspector =
                        ObjectInspectorFactory.getStandardStructObjectInspector(hiveColumnNames,
                                fieldInspectors);
                row = new ArrayList<Object>(numColumns);
            }
        } catch (Exception e) {
            LOGGER.error("Caught exception while initializing the SqlSerDe", e);
            throw new SerDeException(e);
        }
    }


    private String[] parseProperty(String propertyValue, String delimiter) {
        if ((propertyValue == null) || (propertyValue.trim().isEmpty())) {
            return new String[]{};
        }

        return propertyValue.split(delimiter);
    }


    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        LOGGER.debug("Deserializing from SerDe");
        if (!(blob instanceof ResultWritable)) {
            throw new SerDeException("Expected MapWritable. Got " + blob.getClass().getName());
        }

        if ((row == null) || (columnNames == null)) {
            throw new SerDeException("JDBC SerDe hasn't been initialized properly");
        }

        row.clear();
        ResultWritable input = (ResultWritable) blob;


        for (int i = 0; i < columnNames.size(); i++) {
            Object value = input.getJdbcRowValue(columnNames.get(i));
            String columnType = hiveColumnTypeArray[i];
            //根据hive的数据类型对jdbc的数据类型进行转换成hive的数据类型的值。
            try {
                value = MysqlJdbcChangeHiveType.changeJdbcTypeToHive(columnType, input.getJdbcTypeName(columnNames.get(i)), value);
            } catch (HiveJdbcDatabaseAccessException e) {
                throw new SerDeException(e);
            }
            if (value == null) {
                row.add(null);
            } else {
                row.add(value);
            }
        }
        return row;
    }


    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }


    @Override
    public Class<? extends Writable> getSerializedClass() {
        //        return MapWritable.class;
        return PutWritable.class;
    }


    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException(getClass().toString()
                    + " can only serialize struct types, but we got: "
                    + objInspector.getTypeName());
        }
        StructObjectInspector soi = (StructObjectInspector) objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> values = soi.getStructFieldsDataAsList(obj);
        List<JdbcWriteBean> jdbcWriteBeans = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            jdbcWriteBeans.add(new JdbcWriteBean(fields.get(i).getFieldName(), values.get(i)));
        }
        return new PutWritable(jdbcWriteBeans);
    }


    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

}
