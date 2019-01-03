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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Writable;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.hive.storage.jdbc.write.type.JdbcFieldTypeBean;

import java.util.List;

public interface DatabaseAccessor {

    List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException;


    int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException;


    JdbcRecordIterator
    getRecordIterator(Configuration conf, int limit, int offset) throws HiveJdbcDatabaseAccessException;


    int insertDataJdbc(Writable w, Configuration conf) throws HiveJdbcDatabaseAccessException;

    List<JdbcFieldTypeBean> getColumnTypeNames(Configuration conf) throws HiveJdbcDatabaseAccessException;

    boolean isTableExist(Configuration conf) throws Exception;

    int createTable(Configuration conf, Table table) throws Exception;

    int dropTable(Configuration conf) throws Exception;

}
