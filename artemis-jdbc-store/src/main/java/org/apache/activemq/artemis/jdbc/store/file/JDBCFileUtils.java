/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.jdbc.store.file;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

import static org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider.Factory.SQLDialect.DB2;
import static org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider.Factory.SQLDialect.POSTGRESQL;

class JDBCFileUtils {

   static JDBCSequentialFileFactoryDriver getDBFileDriver(String driverClass,
                                                          String jdbcConnectionUrl,
                                                          SQLProvider provider) throws SQLException {
      final JDBCSequentialFileFactoryDriver dbDriver;
      final PropertySQLProvider.Factory.SQLDialect sqlDialect = PropertySQLProvider.Factory.identifyDialect(driverClass);
      if (POSTGRESQL.equals(sqlDialect)) {
         dbDriver = new PostgresSequentialSequentialFileDriver();
      } else if (DB2.equals(sqlDialect)) {
         dbDriver = new Db2SequentialFileDriver();
      } else {
         dbDriver = new JDBCSequentialFileFactoryDriver();
      }
      dbDriver.setSqlProvider(provider);
      dbDriver.setJdbcConnectionUrl(jdbcConnectionUrl);
      dbDriver.setJdbcDriverClass(driverClass);
      return dbDriver;
   }

   static JDBCSequentialFileFactoryDriver getDBFileDriver(DataSource dataSource, SQLProvider provider) throws SQLException {
      final JDBCSequentialFileFactoryDriver dbDriver;
      final PropertySQLProvider.Factory.SQLDialect sqlDialect;
      try (Connection connection = dataSource.getConnection()) {
         sqlDialect = PropertySQLProvider.Factory.investigateDialect(connection);
      }
      if (POSTGRESQL.equals(sqlDialect)) {
         dbDriver = new PostgresSequentialSequentialFileDriver(dataSource, provider);
      } else if (DB2.equals(sqlDialect)) {
         dbDriver = new Db2SequentialFileDriver(dataSource, provider);
      } else {
         dbDriver = new JDBCSequentialFileFactoryDriver(dataSource, provider);
      }
      return dbDriver;
   }

   static JDBCSequentialFileFactoryDriver getDBFileDriver(Connection connection, SQLProvider provider) throws SQLException {
      JDBCSequentialFileFactoryDriver dbDriver;
      final PropertySQLProvider.Factory.SQLDialect sqlDialect = PropertySQLProvider.Factory.investigateDialect(connection);
      if (POSTGRESQL.equals(sqlDialect)) {
         dbDriver = new PostgresSequentialSequentialFileDriver(connection, provider);
         dbDriver.setConnection(connection);
      } else if (DB2.equals(sqlDialect)) {
         dbDriver = new Db2SequentialFileDriver(connection, provider);
      } else {
         dbDriver = new JDBCSequentialFileFactoryDriver(connection, provider);
      }
      return dbDriver;
   }
}
