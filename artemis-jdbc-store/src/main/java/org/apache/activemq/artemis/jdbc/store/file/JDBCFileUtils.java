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
import java.sql.SQLException;

import org.apache.activemq.artemis.jdbc.store.drivers.postgres.PostgresSQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

class JDBCFileUtils {

   static JDBCSequentialFileFactoryDriver getDBFileDriver(String driverClass,
                                                          String jdbcConnectionUrl,
                                                          SQLProvider provider) throws SQLException {
      JDBCSequentialFileFactoryDriver dbDriver = new JDBCSequentialFileFactoryDriver();
      dbDriver.setSqlProvider(provider);
      dbDriver.setJdbcConnectionUrl(jdbcConnectionUrl);
      dbDriver.setJdbcDriverClass(driverClass);
      return dbDriver;
   }

   static JDBCSequentialFileFactoryDriver getDBFileDriver(DataSource dataSource, SQLProvider provider) throws SQLException {
      JDBCSequentialFileFactoryDriver dbDriver;
      if (provider instanceof PostgresSQLProvider) {
         dbDriver = new PostgresSequentialSequentialFileDriver();
         dbDriver.setDataSource(dataSource);
      } else {
         dbDriver = new JDBCSequentialFileFactoryDriver(dataSource, provider);
      }
      return dbDriver;
   }
}
