/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jdbc.store.drivers;

import org.apache.activemq.artemis.jdbc.store.drivers.derby.DerbySQLProvider;
import org.apache.activemq.artemis.jdbc.store.drivers.mysql.MySQLSQLProvider;
import org.apache.activemq.artemis.jdbc.store.drivers.postgres.PostgresSQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.GenericSQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.jboss.logging.Logger;

public class JDBCUtils {

   private static final Logger logger = Logger.getLogger(JDBCUtils.class);

   public static SQLProvider.Factory getSQLProviderFactory(String url) {
      SQLProvider.Factory factory;
      if (url.contains("derby")) {
         logger.tracef("getSQLProvider Returning Derby SQL provider for url::%s", url);
         factory = new DerbySQLProvider.Factory();
      } else if (url.contains("postgres")) {
         logger.tracef("getSQLProvider Returning postgres SQL provider for url::%s", url);
         factory = new PostgresSQLProvider.Factory();
      } else if (url.contains("mysql")) {
         logger.tracef("getSQLProvider Returning mysql SQL provider for url::%s", url);
         factory = new MySQLSQLProvider.Factory();
      } else {
         logger.tracef("getSQLProvider Returning generic SQL provider for url::%s", url);
         factory = new GenericSQLProvider.Factory();
      }
      return factory;
   }

   public static SQLProvider getSQLProvider(String driverClass, String tableName) {
      SQLProvider.Factory factory;
      if (driverClass.contains("derby")) {
         logger.tracef("getSQLProvider Returning Derby SQL provider for driver::%s, tableName::%s", driverClass, tableName);
         factory = new DerbySQLProvider.Factory();
      } else if (driverClass.contains("postgres")) {
         logger.tracef("getSQLProvider Returning postgres SQL provider for driver::%s, tableName::%s", driverClass, tableName);
         factory = new PostgresSQLProvider.Factory();
      } else if (driverClass.contains("mysql")) {
         logger.tracef("getSQLProvider Returning mysql SQL provider for driver::%s, tableName::%s", driverClass, tableName);
         factory = new MySQLSQLProvider.Factory();
      } else {
         logger.tracef("getSQLProvider Returning generic SQL provider for driver::%s, tableName::%s", driverClass, tableName);
         factory = new GenericSQLProvider.Factory();
      }
      return factory.create(tableName);
   }

}
