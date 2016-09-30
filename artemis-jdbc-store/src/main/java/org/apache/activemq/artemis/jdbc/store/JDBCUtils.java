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
package org.apache.activemq.artemis.jdbc.store;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.activemq.artemis.jdbc.store.drivers.derby.DerbySQLProvider;
import org.apache.activemq.artemis.jdbc.store.drivers.mysql.MySQLSQLProvider;
import org.apache.activemq.artemis.jdbc.store.drivers.postgres.PostgresSQLProvider;
import org.apache.activemq.artemis.jdbc.store.drivers.postgres.PostgresSequentialSequentialFileDriver;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactoryDriver;
import org.apache.activemq.artemis.jdbc.store.sql.GenericSQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.jboss.logging.Logger;

public class JDBCUtils {

   private static final Logger logger = Logger.getLogger(JDBCUtils.class);

   public static Driver getDriver(String className) throws Exception {

      try {
         Driver driver = (Driver) Class.forName(className).newInstance();

         // Shutdown the derby if using the derby embedded driver.
         if (className.equals("org.apache.derby.jdbc.EmbeddedDriver")) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
               @Override
               public void run() {
                  try {
                     DriverManager.getConnection("jdbc:derby:;shutdown=true");
                  } catch (Exception e) {
                  }
               }
            });
         }
         return driver;
      } catch (ClassNotFoundException cnfe) {
         throw new RuntimeException("Could not find class: " + className);
      } catch (Exception e) {
         throw new RuntimeException("Unable to instantiate driver class: ", e);
      }
   }

   public static void createTableIfNotExists(Connection connection, String tableName, String sql) throws SQLException {
      logger.tracef("Validating if table %s didn't exist before creating", tableName);
      try {
         connection.setAutoCommit(false);
         try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null)) {
            if (rs != null && !rs.next()) {
               logger.tracef("Table %s did not exist, creating it with SQL=%s", tableName, sql);
               try (Statement statement = connection.createStatement()) {
                  statement.executeUpdate(sql);
               }
            }
         }
         connection.commit();
      } catch (SQLException e) {
         connection.rollback();
      }

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

   public static JDBCSequentialFileFactoryDriver getDBFileDriver(String driverClass,
                                                                 String jdbcConnectionUrl,
                                                                 SQLProvider provider) throws SQLException {
      JDBCSequentialFileFactoryDriver dbDriver = new JDBCSequentialFileFactoryDriver();
      dbDriver.setSqlProvider(provider);
      dbDriver.setJdbcConnectionUrl(jdbcConnectionUrl);
      dbDriver.setJdbcDriverClass(driverClass);
      return dbDriver;
   }

   public static JDBCSequentialFileFactoryDriver getDBFileDriver(DataSource dataSource,
                                                                 String tableName,
                                                                 SQLProvider provider) throws SQLException {
      JDBCSequentialFileFactoryDriver dbDriver;
      if (provider instanceof PostgresSQLProvider) {
         dbDriver = new PostgresSequentialSequentialFileDriver();
         dbDriver.setDataSource(dataSource);
      } else {
         dbDriver = new JDBCSequentialFileFactoryDriver(tableName, dataSource, provider);
      }
      return dbDriver;
   }

}
