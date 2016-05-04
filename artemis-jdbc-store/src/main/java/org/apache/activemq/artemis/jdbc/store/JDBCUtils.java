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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.activemq.artemis.jdbc.store.file.sql.DerbySQLProvider;
import org.apache.activemq.artemis.jdbc.store.file.sql.GenericSQLProvider;
import org.apache.activemq.artemis.jdbc.store.file.sql.SQLProvider;

public class JDBCUtils {

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
                  }
                  catch (Exception e) {
                  }
               }
            });
         }
         return driver;
      }
      catch (ClassNotFoundException cnfe) {
         throw new RuntimeException("Could not find class: " + className);
      }
      catch (Exception e) {
         throw new RuntimeException("Unable to instantiate driver class: ", e);
      }
   }

   public static void createTableIfNotExists(Connection connection, String tableName, String sql) throws SQLException {
      ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null);
      if (!rs.next()) {
         Statement statement = connection.createStatement();
         statement.executeUpdate(sql);
      }
   }

   public static SQLProvider getSQLProvider(String driverClass, String tableName) {
      if (driverClass.contains("derby")) {
         return new DerbySQLProvider(tableName);
      }
      else {
         return new GenericSQLProvider(tableName);
      }
   }
}
