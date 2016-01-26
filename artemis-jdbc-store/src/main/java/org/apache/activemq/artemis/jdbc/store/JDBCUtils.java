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
import java.util.Collections;
import java.util.List;

import org.apache.derby.jdbc.AutoloadedDriver;

public class JDBCUtils {

   public static Driver getDriver() throws Exception {
      Driver dbDriver = null;
      // Load Database driver, sets Derby Autoloaded Driver as lowest priority.
      List<Driver> drivers = Collections.list(DriverManager.getDrivers());
      if (drivers.size() <= 2 && drivers.size() > 0) {
         dbDriver = drivers.get(0);
         boolean isDerby = dbDriver instanceof AutoloadedDriver;

         if (drivers.size() > 1 && isDerby) {
            dbDriver = drivers.get(1);
         }

         if (isDerby) {
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
      }
      else {
         String error = drivers.isEmpty() ? "No DB driver found on class path" : "Too many DB drivers on class path, not sure which to use";
         throw new RuntimeException(error);
      }
      return dbDriver;
   }

   public static void createTableIfNotExists(Connection connection, String tableName, String sql) throws SQLException {
      ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null);
      if (!rs.next()) {
         Statement statement = connection.createStatement();
         statement.executeUpdate(sql);
      }
   }
}
