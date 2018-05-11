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
package org.apache.activemq.artemis.core.server.impl.jdbc;

import java.sql.SQLException;

import org.apache.activemq.artemis.jdbc.store.drivers.AbstractJDBCDriver;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.junit.Assert;

public class TestJDBCDriver extends AbstractJDBCDriver {

   public static TestJDBCDriver usingConnectionUrl(String jdbcConnectionUrl,
                                                   String jdbcDriverClass,
                                                   SQLProvider provider) {
      return usingConnectionUrl(jdbcConnectionUrl, jdbcDriverClass, provider, false);
   }

   public static TestJDBCDriver usingConnectionUrl(String jdbcConnectionUrl,
                                                   String jdbcDriverClass,
                                                   SQLProvider provider,
                                                   boolean initialize) {
      TestJDBCDriver driver = new TestJDBCDriver(initialize);
      driver.setSqlProvider(provider);
      driver.setJdbcConnectionUrl(jdbcConnectionUrl);
      driver.setJdbcDriverClass(jdbcDriverClass);
      return driver;
   }

   private boolean initialize;

   private TestJDBCDriver(boolean initialize) {
      this.initialize = initialize;
   }

   @Override
   protected void prepareStatements() throws SQLException {
   }

   @Override
   protected void createSchema() throws SQLException {
      try {
         connection.createStatement().execute(sqlProvider.createNodeManagerStoreTableSQL());
         if (initialize) {
            connection.createStatement().execute(sqlProvider.createNodeIdSQL());
            connection.createStatement().execute(sqlProvider.createStateSQL());
            connection.createStatement().execute(sqlProvider.createLiveLockSQL());
            connection.createStatement().execute(sqlProvider.createBackupLockSQL());
         }
      } catch (SQLException e) {
         Assert.fail(e.getMessage());
      }
   }

}
