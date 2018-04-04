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

package org.apache.activemq.artemis.core.server.impl.jdbc;

import java.util.UUID;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class JdbcSharedStateManagerTest extends ActiveMQTestBase {

   private DatabaseStorageConfiguration dbConf;
   private SQLProvider sqlProvider;

   @Before
   public void configure() {
      dbConf = createDefaultDatabaseStorageConfiguration();
      sqlProvider = JDBCUtils.getSQLProvider(
         dbConf.getJdbcDriverClassName(),
         dbConf.getNodeManagerStoreTableName(),
         SQLProvider.DatabaseStoreType.NODE_MANAGER);
   }

   private TestJDBCDriver createFakeDriver(boolean initializeTable) {
      return TestJDBCDriver.usingConnectionUrl(
         dbConf.getJdbcConnectionUrl(),
         dbConf.getJdbcDriverClassName(),
         sqlProvider,
         initializeTable);
   }

   private JdbcSharedStateManager createSharedStateManager() {
      return JdbcSharedStateManager.usingConnectionUrl(
         UUID.randomUUID().toString(),
         dbConf.getJdbcLockExpirationMillis(),
         dbConf.getJdbcConnectionUrl(),
         dbConf.getJdbcDriverClassName(),
         sqlProvider);
   }

   @Test(timeout = 10000)
   public void shouldStartIfTableNotExist() throws Exception {
      final JdbcSharedStateManager sharedStateManager = createSharedStateManager();
      try {
         sharedStateManager.destroy();
      } finally {
         sharedStateManager.stop();
      }
   }

   @Test(timeout = 10000)
   public void shouldStartIfTableExistEmpty() throws Exception {
      final TestJDBCDriver fakeDriver = createFakeDriver(false);
      fakeDriver.start();
      final JdbcSharedStateManager sharedStateManager = createSharedStateManager();
      sharedStateManager.stop();
      try {
         fakeDriver.destroy();
      } finally {
         fakeDriver.stop();
      }
   }

   @Test(timeout = 10000)
   public void shouldStartIfTableExistInitialized() throws Exception {
      final TestJDBCDriver fakeDriver = createFakeDriver(true);
      fakeDriver.start();
      final JdbcSharedStateManager sharedStateManager = createSharedStateManager();
      sharedStateManager.stop();
      try {
         fakeDriver.destroy();
      } finally {
         fakeDriver.stop();
      }
   }

   @Test(timeout = 10000)
   public void shouldStartTwoIfTableNotExist() throws Exception {
      final JdbcSharedStateManager liveSharedStateManager = createSharedStateManager();
      final JdbcSharedStateManager backupSharedStateManager = createSharedStateManager();
      backupSharedStateManager.stop();
      try {
         liveSharedStateManager.destroy();
      } finally {
         liveSharedStateManager.stop();
      }
   }
}
