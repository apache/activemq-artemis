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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class JdbcNodeManagerTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Parameterized.Parameter
   public boolean useAuthentication;
   private DatabaseStorageConfiguration dbConf;
   private ScheduledExecutorService leaseLockExecutor;

   @Parameterized.Parameters(name = "authentication = {0}")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{{false}, {true}});
   }

   @Before
   public void configure() {
      if (useAuthentication) {
         System.setProperty("derby.connection.requireAuthentication", "true");
         System.setProperty("derby.user." + getJDBCUser(), getJDBCPassword());
      }
      dbConf = createDefaultDatabaseStorageConfiguration();
      dbConf.setJdbcUser(getJDBCUser());
      dbConf.setJdbcPassword(getJDBCPassword());
      leaseLockExecutor = Executors.newSingleThreadScheduledExecutor();
      runAfter(leaseLockExecutor::shutdownNow);
   }


   @Override
   protected String getJDBCUser() {
      if (useAuthentication) {
         return System.getProperty("jdbc.user", "testuser");
      } else {
         return null;
      }
   }

   @Override
   protected String getJDBCPassword() {
      if (useAuthentication) {
         return System.getProperty("jdbc.password", "testpassword");
      } else {
         return null;
      }
   }

   @Test
   public void shouldStartAndStopGracefullyTest() throws Exception {
      final JdbcNodeManager nodeManager = JdbcNodeManager.with(dbConf, leaseLockExecutor, null);
      nodeManager.start();
      nodeManager.stop();
   }

}
