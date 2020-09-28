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

import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JdbcNodeManagerTest extends ActiveMQTestBase {

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
         System.setProperty("derby.user." + getJdbcUser(), getJdbcPassword());
      }
      dbConf = createDefaultDatabaseStorageConfiguration();
      dbConf.setJdbcUser(getJdbcUser());
      dbConf.setJdbcPassword(getJdbcPassword());
      leaseLockExecutor = Executors.newSingleThreadScheduledExecutor();
   }

   @After
   public void shutdownExecutors() throws InterruptedException {
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         leaseLockExecutor.execute(latch::countDown);
         Assert.assertTrue("the scheduler of the lease lock has some pending task in ", latch.await(10, TimeUnit.SECONDS));
      } finally {
         leaseLockExecutor.shutdownNow();
      }
   }

   @After
   @Override
   public void shutdownDerby() {
      try {
         if (useAuthentication) {
            DriverManager.getConnection("jdbc:derby:;shutdown=true", getJdbcUser(), getJdbcPassword());
         } else {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
         }
      } catch (Exception ignored) {
      }
      if (useAuthentication) {
         System.clearProperty("derby.connection.requireAuthentication");
         System.clearProperty("derby.user." + getJdbcUser());
      }
   }

   protected String getJdbcUser() {
      if (useAuthentication) {
         return System.getProperty("jdbc.user", "testuser");
      } else {
         return null;
      }
   }

   protected String getJdbcPassword() {
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
