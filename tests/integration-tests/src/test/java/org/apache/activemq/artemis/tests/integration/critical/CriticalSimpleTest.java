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

package org.apache.activemq.artemis.tests.integration.critical;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.utils.critical.CriticalAnalyzerPolicy;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.critical.CriticalComponent;
import org.junit.Assert;
import org.junit.Test;

public class CriticalSimpleTest extends ActiveMQTestBase {
   @Test
   public void testSimpleShutdown() throws Exception {

      Configuration configuration = createDefaultConfig(false);
      configuration.setCriticalAnalyzerCheckPeriod(10).setCriticalAnalyzerPolicy(CriticalAnalyzerPolicy.SHUTDOWN);
      ActiveMQServer server = createServer(false, configuration, AddressSettings.DEFAULT_PAGE_SIZE, AddressSettings.DEFAULT_MAX_SIZE_BYTES);
      server.start();

      try {

         CountDownLatch latch = new CountDownLatch(1);

         server.getConfiguration().registerBrokerPlugin(new ActiveMQServerPlugin() {
            @Override
            public void criticalFailure(CriticalComponent components) throws ActiveMQException {
               latch.countDown();
            }
         });

         server.getCriticalAnalyzer().add(new CriticalComponent() {
            @Override
            public boolean isExpired(long timeout) {
               return true;
            }
         });

         Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
         Wait.waitFor(() -> !server.isStarted());


         Assert.assertFalse(server.isStarted());

      } finally {
         server.stop();
      }


   }

   @Test
   public void testCriticalOff() throws Exception {

      Configuration configuration = createDefaultConfig(false);
      configuration.setCriticalAnalyzerCheckPeriod(10).setCriticalAnalyzer(false);
      ActiveMQServer server = createServer(false, configuration, AddressSettings.DEFAULT_PAGE_SIZE, AddressSettings.DEFAULT_MAX_SIZE_BYTES);
      server.start();

      try {
         server.getCriticalAnalyzer().add(new CriticalComponent() {
            @Override
            public boolean isExpired(long timeout) {
               return true;
            }
         });

         Wait.waitFor(() -> !server.isStarted(), 500, 10);


         Assert.assertTrue(server.isStarted());

      } finally {
         server.stop();
      }


   }

}
