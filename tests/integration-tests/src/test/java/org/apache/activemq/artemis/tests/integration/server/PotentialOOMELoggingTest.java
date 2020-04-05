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
package org.apache.activemq.artemis.tests.integration.server;

import java.util.UUID;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PotentialOOMELoggingTest extends ActiveMQTestBase {

   @BeforeClass
   public static void prepareLogger() {
      AssertionLoggerHandler.startCapture();
   }

   @Test
   /**
    * When running this test from an IDE add this to the test command line so that the AssertionLoggerHandler works properly:
    *
    *   -Djava.util.logging.manager=org.jboss.logmanager.LogManager  -Dlogging.configuration=file:<path_to_source>/tests/config/logging.properties
    */ public void testBlockLogging() throws Exception {
      ActiveMQServer server = createServer(false, createDefaultInVMConfig());
      for (int i = 0; i < 10000; i++) {
         server.getConfiguration().addQueueConfiguration(new QueueConfiguration(UUID.randomUUID().toString()));
      }
      server.getConfiguration().setGlobalMaxSize(-1);
      server.getConfiguration().getAddressesSettings().put("#", new AddressSettings().setMaxSizeBytes(10485760 * 10));
      server.start();

      // Using the code only so the test doesn't fail just because someone edits the log text
      Assert.assertTrue("Expected to find 222205", AssertionLoggerHandler.findText("AMQ222205"));
   }

   @AfterClass
   public static void clearLogger() {
      AssertionLoggerHandler.stopCapture();
   }
}
