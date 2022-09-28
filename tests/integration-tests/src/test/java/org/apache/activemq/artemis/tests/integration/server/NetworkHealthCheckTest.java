/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NetworkHealthCheck;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler.LogLevel;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class NetworkHealthCheckTest extends ActiveMQTestBase {

   private static final String HEALTH_CHECK_LOGGER_NAME = NetworkHealthCheck.class.getName();
   private static LogLevel previousLevel = null;

   @BeforeClass
   public static void prepareLogger() {
      previousLevel = AssertionLoggerHandler.setLevel(HEALTH_CHECK_LOGGER_NAME, LogLevel.DEBUG);
      AssertionLoggerHandler.startCapture();
   }

   @AfterClass
   public static void clearLogger() {
      try {
         AssertionLoggerHandler.stopCapture();
      } finally {
         AssertionLoggerHandler.setLevel(HEALTH_CHECK_LOGGER_NAME, previousLevel);
      }
   }


   @Test
   public void testCustomIpv4Command() throws Exception {
      final int checkingTimeout = 1;
      final String checkingHost = "10.0.0.1";
      final String customIpv4Command = "DUMMYPING %d %s";


      Configuration config = createBasicConfig()
         .setNetworkCheckTimeout(checkingTimeout)
         .setNetworkCheckList(checkingHost)
         .setNetworkCheckPingCommand(customIpv4Command);

      ActiveMQServer server = createServer(false, config);

      server.start();
      try {
         Assert.assertTrue(AssertionLoggerHandler.findText("executing ping:: " + String.format(customIpv4Command, checkingTimeout, checkingHost)));
         Assert.assertFalse(AssertionLoggerHandler.findText(String.format(NetworkHealthCheck.IPV4_DEFAULT_COMMAND, checkingTimeout, checkingHost)));
      } finally {
         server.stop();
      }
   }
}
