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
package org.apache.activemq.artemis.tests.integration.security;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLDecoder;

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.SpawnedTestBase;
import org.apache.activemq.artemis.tests.util.SpawnedVMSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NettySecurityClientTest extends SpawnedTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private ActiveMQServer messagingService;

   @Test
   public void testProducerConsumerClientWithoutSecurityManager() throws Exception {
      doTestProducerConsumerClient(false);
   }

   @Test
   public void testProducerConsumerClientWithSecurityManager() throws Exception {
      doTestProducerConsumerClient(true);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      ConfigurationImpl config = createBasicConfig().addAcceptorConfiguration(getNettyAcceptorTransportConfiguration(true));
      messagingService = createServer(false, config);
      messagingService.start();
      waitForServerToStart(messagingService);
   }

   private void doTestProducerConsumerClient(final boolean withSecurityManager) throws Exception {
      String[] vmargs = new String[0];
      if (withSecurityManager) {
         URL securityPolicyURL = Thread.currentThread().getContextClassLoader().getResource("restricted-security-client.policy");
         vmargs = new String[]{"-Djava.security.manager", "-Djava.security.policy=" + URLDecoder.decode(securityPolicyURL.getPath(), "UTF-8")};
      }

      // spawn a JVM that creates a client with a security manager which sends and receives a
      // test message
      Process p = SpawnedVMSupport.spawnVM(SimpleClient.class.getName(), "-Xms512m", "-Xmx512m", vmargs, true, true, false, new String[]{NETTY_CONNECTOR_FACTORY});

      InputStreamReader isr = new InputStreamReader(p.getInputStream());

      BufferedReader br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null) {
         line = line.replace('|', '\n');
         if (line.startsWith("Listening")) {
            continue;
         } else if ("OK".equals(line.trim())) {
            break;
         } else {
            //Assert.fail("Exception when starting the client: " + line);
            System.out.println(line);
         }
      }

      SpawnedVMSupport.startLogger(SimpleClient.class.getName(), p);

      // the client VM should exit by itself. If it doesn't, that means we have a problem
      // and the test will timeout
      NettySecurityClientTest.log.debug("waiting for the client VM to exit ...");
      p.waitFor();

      Assert.assertEquals("client VM did not exit cleanly", 0, p.exitValue());
   }
}
