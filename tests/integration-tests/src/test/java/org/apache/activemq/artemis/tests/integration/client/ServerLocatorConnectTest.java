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
package org.apache.activemq.artemis.tests.integration.client;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.uri.ServerLocatorParser;
import org.junit.Before;
import org.junit.Test;

public class ServerLocatorConnectTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      Configuration configuration = createDefaultConfig(isNetty());
      server = createServer(false, configuration);
      server.start();
   }

   @Test
   public void testURL() throws Exception {
      ServerLocatorParser parser = new ServerLocatorParser();
      // This URL was failing in some ConnectionFactoryTests.
      // The issue seemed to be the # to be creating extra spaces on the parsing
      // Added some treatment to fix that, and I kept the test here.
      URI uri = new URI("tcp://localhost:61616?&blockOnNonDurableSend=true&" +
                           "retryIntervalMultiplier=1.0&maxRetryInterval=2000&producerMaxRate=-1&" +
                           "blockOnDurableSend=true&connectionTTL=60000&compressLargeMessage=false&reconnectAttempts=0&" +
                           "cacheLargeMessagesClient=false&scheduledThreadPoolMaxSize=5&useGlobalPools=true&" +
                           "callFailoverTimeout=-1&initialConnectAttempts=1&clientFailureCheckPeriod=30000&" +
                           "blockOnAcknowledge=true&consumerWindowSize=1048576&minLargeMessageSize=102400&" +
                           "autoGroup=false&threadPoolMaxSize=-1&confirmationWindowSize=-1&" +
                           "transactionBatchSize=1048576&callTimeout=30000&preAcknowledge=false&" +
                           "connectionLoadBalancingPolicyClassName=org.apache.activemq.artemis.api.core.client.loadbalance." +
                           "RoundRobinConnectionLoadBalancingPolicy&dupsOKBatchSize=1048576&initialMessagePacketSize=1500&" +
                           "consumerMaxRate=-1&retryInterval=2000&failoverOnInitialConnection=false&producerWindowSize=65536&" +
                           "port=61616&host=localhost#");

      // try it a few times to make sure it fails if it's broken
      for (int i = 0; i < 10; i++) {
         ServerLocator locator = parser.newObject(uri, null);
         ClientSessionFactory csf = createSessionFactory(locator);
         csf.close();
         locator.close();
      }

   }

   @Test
   public void testSingleConnectorSingleServer() throws Exception {
      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())));
      ClientSessionFactory csf = createSessionFactory(locator);
      csf.close();
      locator.close();
   }

   @Test
   public void testSingleConnectorSingleServerConnect() throws Exception {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())));
      ClientSessionFactoryInternal csf = locator.connect();
      assertNotNull(csf);
      assertEquals(csf.numConnections(), 1);
      locator.close();
   }

   @Test
   public void testMultipleConnectorSingleServerConnect() throws Exception {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(1, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(2, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(3, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(4, isNetty())));
      ClientSessionFactoryInternal csf = locator.connect();
      assertNotNull(csf);
      assertEquals(csf.numConnections(), 1);
      locator.close();
   }

   @Test
   public void testMultipleConnectorSingleServerConnectReconnect() throws Exception {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(1, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(2, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(3, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(4, isNetty())));
      locator.setReconnectAttempts(15);
      ClientSessionFactoryInternal csf = locator.connect();
      assertNotNull(csf);
      assertEquals(csf.numConnections(), 1);
      locator.close();
   }

   @Test
   public void testMultipleConnectorSingleServerNoConnect() throws Exception {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(1, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(2, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(3, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(4, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(5, isNetty())));
      ClientSessionFactoryInternal csf = null;
      try {
         csf = locator.connect();
      } catch (ActiveMQNotConnectedException nce) {
         //ok
      } catch (Exception e) {
         assertTrue(e instanceof ActiveMQException);
         fail("Invalid Exception type:" + ((ActiveMQException) e).getType());
      }
      assertNull(csf);
      locator.close();
   }

   @Test
   public void testMultipleConnectorSingleServerNoConnectAttemptReconnect() throws Exception {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(1, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(2, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(3, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(4, isNetty())), createTransportConfiguration(isNetty(), false, generateParams(5, isNetty())));
      locator.setReconnectAttempts(15);
      CountDownLatch countDownLatch = new CountDownLatch(1);
      Connector target = new Connector(locator, countDownLatch);
      Thread t = new Thread(target);
      t.start();
      //let them get started
      Thread.sleep(500);
      locator.close();
      assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
      assertNull(target.csf);
   }

   public boolean isNetty() {
      return true;
   }

   static class Connector implements Runnable {

      private final ServerLocatorInternal locator;
      ClientSessionFactory csf = null;
      CountDownLatch latch;
      Exception e;

      Connector(ServerLocatorInternal locator, CountDownLatch latch) {
         this.locator = locator;
         this.latch = latch;
      }

      @Override
      public void run() {
         try {
            csf = locator.connect();
         } catch (Exception e) {
            this.e = e;
         }
         latch.countDown();
      }
   }
}
