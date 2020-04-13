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
package org.apache.activemq.artemis.tests.integration.bridge;

import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class BridgeRoutingTest extends ActiveMQTestBase {

   private ActiveMQServer server0;
   private ActiveMQServer server1;

   private final boolean netty;

   @Parameterized.Parameters(name = "isNetty={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{true}, {false}});
   }

   public BridgeRoutingTest(boolean isNetty) {
      this.netty = isNetty;
   }

   protected boolean isNetty() {
      return netty;
   }

   private String getServer0URL() {
      return isNetty() ? "tcp://localhost:61616" : "vm://0";
   }

   private String getServer1URL() {
      return isNetty() ? "tcp://localhost:61617" : "vm://1";
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server0 = createServer(false, createBasicConfig());
      server1 = createServer(false, createBasicConfig());
      server0.getConfiguration().addAcceptorConfiguration("acceptor", getServer0URL());
      server0.getConfiguration().addConnectorConfiguration("connector", getServer1URL());
      server1.getConfiguration().addAcceptorConfiguration("acceptor", getServer1URL());
      server0.start();
      server1.start();
   }

   @Test
   public void testAnycastBridge() throws Exception {
      testBridgeInternal(RoutingType.MULTICAST, RoutingType.ANYCAST, ComponentConfigurationRoutingType.ANYCAST, 0, 1);
   }

   @Test
   public void testAnycastBridgeNegative() throws Exception {
      testBridgeInternal(RoutingType.MULTICAST, RoutingType.ANYCAST, ComponentConfigurationRoutingType.PASS, 500, 0);
   }

   @Test
   public void testMulticastBridge() throws Exception {
      testBridgeInternal(RoutingType.ANYCAST, RoutingType.MULTICAST, ComponentConfigurationRoutingType.MULTICAST, 0, 1);
   }

   @Test
   public void testMulticastBridgeNegative() throws Exception {
      testBridgeInternal(RoutingType.ANYCAST, RoutingType.MULTICAST, ComponentConfigurationRoutingType.PASS, 500, 0);
   }

   @Test
   public void testPassBridge() throws Exception {
      testBridgeInternal(RoutingType.MULTICAST, RoutingType.MULTICAST, ComponentConfigurationRoutingType.PASS, 0, 1);
   }

   @Test
   public void testPassBridge2() throws Exception {
      testBridgeInternal(RoutingType.ANYCAST, RoutingType.ANYCAST, ComponentConfigurationRoutingType.PASS, 0, 1);
   }

   @Test
   public void testPassBridgeNegative() throws Exception {
      testBridgeInternal(RoutingType.ANYCAST, RoutingType.MULTICAST, ComponentConfigurationRoutingType.PASS, 500, 0);
   }

   @Test
   public void testStripBridge() throws Exception {
      testBridgeInternal(RoutingType.MULTICAST, RoutingType.ANYCAST, ComponentConfigurationRoutingType.STRIP, 0, 1);
   }

   @Test
   public void testStripBridge2() throws Exception {
      testBridgeInternal(RoutingType.ANYCAST, RoutingType.MULTICAST, ComponentConfigurationRoutingType.STRIP, 0, 1);
   }

   private void testBridgeInternal(RoutingType sourceRoutingType,
                                   RoutingType destinationRoutingType,
                                   ComponentConfigurationRoutingType bridgeRoutingType,
                                   long sleepTime,
                                   int destinationMessageCount) throws Exception {
      SimpleString source = SimpleString.toSimpleString("source");
      SimpleString destination = SimpleString.toSimpleString("destination");

      server0.createQueue(new QueueConfiguration(source).setRoutingType(sourceRoutingType));
      server1.createQueue(new QueueConfiguration(destination).setRoutingType(destinationRoutingType));

      server0.deployBridge(new BridgeConfiguration()
                              .setRoutingType(bridgeRoutingType)
                              .setName("bridge")
                              .setForwardingAddress(destination.toString())
                              .setQueueName(source.toString())
                              .setConfirmationWindowSize(10)
                              .setStaticConnectors(Arrays.asList("connector")));

      try (ServerLocator locator = ActiveMQClient.createServerLocator(getServer0URL());
           ClientSessionFactory sessionFactory = locator.createSessionFactory();
           ClientSession session = sessionFactory.createSession();
           ClientProducer producer = session.createProducer(source)) {
         producer.send(session.createMessage(true).setRoutingType(sourceRoutingType));
      }

      Wait.waitFor(() -> server0.locateQueue(source).getMessageCount() == 0, 2000, 100);
      Wait.waitFor(() -> server0.getClusterManager().getBridges().get("bridge").getMetrics().getMessagesAcknowledged() == 1, 2000, 100);
      Thread.sleep(sleepTime);
      assertTrue(Wait.waitFor(() -> server1.locateQueue(destination).getMessageCount() == destinationMessageCount, 2000, 100));
   }
}