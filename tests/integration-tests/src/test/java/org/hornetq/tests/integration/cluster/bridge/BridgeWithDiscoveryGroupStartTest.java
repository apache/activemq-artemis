/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.cluster.bridge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * A BridgeWithDiscoveryGroupStartTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class BridgeWithDiscoveryGroupStartTest extends ServiceTestBase
{

   private static final int TIMEOUT = 2000;

   protected boolean isNetty()
   {
      return false;
   }

   @Test
   public void testStartStop() throws Exception
   {
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createClusteredServerWithParams(isNetty(), 0, true, server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      if (isNetty())
      {
         server1Params.put("port", TransportConstants.DEFAULT_PORT + 1);
      }
      else
      {
         server1Params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      HornetQServer server1 = createClusteredServerWithParams(isNetty(), 1, true, server1Params);
      ServerLocator locator = null;
      try
      {
         Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
         TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params);
         TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params);
         connectors.put(server1tc.getName(), server1tc);

         server0.getConfiguration().setConnectorConfigurations(connectors);

         final String testAddress = "testAddress";
         final String queueName0 = "queue0";
         final String forwardAddress = "forwardAddress";
         final String queueName1 = "queue1";

         final String groupAddress = getUDPDiscoveryAddress();
         final int port = getUDPDiscoveryPort();


         ArrayList<String> list = new ArrayList<String>();
         list.add(server1tc.getName());

         UDPBroadcastGroupConfiguration endpoint = new UDPBroadcastGroupConfiguration(groupAddress, port, null, -1);

         BroadcastGroupConfiguration bcConfig = new BroadcastGroupConfiguration("bg1",
                                                                                250,
                                                                                list,
                                                                                endpoint);

         server0.getConfiguration().getBroadcastGroupConfigurations().add(bcConfig);

         DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration("dg1", 5000, 5000, endpoint);

         server0.getConfiguration().getDiscoveryGroupConfigurations().put(dcConfig.getName(), dcConfig);

         final String bridgeName = "bridge1";

         ArrayList<String> staticConnectors = new ArrayList<String>();
         staticConnectors.add(server1tc.getName());
         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                           queueName0,
                                                                           forwardAddress,
                                                                           null,
                                                                           null,
                                                                           HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                           HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                                           HornetQClient.DEFAULT_CONNECTION_TTL,
                                                                           1000,
                                                                           HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                                           1d,
                                                                           -1,
                                                                           0,
                                                                           0,
                                                                           true,
                                                                           1024,
                                                                           staticConnectors,
                                                                           false,
                                                                           HornetQDefaultConfiguration.getDefaultClusterUser(),
                                                                           HornetQDefaultConfiguration.getDefaultClusterPassword());

         List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
         bridgeConfigs.add(bridgeConfiguration);
         server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

         CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName0, null, true);
         List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
         queueConfigs0.add(queueConfig0);
         server0.getConfiguration().setQueueConfigurations(queueConfigs0);

         CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName1, null, true);
         List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
         queueConfigs1.add(queueConfig1);
         server1.getConfiguration().setQueueConfigurations(queueConfigs1);

         server1.start();
         server0.start();

         locator = HornetQClient.createServerLocatorWithoutHA(server0tc, server1tc);
         ClientSessionFactory sf0 = locator.createSessionFactory(server0tc);

         ClientSessionFactory sf1 = locator.createSessionFactory(server1tc);

         ClientSession session0 = sf0.createSession(false, true, true);

         ClientSession session1 = sf1.createSession(false, true, true);

         ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

         ClientConsumer consumer1 = session1.createConsumer(queueName1);

         session1.start();

         final int numMessages = 10;

         final SimpleString propKey = new SimpleString("testkey");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive(BridgeWithDiscoveryGroupStartTest.TIMEOUT);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);

         bridge.stop();
         bridge.flushExecutor();

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         Assert.assertNull(consumer1.receiveImmediate());

         bridge.start();

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive(BridgeWithDiscoveryGroupStartTest.TIMEOUT);

            Assert.assertNotNull(message);

            Assert.assertEquals(i, message.getObjectProperty(propKey));

            message.acknowledge();
         }

         Assert.assertNull(consumer1.receiveImmediate());

         session0.close();

         session1.close();

         sf0.close();

         sf1.close();
      }
      finally
      {
         if (locator != null)
         {
            locator.close();
         }
         server0.stop();

         server1.stop();
      }

   }

   /**
    * @return
    */
   private String getConnector()
   {
      if (isNetty())
      {
         return NettyConnectorFactory.class.getName();
      }
      else
      {
         return InVMConnectorFactory.class.getName();
      }
   }
}
