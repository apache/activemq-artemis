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
package org.hornetq.tests.integration.management;
import org.junit.Before;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServerFactory;

import org.junit.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 11 dec. 2008 17:38:58
 *
 */
public class BridgeControlUsingCoreTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server_0;

   private BridgeConfiguration bridgeConfig;

   private HornetQServer server_1;

   private ClientSession session;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testAttributes() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      CoreMessagingProxy proxy = createProxy(bridgeConfig.getName());

      Assert.assertEquals(bridgeConfig.getName(), proxy.retrieveAttributeValue("name"));
      Assert.assertEquals(bridgeConfig.getDiscoveryGroupName(),
                          proxy.retrieveAttributeValue("discoveryGroupName"));
      Assert.assertEquals(bridgeConfig.getQueueName(), proxy.retrieveAttributeValue("queueName"));
      Assert.assertEquals(bridgeConfig.getForwardingAddress(),
                          proxy.retrieveAttributeValue("forwardingAddress"));
      Assert.assertEquals(bridgeConfig.getFilterString(), proxy.retrieveAttributeValue("filterString"));
      Assert.assertEquals(bridgeConfig.getRetryInterval(),
                          ((Long)proxy.retrieveAttributeValue("retryInterval")).longValue());
      Assert.assertEquals(bridgeConfig.getRetryIntervalMultiplier(),
                          proxy.retrieveAttributeValue("retryIntervalMultiplier"));
      Assert.assertEquals(bridgeConfig.getReconnectAttempts(),
                          ((Integer)proxy.retrieveAttributeValue("reconnectAttempts")).intValue());
      Assert.assertEquals(bridgeConfig.isUseDuplicateDetection(),
                          ((Boolean)proxy.retrieveAttributeValue("useDuplicateDetection")).booleanValue());

      Object[] data = (Object[])proxy.retrieveAttributeValue("staticConnectors");
      Assert.assertEquals(bridgeConfig.getStaticConnectors().get(0), data[0]);

      Assert.assertTrue((Boolean)proxy.retrieveAttributeValue("started"));
   }

   @Test
   public void testStartStop() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      CoreMessagingProxy proxy = createProxy(bridgeConfig.getName());

      // started by the server
      Assert.assertTrue((Boolean)proxy.retrieveAttributeValue("Started"));

      proxy.invokeOperation("stop");
      Assert.assertFalse((Boolean)proxy.retrieveAttributeValue("Started"));

      proxy.invokeOperation("start");
      Assert.assertTrue((Boolean)proxy.retrieveAttributeValue("Started"));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> acceptorParams = new HashMap<String, Object>();
      acceptorParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         acceptorParams,
                                                                         RandomUtil.randomString());

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          acceptorParams,
                                                                          RandomUtil.randomString());

      CoreQueueConfiguration sourceQueueConfig = new CoreQueueConfiguration()
         .setAddress(RandomUtil.randomString())
         .setName(RandomUtil.randomString())
         .setDurable(false);
      CoreQueueConfiguration targetQueueConfig = new CoreQueueConfiguration()
         .setAddress(RandomUtil.randomString())
         .setName(RandomUtil.randomString())
         .setDurable(false);
      List<String> connectors = new ArrayList<String>();
      connectors.add(connectorConfig.getName());
      bridgeConfig = new BridgeConfiguration()
         .setName(RandomUtil.randomString())
         .setQueueName(sourceQueueConfig.getName())
         .setForwardingAddress(targetQueueConfig.getAddress())
         .setRetryInterval(RandomUtil.randomPositiveLong())
         .setRetryIntervalMultiplier(RandomUtil.randomDouble())
         .setInitialConnectAttempts(RandomUtil.randomPositiveInt())
         .setReconnectAttempts(RandomUtil.randomPositiveInt())
         .setReconnectAttemptsOnSameNode(RandomUtil.randomPositiveInt())
         .setUseDuplicateDetection(RandomUtil.randomBoolean())
         .setConfirmationWindowSize(RandomUtil.randomPositiveInt())
         .setStaticConnectors(connectors);

      Configuration conf_1 = createBasicConfig()
         .addAcceptorConfiguration(acceptorConfig)
         .addQueueConfiguration(targetQueueConfig);

      Configuration conf_0 = createBasicConfig()
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY))
         .addConnectorConfiguration(connectorConfig.getName(), connectorConfig)
         .addQueueConfiguration(sourceQueueConfig)
         .addBridgeConfiguration(bridgeConfig);

      server_1 = addServer(HornetQServers.newHornetQServer(conf_1, MBeanServerFactory.createMBeanServer(), false));
      server_1.start();

      server_0 = addServer(HornetQServers.newHornetQServer(conf_0, mbeanServer, false));
      server_0.start();
      ServerLocator locator =
               addServerLocator(HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(
                                                                                                      INVM_CONNECTOR_FACTORY)));
      ClientSessionFactory sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
      session.start();
   }


   protected CoreMessagingProxy createProxy(final String name) throws Exception
   {
      CoreMessagingProxy proxy = new CoreMessagingProxy(session, ResourceNames.CORE_BRIDGE + name);

      return proxy;
   }
}