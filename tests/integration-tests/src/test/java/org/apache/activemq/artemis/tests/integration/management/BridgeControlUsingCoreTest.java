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
package org.apache.activemq.artemis.tests.integration.management;

import javax.management.MBeanServerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BridgeControlUsingCoreTest extends ManagementTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server_0;

   private BridgeConfiguration bridgeConfig;

   private ActiveMQServer server_1;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testAttributes() throws Exception {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      CoreMessagingProxy proxy = createProxy(bridgeConfig.getName());

      Assert.assertEquals(bridgeConfig.getName(), proxy.retrieveAttributeValue("name"));
      Assert.assertEquals(bridgeConfig.getDiscoveryGroupName(), proxy.retrieveAttributeValue("discoveryGroupName"));
      Assert.assertEquals(bridgeConfig.getQueueName(), proxy.retrieveAttributeValue("queueName"));
      Assert.assertEquals(bridgeConfig.getForwardingAddress(), proxy.retrieveAttributeValue("forwardingAddress"));
      Assert.assertEquals(bridgeConfig.getFilterString(), proxy.retrieveAttributeValue("filterString"));
      Assert.assertEquals(bridgeConfig.getRetryInterval(), proxy.retrieveAttributeValue("retryInterval", Long.class));
      Assert.assertEquals(bridgeConfig.getRetryIntervalMultiplier(), proxy.retrieveAttributeValue("retryIntervalMultiplier", Double.class));
      Assert.assertEquals(bridgeConfig.getReconnectAttempts(), proxy.retrieveAttributeValue("reconnectAttempts", Integer.class));
      Assert.assertEquals(bridgeConfig.isUseDuplicateDetection(), proxy.retrieveAttributeValue("useDuplicateDetection", Boolean.class));

      Object[] data = (Object[]) proxy.retrieveAttributeValue("staticConnectors");
      Assert.assertEquals(bridgeConfig.getStaticConnectors().get(0), data[0]);

      Assert.assertTrue((Boolean) proxy.retrieveAttributeValue("started"));
   }

   @Test
   public void testStartStop() throws Exception {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      CoreMessagingProxy proxy = createProxy(bridgeConfig.getName());

      // started by the server
      Assert.assertTrue((Boolean) proxy.retrieveAttributeValue("Started"));

      proxy.invokeOperation("stop");
      Assert.assertFalse((Boolean) proxy.retrieveAttributeValue("Started"));

      proxy.invokeOperation("start");
      Assert.assertTrue((Boolean) proxy.retrieveAttributeValue("Started"));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Map<String, Object> acceptorParams = new HashMap<>();
      acceptorParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(), acceptorParams, RandomUtil.randomString());

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(), acceptorParams, RandomUtil.randomString());

      CoreQueueConfiguration sourceQueueConfig = new CoreQueueConfiguration().setAddress(RandomUtil.randomString()).setName(RandomUtil.randomString()).setDurable(false);
      CoreQueueConfiguration targetQueueConfig = new CoreQueueConfiguration().setAddress(RandomUtil.randomString()).setName(RandomUtil.randomString()).setDurable(false);
      List<String> connectors = new ArrayList<>();
      connectors.add(connectorConfig.getName());
      bridgeConfig = new BridgeConfiguration().setName(RandomUtil.randomString()).setQueueName(sourceQueueConfig.getName()).setForwardingAddress(targetQueueConfig.getAddress()).setRetryInterval(RandomUtil.randomPositiveLong()).setRetryIntervalMultiplier(RandomUtil.randomDouble()).setInitialConnectAttempts(RandomUtil.randomPositiveInt()).setReconnectAttempts(RandomUtil.randomPositiveInt()).setReconnectAttemptsOnSameNode(RandomUtil.randomPositiveInt()).setUseDuplicateDetection(RandomUtil.randomBoolean()).setConfirmationWindowSize(RandomUtil.randomPositiveInt()).setStaticConnectors(connectors);

      Configuration conf_1 = createBasicConfig().addAcceptorConfiguration(acceptorConfig).addQueueConfiguration(targetQueueConfig);

      Configuration conf_0 = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY)).addConnectorConfiguration(connectorConfig.getName(), connectorConfig).addQueueConfiguration(sourceQueueConfig).addBridgeConfiguration(bridgeConfig);

      server_1 = addServer(ActiveMQServers.newActiveMQServer(conf_1, MBeanServerFactory.createMBeanServer(), false));
      server_1.start();

      server_0 = addServer(ActiveMQServers.newActiveMQServer(conf_0, mbeanServer, false));
      server_0.start();
   }

   protected CoreMessagingProxy createProxy(final String name) throws Exception {
      CoreMessagingProxy proxy = new CoreMessagingProxy(addServerLocator(createInVMNonHALocator()), ResourceNames.CORE_BRIDGE + name);

      return proxy;
   }
}