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

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.tests.integration.SimpleNotificationService;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BridgeControlTest extends ManagementTestBase {

   private ActiveMQServer server_0;
   private ActiveMQServer server_1;

   private BridgeConfiguration bridgeConfig;

   @Test
   public void testAttributes() throws Exception {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      BridgeControl bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      Assert.assertEquals(bridgeConfig.getName(), bridgeControl.getName());
      Assert.assertEquals(bridgeConfig.getDiscoveryGroupName(), bridgeControl.getDiscoveryGroupName());
      Assert.assertEquals(bridgeConfig.getQueueName(), bridgeControl.getQueueName());
      Assert.assertEquals(bridgeConfig.getForwardingAddress(), bridgeControl.getForwardingAddress());
      Assert.assertEquals(bridgeConfig.getFilterString(), bridgeControl.getFilterString());
      Assert.assertEquals(bridgeConfig.getRetryInterval(), bridgeControl.getRetryInterval());
      Assert.assertEquals(bridgeConfig.getRetryIntervalMultiplier(), bridgeControl.getRetryIntervalMultiplier(), 0.000001);
      Assert.assertEquals(bridgeConfig.getReconnectAttempts(), bridgeControl.getReconnectAttempts());
      Assert.assertEquals(bridgeConfig.isUseDuplicateDetection(), bridgeControl.isUseDuplicateDetection());

      String[] connectorPairData = bridgeControl.getStaticConnectors();
      Assert.assertEquals(bridgeConfig.getStaticConnectors().get(0), connectorPairData[0]);

      Assert.assertTrue(bridgeControl.isStarted());
   }

   @Test
   public void testStartStop() throws Exception {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      BridgeControl bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      // started by the server
      Assert.assertTrue(bridgeControl.isStarted());

      bridgeControl.stop();
      Assert.assertFalse(bridgeControl.isStarted());

      bridgeControl.start();
      Assert.assertTrue(bridgeControl.isStarted());
   }

   @Test
   public void testNotifications() throws Exception {
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      BridgeControl bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      server_0.getManagementService().addNotificationListener(notifListener);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      bridgeControl.stop();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(CoreNotificationType.BRIDGE_STOPPED, notif.getType());
      Assert.assertEquals(bridgeControl.getName(), notif.getProperties().getSimpleStringProperty(new SimpleString("name")).toString());

      bridgeControl.start();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(CoreNotificationType.BRIDGE_STARTED, notif.getType());
      Assert.assertEquals(bridgeControl.getName(), notif.getProperties().getSimpleStringProperty(new SimpleString("name")).toString());
   }

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

      Configuration conf_1 = createBasicConfig().addAcceptorConfiguration(acceptorConfig).addQueueConfiguration(targetQueueConfig);

      bridgeConfig = new BridgeConfiguration().setName(RandomUtil.randomString()).setQueueName(sourceQueueConfig.getName()).setForwardingAddress(targetQueueConfig.getAddress()).setRetryInterval(RandomUtil.randomPositiveLong()).setRetryIntervalMultiplier(RandomUtil.randomDouble()).setInitialConnectAttempts(RandomUtil.randomPositiveInt()).setReconnectAttempts(RandomUtil.randomPositiveInt()).setReconnectAttemptsOnSameNode(RandomUtil.randomPositiveInt()).setUseDuplicateDetection(RandomUtil.randomBoolean()).setConfirmationWindowSize(RandomUtil.randomPositiveInt()).setStaticConnectors(connectors).setPassword(CLUSTER_PASSWORD);

      Configuration conf_0 = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName())).addConnectorConfiguration(connectorConfig.getName(), connectorConfig).addQueueConfiguration(sourceQueueConfig).addBridgeConfiguration(bridgeConfig);

      server_1 = addServer(ActiveMQServers.newActiveMQServer(conf_1, MBeanServerFactory.createMBeanServer(), false));
      addServer(server_1);
      server_1.start();

      server_0 = addServer(ActiveMQServers.newActiveMQServer(conf_0, mbeanServer, false));
      addServer(server_0);
      server_0.start();
   }

   protected BridgeControl createBridgeControl(final String name, final MBeanServer mbeanServer1) throws Exception {
      return ManagementControlHelper.createBridgeControl(name, mbeanServer1);
   }
}
