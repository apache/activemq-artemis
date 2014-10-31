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

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.junit.Assert;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.management.BridgeControl;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.management.Notification;
import org.hornetq.tests.integration.SimpleNotificationService;
import org.hornetq.tests.util.RandomUtil;

/**
 * A BridgeControlTest
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class BridgeControlTest extends ManagementTestBase
{
   private HornetQServer server_0;
   private HornetQServer server_1;

   private BridgeConfiguration bridgeConfig;

   @Test
   public void testAttributes() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      BridgeControl bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      Assert.assertEquals(bridgeConfig.getName(), bridgeControl.getName());
      Assert.assertEquals(bridgeConfig.getDiscoveryGroupName(), bridgeControl.getDiscoveryGroupName());
      Assert.assertEquals(bridgeConfig.getQueueName(), bridgeControl.getQueueName());
      Assert.assertEquals(bridgeConfig.getForwardingAddress(), bridgeControl.getForwardingAddress());
      Assert.assertEquals(bridgeConfig.getFilterString(), bridgeControl.getFilterString());
      Assert.assertEquals(bridgeConfig.getRetryInterval(), bridgeControl.getRetryInterval());
      Assert.assertEquals(bridgeConfig.getRetryIntervalMultiplier(), bridgeControl.getRetryIntervalMultiplier(),
                          0.000001);
      Assert.assertEquals(bridgeConfig.getReconnectAttempts(), bridgeControl.getReconnectAttempts());
      Assert.assertEquals(bridgeConfig.isUseDuplicateDetection(), bridgeControl.isUseDuplicateDetection());

      String[] connectorPairData = bridgeControl.getStaticConnectors();
      Assert.assertEquals(bridgeConfig.getStaticConnectors().get(0), connectorPairData[0]);

      Assert.assertTrue(bridgeControl.isStarted());
   }

   @Test
   public void testStartStop() throws Exception
   {
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
   public void testNotifications() throws Exception
   {
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      BridgeControl bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      server_0.getManagementService().addNotificationListener(notifListener);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      bridgeControl.stop();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(NotificationType.BRIDGE_STOPPED, notif.getType());
      Assert.assertEquals(bridgeControl.getName(), notif.getProperties()
                                                        .getSimpleStringProperty(new SimpleString("name"))
                                                        .toString());

      bridgeControl.start();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(NotificationType.BRIDGE_STARTED, notif.getType());
      Assert.assertEquals(bridgeControl.getName(), notif.getProperties()
                                                        .getSimpleStringProperty(new SimpleString("name"))
                                                        .toString());
   }

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

      CoreQueueConfiguration sourceQueueConfig = new CoreQueueConfiguration(RandomUtil.randomString(),
                                                                    RandomUtil.randomString(),
                                                                    null,
                                                                    false);
      CoreQueueConfiguration targetQueueConfig = new CoreQueueConfiguration(RandomUtil.randomString(),
                                                                    RandomUtil.randomString(),
                                                                    null,
                                                                    false);
      List<String> connectors = new ArrayList<String>();
      connectors.add(connectorConfig.getName());
      bridgeConfig = new BridgeConfiguration(RandomUtil.randomString(),
                                             sourceQueueConfig.getName(),
                                             targetQueueConfig.getAddress(),
                                             null,
                                             null,
                                             HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                             HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                             HornetQClient.DEFAULT_CONNECTION_TTL,
                                             RandomUtil.randomPositiveLong(),
                                             HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                             RandomUtil.randomDouble(),
                                             RandomUtil.randomPositiveInt(),
                                             RandomUtil.randomPositiveInt(),
                                             RandomUtil.randomPositiveInt(),
                                             RandomUtil.randomBoolean(),
                                             RandomUtil.randomPositiveInt(),
                                             connectors,
                                             false,
                                       HornetQDefaultConfiguration.getDefaultClusterUser(), CLUSTER_PASSWORD);

      Configuration conf_1 = createBasicConfig();
      conf_1.setSecurityEnabled(false);
      conf_1.setJMXManagementEnabled(true);
      conf_1.getAcceptorConfigurations().add(acceptorConfig);
      conf_1.getQueueConfigurations().add(targetQueueConfig);

      Configuration conf_0 = createBasicConfig();
      conf_0.setSecurityEnabled(false);
      conf_0.setJMXManagementEnabled(true);
      conf_0.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      conf_0.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      conf_0.getQueueConfigurations().add(sourceQueueConfig);
      conf_0.getBridgeConfigurations().add(bridgeConfig);

      server_1 = HornetQServers.newHornetQServer(conf_1, MBeanServerFactory.createMBeanServer(), false);
      addServer(server_1);
      server_1.start();

      server_0 = HornetQServers.newHornetQServer(conf_0, mbeanServer, false);
      addServer(server_0);
      server_0.start();
   }

   protected BridgeControl createBridgeControl(final String name, final MBeanServer mbeanServer1) throws Exception
   {
      return ManagementControlHelper.createBridgeControl(name, mbeanServer1);
   }
}
