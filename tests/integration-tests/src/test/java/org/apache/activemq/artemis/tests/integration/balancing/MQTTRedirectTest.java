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

package org.apache.activemq.artemis.tests.integration.balancing;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.management.BrokerBalancerControl;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTReasonCodes;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.balancing.policies.FirstElementPolicy;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.integration.security.SecurityTest;
import org.apache.activemq.artemis.utils.Wait;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Assert;
import org.junit.Test;

public class MQTTRedirectTest extends BalancingTestBase {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = SecurityTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   @Test
   public void testSimpleRedirect() throws Exception {
      final String topicName = "RedirectTestTopic";

      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupLiveServerWithDiscovery(1, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, FirstElementPolicy.NAME, null, false, null, 1);

      startServers(0, 1);

      getServer(0).createQueue(new QueueConfiguration(topicName).setRoutingType(RoutingType.ANYCAST));
      getServer(1).createQueue(new QueueConfiguration(topicName).setRoutingType(RoutingType.ANYCAST));

      QueueControl queueControl0 = (QueueControl)getServer(0).getManagementService()
         .getResource(ResourceNames.QUEUE + topicName);
      QueueControl queueControl1 = (QueueControl)getServer(1).getManagementService()
         .getResource(ResourceNames.QUEUE + topicName);

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(0, queueControl1.countMessages());

      MqttConnectOptions connOpts = new MqttConnectOptions();
      connOpts.setCleanSession(true);
      connOpts.setUserName("admin");
      connOpts.setPassword("admin".toCharArray());

      MqttClient client0 = new MqttClient("tcp://" + TransportConstants.DEFAULT_HOST + ":" + TransportConstants.DEFAULT_PORT, "TEST", new MemoryPersistence());
      try {
         client0.connect(connOpts);
         Assert.fail();
      } catch (MqttException e) {
         Assert.assertEquals(MQTTReasonCodes.USE_ANOTHER_SERVER, (byte) e.getReasonCode());
      }
      client0.close();

      BrokerBalancerControl brokerBalancerControl = (BrokerBalancerControl)getServer(0).getManagementService()
         .getResource(ResourceNames.BROKER_BALANCER + BROKER_BALANCER_NAME);

      CompositeData targetData = brokerBalancerControl.getTarget("admin");
      CompositeData targetConnectorData = (CompositeData)targetData.get("connector");
      TabularData targetConnectorParams = (TabularData)targetConnectorData.get("params");
      CompositeData hostData = targetConnectorParams.get(new Object[]{TransportConstants.HOST_PROP_NAME});
      CompositeData portData = targetConnectorParams.get(new Object[]{TransportConstants.PORT_PROP_NAME});
      String host = hostData != null ? (String)hostData.get("value") : TransportConstants.DEFAULT_HOST;
      int port = portData != null ? Integer.parseInt((String)portData.get("value")) : TransportConstants.DEFAULT_PORT;

      CountDownLatch latch = new CountDownLatch(1);
      List<MqttMessage> messages = new ArrayList<>();

      MqttClient client1 = new MqttClient("tcp://" + host + ":" + port, "TEST", new MemoryPersistence());
      client1.connect(connOpts);

      Assert.assertEquals(0, queueControl0.countMessages());
      Assert.assertEquals(0, queueControl1.countMessages());

      client1.subscribe(topicName, (s, mqttMessage) -> {
         messages.add(mqttMessage);
         latch.countDown();
      });

      client1.publish(topicName, new MqttMessage("TEST".getBytes()));

      Assert.assertTrue(latch.await(3000, TimeUnit.MILLISECONDS));
      Assert.assertEquals("TEST", new String(messages.get(0).getPayload()));

      client1.disconnect();
      client1.close();

      Assert.assertEquals(0, queueControl0.countMessages());
      Wait.assertEquals(0, (Wait.LongCondition) queueControl1::countMessages);
   }

   @Test
   public void testRoleNameKeyLocalTarget() throws Exception {

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      servers[0] = addServer(ActiveMQServers.newActiveMQServer(createDefaultConfig(true).setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      setupBalancerServerWithLocalTarget(0, TargetKey.ROLE_NAME, "b", "b");

      startServers(0);

      MqttConnectOptions connOpts = new MqttConnectOptions();
      connOpts.setCleanSession(true);
      connOpts.setUserName("a");
      connOpts.setPassword("a".toCharArray());

      MqttClient client0 = new MqttClient("tcp://" + TransportConstants.DEFAULT_HOST + ":" + TransportConstants.DEFAULT_PORT, "TEST", new MemoryPersistence());
      try {
         client0.connect(connOpts);
         fail("Expect to be rejected as not in role b");
      } catch (MqttException e) {
         Assert.assertEquals(MQTTReasonCodes.USE_ANOTHER_SERVER, (byte) e.getReasonCode());
      }
      client0.close();

      MqttClient client1 = new MqttClient("tcp://" + TransportConstants.DEFAULT_HOST + ":" + TransportConstants.DEFAULT_PORT, "TEST", new MemoryPersistence());
      connOpts.setUserName("b");
      connOpts.setPassword("b".toCharArray());

      // expect to be accepted, b has role b
      client1.connect(connOpts);
      client1.disconnect();
      client1.close();
   }
}

