/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.smoke.jmx2;

import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.collect.ImmutableMap;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JmxServerControlTest extends SmokeTestBase {
   // This test will use a smoke created by the pom on this project (smoke-tsts)

   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT = 11099;

   public static final String SERVER_NAME_0 = "jmx2";

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      disableCheckThread();
      startServer(SERVER_NAME_0, 0, 30000);
   }

   @Test
   public void testListConsumers() throws Exception {
      // Without this, the RMI server would bind to the default interface IP (the user's local IP mostly)
      System.setProperty("java.rmi.server.hostname", JMX_SERVER_HOSTNAME);

      // I don't specify both ports here manually on purpose. See actual RMI registry connection port extraction below.
      String urlString = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT + "/jmxrmi";

      JMXServiceURL url = new JMXServiceURL(urlString);
      JMXConnector jmxConnector = null;

      try {
         jmxConnector = JMXConnectorFactory.connect(url);
         System.out.println("Successfully connected to: " + urlString);
      } catch (Exception e) {
         jmxConnector = null;
         e.printStackTrace();
         Assert.fail(e.getMessage());
      }

      try {
         MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
         String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
         ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
         ActiveMQServerControl activeMQServerControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);

         String addressName = "test_list_consumers_address";
         String queueName = "test_list_consumers_queue";
         activeMQServerControl.createAddress(addressName, RoutingType.ANYCAST.name());
         activeMQServerControl.createQueue(new QueueConfiguration(queueName).setAddress(addressName).setRoutingType(RoutingType.ANYCAST).toJSON());
         String uri = "tcp://localhost:61616";
         try (ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactory(uri, null)) {
            MessageConsumer consumer = cf.createConnection().createSession(true, Session.SESSION_TRANSACTED).createConsumer(new ActiveMQQueue(queueName));

            try {
               String options = JsonUtil.toJsonObject(ImmutableMap.of("field","queue", "operation", "EQUALS", "value", queueName)).toString();
               String consumersAsJsonString = activeMQServerControl.listConsumers(options, 1, 10);

               JsonObject consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
               JsonArray array = (JsonArray) consumersAsJsonObject.get("data");

               Assert.assertEquals("number of consumers returned from query", 1, array.size());
               JsonObject jsonConsumer = array.getJsonObject(0);
               Assert.assertEquals("queue name in consumer", queueName, jsonConsumer.getString("queue"));
               Assert.assertEquals("address name in consumer", addressName, jsonConsumer.getString("address"));
            } finally {
               consumer.close();
            }
         }
      } finally {
         jmxConnector.close();
      }
   }
}
