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
package org.apache.activemq.artemis.tests.integration.jms.server.management;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.integration.management.ManagementTestBase;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

import static org.apache.activemq.artemis.tests.util.RandomUtil.randomString;

public class TopicControlUsingJMSTest extends ManagementTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private JMSServerManagerImpl serverManager;

   private String clientID;

   private String subscriptionName;

   protected ActiveMQTopic topic;

   protected JMSMessagingProxy proxy;

   private QueueConnection connection;

   private QueueSession session;

   private final String topicBinding = "/topic/" + randomString();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testGetAttributes() throws Exception {
      Assert.assertEquals(topic.getTopicName(), proxy.retrieveAttributeValue("name"));
      Assert.assertEquals(topic.getAddress(), proxy.retrieveAttributeValue("address"));
      Assert.assertEquals(topic.isTemporary(), proxy.retrieveAttributeValue("temporary"));
      Object[] bindings = (Object[]) proxy.retrieveAttributeValue("" + "RegistryBindings");
      assertEquals(1, bindings.length);
      Assert.assertEquals(topicBinding, bindings[0]);
   }

   @Test
   public void testGetXXXSubscriptionsCount() throws Exception {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      // 1 non-durable subscriber, 2 durable subscribers
      JMSUtil.createConsumer(connection_1, topic);

      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      Assert.assertEquals(3, proxy.retrieveAttributeValue("subscriptionCount"));
      Assert.assertEquals(1, proxy.retrieveAttributeValue("nonDurableSubscriptionCount"));
      Assert.assertEquals(2, proxy.retrieveAttributeValue("durableSubscriptionCount"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testGetXXXMessagesCount() throws Exception {
      // 1 non-durable subscriber, 2 durable subscribers
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      Assert.assertEquals(0, proxy.retrieveAttributeValue("messageCount"));
      Assert.assertEquals(0, proxy.retrieveAttributeValue("nonDurableMessageCount"));
      Assert.assertEquals(0, proxy.retrieveAttributeValue("durableMessageCount"));

      JMSUtil.sendMessages(topic, 2);

      Assert.assertEquals(3 * 2, proxy.retrieveAttributeValue("messageCount"));
      Assert.assertEquals(1 * 2, proxy.retrieveAttributeValue("nonDurableMessageCount"));
      Assert.assertEquals(2 * 2, proxy.retrieveAttributeValue("durableMessageCount"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testListXXXSubscriptionsCount() throws Exception {
      // 1 non-durable subscriber, 2 durable subscribers
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      Assert.assertEquals(3, ((Object[]) proxy.invokeOperation("listAllSubscriptions")).length);
      Assert.assertEquals(1, ((Object[]) proxy.invokeOperation("listNonDurableSubscriptions")).length);
      Assert.assertEquals(2, ((Object[]) proxy.invokeOperation("listDurableSubscriptions")).length);

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testCountMessagesForSubscription() throws Exception {
      String key = "key";
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      JMSUtil.sendMessageWithProperty(session, topic, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, topic, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, topic, key, matchingValue);

      Assert.assertEquals(3, proxy.retrieveAttributeValue("messageCount"));

      Assert.assertEquals(2, proxy.invokeOperation("countMessagesForSubscription", clientID, subscriptionName, key + " =" +
                             matchingValue));
      Assert.assertEquals(1, proxy.invokeOperation("countMessagesForSubscription", clientID, subscriptionName, key + " =" +
                             unmatchingValue));

      connection.close();
   }

   @Test
   public void testCountMessagesForUnknownSubscription() throws Exception {
      String unknownSubscription = RandomUtil.randomString();

      try {
         proxy.invokeOperation("countMessagesForSubscription", clientID, unknownSubscription, null);
         Assert.fail();
      }
      catch (Exception e) {
      }
   }

   @Test
   public void testCountMessagesForUnknownClientID() throws Exception {
      String unknownClientID = RandomUtil.randomString();

      try {
         proxy.invokeOperation("countMessagesForSubscription", unknownClientID, subscriptionName, null);
         Assert.fail();
      }
      catch (Exception e) {
      }
   }

   @Test
   public void testDropDurableSubscriptionWithExistingSubscription() throws Exception {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      Assert.assertEquals(1, proxy.retrieveAttributeValue("durableSubscriptionCount"));

      connection.close();

      proxy.invokeOperation("dropDurableSubscription", clientID, subscriptionName);

      Assert.assertEquals(0, proxy.retrieveAttributeValue("durableSubscriptionCount"));
   }

   @Test
   public void testDropDurableSubscriptionWithUnknownSubscription() throws Exception {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      Assert.assertEquals(1, proxy.retrieveAttributeValue("durableSubscriptionCount"));

      try {
         proxy.invokeOperation("dropDurableSubscription", clientID, "this subscription does not exist");
         Assert.fail("should throw an exception");
      }
      catch (Exception e) {

      }

      Assert.assertEquals(1, proxy.retrieveAttributeValue("durableSubscriptionCount"));

      connection.close();
   }

   @Test
   public void testDropAllSubscriptions() throws Exception {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      TopicSubscriber durableSubscriber_1 = JMSUtil.createDurableSubscriber(connection_1, topic, clientID, subscriptionName);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      TopicSubscriber durableSubscriber_2 = JMSUtil.createDurableSubscriber(connection_2, topic, clientID + "2", subscriptionName + "2");

      Assert.assertEquals(2, proxy.retrieveAttributeValue("subscriptionCount"));

      durableSubscriber_1.close();
      durableSubscriber_2.close();

      Assert.assertEquals(2, proxy.retrieveAttributeValue("subscriptionCount"));
      proxy.invokeOperation("dropAllSubscriptions");

      Assert.assertEquals(0, proxy.retrieveAttributeValue("subscriptionCount"));

      connection_1.close();
      connection_2.close();
   }

   @Test
   public void testRemoveAllMessages() throws Exception {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_1, topic, clientID, subscriptionName);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID + "2", subscriptionName + "2");

      JMSUtil.sendMessages(topic, 3);

      Assert.assertEquals(3 * 2, proxy.retrieveAttributeValue("messageCount"));

      int removedCount = (Integer) proxy.invokeOperation("removeMessages", "");
      Assert.assertEquals(3 * 2, removedCount);
      Assert.assertEquals(0, proxy.retrieveAttributeValue("messageCount"));

      connection_1.close();
      connection_2.close();
   }

   @Test
   public void testListMessagesForSubscription() throws Exception {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      JMSUtil.sendMessages(topic, 3);

      Object[] data = (Object[]) proxy.invokeOperation("listMessagesForSubscription", ActiveMQDestination.createQueueNameForDurableSubscription(true, clientID, subscriptionName));
      Assert.assertEquals(3, data.length);

      connection.close();
   }

   @Test
   public void testListMessagesForSubscriptionWithUnknownClientID() throws Exception {
      String unknownClientID = RandomUtil.randomString();

      try {
         proxy.invokeOperation("listMessagesForSubscription", ActiveMQDestination.createQueueNameForDurableSubscription(true, unknownClientID, subscriptionName));
         Assert.fail();
      }
      catch (Exception e) {
      }
   }

   @Test
   public void testListMessagesForSubscriptionWithUnknownSubscription() throws Exception {
      String unknownSubscription = RandomUtil.randomString();

      try {
         proxy.invokeOperation("listMessagesForSubscription", ActiveMQDestination.createQueueNameForDurableSubscription(true, clientID, unknownSubscription));
         Assert.fail();
      }
      catch (Exception e) {
      }
   }

   @Test
   public void testGetMessagesAdded() throws Exception {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      assertEquals(0, proxy.retrieveAttributeValue("messagesAdded"));

      JMSUtil.sendMessages(topic, 2);

      assertEquals(3 * 2, proxy.retrieveAttributeValue("messagesAdded"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testGetMessagesDelivering() throws Exception {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer cons_1 = JMSUtil.createConsumer(connection_1, topic, Session.CLIENT_ACKNOWLEDGE);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer cons_2 = JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName, Session.CLIENT_ACKNOWLEDGE);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer cons_3 = JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2", Session.CLIENT_ACKNOWLEDGE);

      assertEquals(0, proxy.retrieveAttributeValue("deliveringCount"));

      JMSUtil.sendMessages(topic, 2);

      assertEquals(0, proxy.retrieveAttributeValue("deliveringCount"));

      connection_1.start();
      connection_2.start();
      connection_3.start();

      Message msg_1 = null;
      Message msg_2 = null;
      Message msg_3 = null;
      for (int i = 0; i < 2; i++) {
         msg_1 = cons_1.receive(5000);
         assertNotNull(msg_1);
         msg_2 = cons_2.receive(5000);
         assertNotNull(msg_2);
         msg_3 = cons_3.receive(5000);
         assertNotNull(msg_3);
      }

      assertEquals(3 * 2, proxy.retrieveAttributeValue("deliveringCount"));

      msg_1.acknowledge();
      assertEquals(2 * 2, proxy.retrieveAttributeValue("deliveringCount"));
      msg_2.acknowledge();
      assertEquals(1 * 2, proxy.retrieveAttributeValue("deliveringCount"));
      msg_3.acknowledge();
      assertEquals(0, proxy.retrieveAttributeValue("deliveringCount"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Configuration config = createDefaultInVMConfig().setJMXManagementEnabled(true);
      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, false));
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      serverManager.start();
      serverManager.setRegistry(new JndiBindingRegistry(new InVMNamingContext()));
      serverManager.activated();

      clientID = RandomUtil.randomString();
      subscriptionName = RandomUtil.randomString();

      String topicName = RandomUtil.randomString();
      serverManager.createTopic(false, topicName, topicBinding);
      topic = (ActiveMQTopic) ActiveMQJMSClient.createTopic(topicName);

      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      ActiveMQQueue managementQueue = (ActiveMQQueue) ActiveMQJMSClient.createQueue("activemq.management");
      proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_TOPIC + topic.getTopicName());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
