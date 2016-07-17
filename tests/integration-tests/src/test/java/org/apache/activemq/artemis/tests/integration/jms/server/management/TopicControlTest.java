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

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.api.jms.management.SubscriptionInfo;
import org.apache.activemq.artemis.api.jms.management.TopicControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.jms.server.management.JMSNotificationType;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.integration.management.ManagementTestBase;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;
import javax.json.JsonArray;
import javax.management.Notification;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TopicControlTest extends ManagementTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private JMSServerManagerImpl serverManager;

   private String clientID;

   private String subscriptionName;

   protected ActiveMQTopic topic;

   private String topicBinding = "/topic/" + RandomUtil.randomString();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testGetAttributes() throws Exception {
      TopicControl topicControl = createManagementControl();

      Assert.assertEquals(topic.getTopicName(), topicControl.getName());
      Assert.assertEquals(topic.getAddress(), topicControl.getAddress());
      Assert.assertEquals(topic.isTemporary(), topicControl.isTemporary());
      Object[] bindings = topicControl.getRegistryBindings();
      Assert.assertEquals(1, bindings.length);
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

      TopicControl topicControl = createManagementControl();
      Assert.assertEquals(3, topicControl.getSubscriptionCount());
      Assert.assertEquals(1, topicControl.getNonDurableSubscriptionCount());
      Assert.assertEquals(2, topicControl.getDurableSubscriptionCount());

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
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "_2", subscriptionName + "2");

      TopicControl topicControl = createManagementControl();

      Assert.assertEquals(0, topicControl.getMessageCount());
      Assert.assertEquals(0, topicControl.getNonDurableMessageCount());
      Assert.assertEquals(0, topicControl.getDurableMessageCount());

      JMSUtil.sendMessages(topic, 2);

      waitForMessageCount(3 * 2, topicControl, 3000);
      waitForNonDurableMessageCount(1 * 2, topicControl, 3000);
      waitForDurableMessageCount(2 * 2, topicControl, 3000);

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testListXXXSubscriptionsCount() throws Exception {
      // 1 non-durable subscriber, 2 durable subscribers
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer cons = JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      TopicSubscriber subs1 = JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      TopicSubscriber subs2 = JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      TopicControl topicControl = createManagementControl();
      Assert.assertEquals(3, topicControl.listAllSubscriptions().length);
      Assert.assertEquals(1, topicControl.listNonDurableSubscriptions().length);
      Assert.assertEquals(2, topicControl.listDurableSubscriptions().length);

      String json = topicControl.listAllSubscriptionsAsJSON();
      System.out.println("Json: " + json);
      JsonArray jsonArray = JsonUtil.readJsonArray(json);

      Assert.assertEquals(3, jsonArray.size());

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testListXXXSubscriptionsAsJSON() throws Exception {
      // 1 non-durable subscriber, 2 durable subscribers
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      TopicControl topicControl = createManagementControl();
      String jsonString = topicControl.listDurableSubscriptionsAsJSON();
      SubscriptionInfo[] infos = SubscriptionInfo.from(jsonString);
      Assert.assertEquals(2, infos.length);
      List<String> expectedClientIds = Arrays.asList(clientID, clientID + "2");
      List<String> expectedSubscriptionNames = Arrays.asList(subscriptionName, subscriptionName + "2");

      Assert.assertTrue(expectedClientIds.contains(infos[0].getClientID()));
      Assert.assertTrue(expectedSubscriptionNames.contains(infos[0].getName()));

      Assert.assertTrue(expectedClientIds.contains(infos[1].getClientID()));
      Assert.assertTrue(expectedSubscriptionNames.contains(infos[1].getName()));

      jsonString = topicControl.listNonDurableSubscriptionsAsJSON();
      infos = SubscriptionInfo.from(jsonString);
      Assert.assertEquals(1, infos.length);
      Assert.assertEquals(null, infos[0].getClientID());
      Assert.assertEquals(null, infos[0].getName());

      jsonString = topicControl.listAllSubscriptionsAsJSON();
      infos = SubscriptionInfo.from(jsonString);
      Assert.assertEquals(3, infos.length);

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testListXXXSubscriptionsAsJSONJMS2() throws Exception {
      ConnectionFactory cf = JMSUtil.createFactory(InVMConnectorFactory.class.getName(), ActiveMQClient.DEFAULT_CONNECTION_TTL_INVM, ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD_INVM);
      JMSContext jmsContext1 = cf.createContext();
      jmsContext1.createSharedDurableConsumer(topic, subscriptionName, null);
      JMSContext jmsContext2 = cf.createContext();
      jmsContext2.createSharedDurableConsumer(topic, subscriptionName + "2", null);
      JMSContext jmsContext3 = cf.createContext();
      jmsContext3.createConsumer(topic);

      TopicControl topicControl = createManagementControl();
      String jsonString = topicControl.listDurableSubscriptionsAsJSON();
      SubscriptionInfo[] infos = SubscriptionInfo.from(jsonString);
      Assert.assertEquals(2, infos.length);

      Assert.assertNull(infos[0].getClientID());
      Assert.assertTrue(infos[0].getName().equals(subscriptionName));

      Assert.assertNull(infos[1].getClientID());
      Assert.assertTrue(infos[1].getName().equals(subscriptionName + "2"));

      jsonString = topicControl.listNonDurableSubscriptionsAsJSON();
      infos = SubscriptionInfo.from(jsonString);
      Assert.assertEquals(1, infos.length);
      Assert.assertNull(infos[0].getClientID());
      Assert.assertNull(infos[0].getName());

      jsonString = topicControl.listAllSubscriptionsAsJSON();
      infos = SubscriptionInfo.from(jsonString);
      Assert.assertEquals(3, infos.length);

      jmsContext1.close();
      jmsContext2.close();
      jmsContext3.close();
   }

   @Test
   public void testListSubscriptionsAsJSONWithHierarchicalTopics() throws Exception {
      serverManager.createTopic(false, "my.jms.#", "jms/all");
      serverManager.createTopic(false, "my.jms.A", "jms/A");
      ActiveMQTopic myTopic = (ActiveMQTopic) ActiveMQJMSClient.createTopic("my.jms.A");

      TopicControl topicControl = ManagementControlHelper.createTopicControl(myTopic, mbeanServer);
      String jsonString = topicControl.listDurableSubscriptionsAsJSON();
      SubscriptionInfo[] infos = SubscriptionInfo.from(jsonString);
      Assert.assertEquals(1, infos.length);
      Assert.assertEquals("ActiveMQ", infos[0].getClientID());
      Assert.assertEquals("ActiveMQ", infos[0].getName());
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

      for (Binding binding : server.getPostOffice().getBindingsForAddress(topic.getSimpleAddress()).getBindings()) {
         ((LocalQueueBinding) binding).getQueue().flushExecutor();
      }

      TopicControl topicControl = createManagementControl();

      Assert.assertEquals(3, topicControl.getMessageCount());

      Assert.assertEquals(2, topicControl.countMessagesForSubscription(clientID, subscriptionName, key + " =" +
         matchingValue));
      Assert.assertEquals(1, topicControl.countMessagesForSubscription(clientID, subscriptionName, key + " =" +
         unmatchingValue));

      connection.close();
   }

   @Test
   public void testCountMessagesForUnknownSubscription() throws Exception {
      String unknownSubscription = RandomUtil.randomString();

      TopicControl topicControl = createManagementControl();

      try {
         topicControl.countMessagesForSubscription(clientID, unknownSubscription, null);
         Assert.fail();
      }
      catch (Exception e) {
      }
   }

   @Test
   public void testCountMessagesForUnknownClientID() throws Exception {
      String unknownClientID = RandomUtil.randomString();

      TopicControl topicControl = createManagementControl();

      try {
         topicControl.countMessagesForSubscription(unknownClientID, subscriptionName, null);
         Assert.fail();
      }
      catch (Exception e) {
      }
   }

   @Test
   public void testDropDurableSubscriptionWithExistingSubscription() throws Exception {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      TopicControl topicControl = createManagementControl();
      Assert.assertEquals(1, topicControl.getDurableSubscriptionCount());

      connection.close();

      topicControl.dropDurableSubscription(clientID, subscriptionName);

      Assert.assertEquals(0, topicControl.getDurableSubscriptionCount());
   }

   @Test
   public void testDropDurableSubscriptionWithUnknownSubscription() throws Exception {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      TopicControl topicControl = createManagementControl();
      Assert.assertEquals(1, topicControl.getDurableSubscriptionCount());

      try {
         topicControl.dropDurableSubscription(clientID, "this subscription does not exist");
         Assert.fail("should throw an exception");
      }
      catch (Exception e) {

      }

      Assert.assertEquals(1, topicControl.getDurableSubscriptionCount());

      connection.close();
   }

   @Test
   public void testDropAllSubscriptions() throws Exception {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection_1.setClientID(clientID);
      Session sess1 = connection_1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TopicSubscriber durableSubscriber_1 = sess1.createDurableSubscriber(topic, subscriptionName);

      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection_2.setClientID(clientID + "2");
      Session sess2 = connection_1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TopicSubscriber durableSubscriber_2 = sess2.createDurableSubscriber(topic, subscriptionName + "2");

      connection_1.start();
      connection_2.start();

      Session sess = connection_1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(topic);

      TextMessage msg1 = sess.createTextMessage("tst1");
      prod.send(msg1);

      Assert.assertNotNull(durableSubscriber_1.receive(5000));
      Assert.assertNotNull(durableSubscriber_2.receive(5000));

      connection_1.close();
      connection_2.close();

      TopicControl topicControl = createManagementControl();

      Assert.assertEquals(2, topicControl.getSubscriptionCount());
      topicControl.dropAllSubscriptions();

      Assert.assertEquals(0, topicControl.getSubscriptionCount());

      connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection_1.setClientID(clientID);
      sess = connection_1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      prod = sess.createProducer(topic);
      TextMessage msg2 = sess.createTextMessage("tst2");
      prod.send(msg2);

   }

   @Test
   public void testRemoveAllMessages() throws Exception {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_1, topic, clientID, subscriptionName);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID + "2", subscriptionName + "2");

      JMSUtil.sendMessages(topic, 3);

      TopicControl topicControl = createManagementControl();
      waitForMessageCount(3 * 2, topicControl, 3000);

      int removedCount = topicControl.removeMessages(null);
      Assert.assertEquals(3 * 2, removedCount);
      Assert.assertEquals(0, topicControl.getMessageCount());

      connection_1.close();
      connection_2.close();
   }

   @Test
   public void testListMessagesForSubscription() throws Exception {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      JMSUtil.sendMessages(topic, 3);

      TopicControl topicControl = createManagementControl();
      Map<String, Object>[] messages = topicControl.listMessagesForSubscription(ActiveMQDestination.createQueueNameForDurableSubscription(true, clientID, subscriptionName));
      Assert.assertEquals(3, messages.length);

      connection.close();
   }

   @Test
   public void testListMessagesForSubscriptionAsJSON() throws Exception {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      String[] ids = JMSUtil.sendMessages(topic, 3);

      TopicControl topicControl = createManagementControl();
      String jsonString = topicControl.listMessagesForSubscriptionAsJSON(ActiveMQDestination.createQueueNameForDurableSubscription(true, clientID, subscriptionName));
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(3, array.size());
      for (int i = 0; i < 3; i++) {
         Assert.assertEquals(ids[i], array.getJsonObject(i).getString("JMSMessageID"));
      }

      connection.close();
   }

   @Test
   public void testListMessagesForSubscriptionWithUnknownClientID() throws Exception {
      String unknownClientID = RandomUtil.randomString();

      TopicControl topicControl = createManagementControl();

      try {
         topicControl.listMessagesForSubscription(ActiveMQDestination.createQueueNameForDurableSubscription(true, unknownClientID, subscriptionName));
         Assert.fail();
      }
      catch (Exception e) {
      }
   }

   @Test
   public void testListMessagesForSubscriptionWithUnknownSubscription() throws Exception {
      String unknownSubscription = RandomUtil.randomString();

      TopicControl topicControl = createManagementControl();

      try {
         topicControl.listMessagesForSubscription(ActiveMQDestination.createQueueNameForDurableSubscription(true, clientID, unknownSubscription));
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

      TopicControl topicControl = createManagementControl();

      Assert.assertEquals(0, topicControl.getMessagesAdded());

      JMSUtil.sendMessages(topic, 2);

      waitForAddedMessageCount(3 * 2, topicControl, 3000);

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

      TopicControl topicControl = createManagementControl();

      Assert.assertEquals(0, topicControl.getDeliveringCount());

      JMSUtil.sendMessages(topic, 2);

      Assert.assertEquals(0, topicControl.getDeliveringCount());

      connection_1.start();
      connection_2.start();
      connection_3.start();

      Message msg_1 = null;
      Message msg_2 = null;
      Message msg_3 = null;
      for (int i = 0; i < 2; i++) {
         msg_1 = cons_1.receive(5000);
         Assert.assertNotNull(msg_1);
         msg_2 = cons_2.receive(5000);
         Assert.assertNotNull(msg_2);
         msg_3 = cons_3.receive(5000);
         Assert.assertNotNull(msg_3);
      }

      Assert.assertEquals(3 * 2, topicControl.getDeliveringCount());

      msg_1.acknowledge();
      Assert.assertEquals(2 * 2, topicControl.getDeliveringCount());
      msg_2.acknowledge();
      Assert.assertEquals(1 * 2, topicControl.getDeliveringCount());
      msg_3.acknowledge();
      Assert.assertEquals(0, topicControl.getDeliveringCount());

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   //make sure notifications are always received no matter whether
   //a Topic is created via JMSServerControl or by JMSServerManager directly.
   @Test
   public void testCreateTopicNotification() throws Exception {
      JMSUtil.JMXListener listener = new JMSUtil.JMXListener();
      this.mbeanServer.addNotificationListener(ObjectNameBuilder.DEFAULT.getJMSServerObjectName(), listener, null, null);

      List<String> connectors = new ArrayList<>();
      connectors.add("invm");

      String testTopicName = "newTopic";
      serverManager.createTopic(true, testTopicName, testTopicName);

      Notification notif = listener.getNotification();

      Assert.assertEquals(JMSNotificationType.TOPIC_CREATED.toString(), notif.getType());
      Assert.assertEquals(testTopicName, notif.getMessage());

      this.serverManager.destroyTopic(testTopicName);

      notif = listener.getNotification();
      Assert.assertEquals(JMSNotificationType.TOPIC_DESTROYED.toString(), notif.getType());
      Assert.assertEquals(testTopicName, notif.getMessage());

      JMSServerControl control = ManagementControlHelper.createJMSServerControl(mbeanServer);

      control.createTopic(testTopicName);

      notif = listener.getNotification();
      Assert.assertEquals(JMSNotificationType.TOPIC_CREATED.toString(), notif.getType());
      Assert.assertEquals(testTopicName, notif.getMessage());

      control.destroyTopic(testTopicName);

      notif = listener.getNotification();
      Assert.assertEquals(JMSNotificationType.TOPIC_DESTROYED.toString(), notif.getType());
      Assert.assertEquals(testTopicName, notif.getMessage());
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
   }

   protected TopicControl createManagementControl() throws Exception {
      return ManagementControlHelper.createTopicControl(topic, mbeanServer);
   }

   // Private -------------------------------------------------------

   private void waitForMessageCount(long expected, TopicControl topicControl, long timeout) throws Exception {
      long timeToWait = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < timeToWait) {
         if (expected == topicControl.getMessageCount()) {
            return;
         }
         Thread.sleep(100);
      }
      assertEquals(expected, topicControl.getMessageCount());
   }

   private void waitForNonDurableMessageCount(long expected, TopicControl topicControl, long timeout) throws Exception {
      long timeToWait = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < timeToWait) {
         if (expected == topicControl.getNonDurableMessageCount()) {
            return;
         }
         Thread.sleep(100);
      }
      assertEquals(expected, topicControl.getNonDurableMessageCount());
   }

   private void waitForDurableMessageCount(long expected, TopicControl topicControl, long timeout) throws Exception {
      long timeToWait = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < timeToWait) {
         if (expected == topicControl.getDurableMessageCount()) {
            return;
         }
         Thread.sleep(100);
      }
      assertEquals(expected, topicControl.getDurableMessageCount());
   }

   private void waitForAddedMessageCount(long expected, TopicControl topicControl, long timeout) throws Exception {
      long timeToWait = System.currentTimeMillis() + timeout;

      while (System.currentTimeMillis() < timeToWait) {
         if (expected == topicControl.getMessagesAdded()) {
            return;
         }
         Thread.sleep(100);
      }
      assertEquals(expected, topicControl.getMessagesAdded());
   }

   // Inner classes -------------------------------------------------

}
