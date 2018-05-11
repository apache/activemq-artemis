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
package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.jms.client.ActiveMQTemporaryTopic;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.activemq.artemis.api.core.management.ResourceNames.ADDRESS;
import static org.apache.activemq.artemis.api.core.management.ResourceNames.QUEUE;

public class AutoCreateJmsDestinationTest extends JMSTestBase {

   public static final String QUEUE_NAME = "test";

   @Test
   public void testAutoCreateOnSendToQueue() throws Exception {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue(QUEUE_NAME);

      MessageProducer producer = session.createProducer(queue);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(mess);
      }

      producer.close();

      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < numMessages; i++) {
         Message m = messageConsumer.receive(5000);
         Assert.assertNotNull(m);
      }

      // make sure the JMX control was created for the address and queue
      assertNotNull(server.getManagementService().getResource(ADDRESS + QUEUE_NAME));
      assertNotNull(server.getManagementService().getResource(QUEUE + QUEUE_NAME));

      connection.close();
   }

   @Test
   public void testAutoCreateOnSendToQueueAnonymousProducer() throws Exception {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue(QUEUE_NAME);

      MessageProducer producer = session.createProducer(null);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(queue, mess);
      }

      producer.close();

      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < numMessages; i++) {
         Message m = messageConsumer.receive(5000);
         Assert.assertNotNull(m);
      }

      connection.close();
   }

   @Test
   public void testAutoCreateOnSendToQueueSecurity() throws Exception {
      ((ActiveMQJAASSecurityManager) server.getSecurityManager()).getConfiguration().addUser("guest", "guest");
      ((ActiveMQJAASSecurityManager) server.getSecurityManager()).getConfiguration().setDefaultUser("guest");
      ((ActiveMQJAASSecurityManager) server.getSecurityManager()).getConfiguration().addRole("guest", "rejectAll");
      Role role = new Role("rejectAll", false, false, false, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getSecurityRepository().addMatch("#", roles);
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue(QUEUE_NAME);

      try {
         session.createProducer(queue);
         Assert.fail("Sending a message here should throw a JMSSecurityException");
      } catch (Exception e) {
         Assert.assertTrue(e instanceof JMSSecurityException);
      }

      connection.close();
   }

   @Test
   public void testAutoCreateOnSendToTopic() throws Exception {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Topic topic = ActiveMQJMSClient.createTopic(QUEUE_NAME);

      MessageProducer producer = session.createProducer(topic);
      producer.send(session.createTextMessage("msg"));

      connection.close();

      assertNotNull(server.getManagementService().getResource(ResourceNames.ADDRESS + "test"));
   }

   @Test
   public void testAutoCreateOnConsumeFromQueue() throws Exception {
      Connection connection = null;
      connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue(QUEUE_NAME);

      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();

      Message m = messageConsumer.receive(500);
      Assert.assertNull(m);

      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(QUEUE_NAME)).getBindable();
      Assert.assertEquals(0, q.getMessageCount());
      Assert.assertEquals(0, q.getMessagesAdded());
      connection.close();
   }

   @Test
   public void testAutoCreateOnSubscribeToTopic() throws Exception {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final String topicName = "test-" + UUID.randomUUID().toString();

      javax.jms.Topic topic = ActiveMQJMSClient.createTopic(topicName);

      MessageConsumer consumer = session.createConsumer(topic);
      MessageProducer producer = session.createProducer(topic);
      producer.send(session.createTextMessage("msg"));
      connection.start();
      assertNotNull(consumer.receive(500));

      assertNotNull(server.getManagementService().getResource(ResourceNames.ADDRESS + topicName));

      connection.close();

      assertNull(server.getManagementService().getResource(ResourceNames.ADDRESS + topicName));
   }

   @Test
   public void testAutoCreateOnDurableSubscribeToTopic() throws Exception {
      Connection connection = cf.createConnection();
      connection.setClientID("myClientID");
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Topic topic = ActiveMQJMSClient.createTopic(QUEUE_NAME);

      MessageConsumer consumer = session.createDurableConsumer(topic, "myDurableSub");
      MessageProducer producer = session.createProducer(topic);
      producer.send(session.createTextMessage("msg"));
      connection.start();
      assertNotNull(consumer.receive(500));

      connection.close();

      assertNotNull(server.getManagementService().getResource(ResourceNames.ADDRESS + "test"));

      assertNotNull(server.locateQueue(SimpleString.toSimpleString("myClientID.myDurableSub")));
   }

   @Test
   public void testTemporaryTopic() throws Exception {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      //      javax.jms.Topic topic = ActiveMQJMSClient.createTopic(QUEUE_NAME);

      ActiveMQTemporaryTopic topic = (ActiveMQTemporaryTopic) session.createTemporaryTopic();

      MessageConsumer consumer = session.createConsumer(topic);
      MessageProducer producer = session.createProducer(topic);
      producer.send(session.createTextMessage("msg"));
      connection.start();
      assertNotNull(consumer.receive(500));

      SimpleString topicAddress = topic.getSimpleAddress();

      consumer.close();

      assertNotNull(server.locateQueue(topicAddress));

      IntegrationTestLogger.LOGGER.info("Topic name: " + topicAddress);

      topic.delete();

      connection.close();

      //      assertNotNull(server.getManagementService().getResource("jms.topic.test"));

      assertNull(server.locateQueue(topicAddress));
   }

   @Test
   public void testAutoCreateOnReconnect() throws Exception {
      Connection connection = cf.createConnection();
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue(QUEUE_NAME);

      MessageConsumer consumer = session.createConsumer(queue);
      MessageProducer producer = session.createProducer(queue);
      producer.send(session.createMessage());
      assertNotNull(consumer.receive(500));
      server.stop();
      server.start();
      waitForServerToStart(server);
      // wait for client to reconnect
      assertTrue(Wait.waitFor(() -> server.getTotalConsumerCount() == 1, 3000, 100));
      producer.send(session.createMessage());
      assertNotNull(consumer.receive(500));
      connection.close();
   }

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      ((ActiveMQJAASSecurityManager) server.getSecurityManager()).getConfiguration().addUser("guest", "guest");
      ((ActiveMQJAASSecurityManager) server.getSecurityManager()).getConfiguration().setDefaultUser("guest");
      ((ActiveMQJAASSecurityManager) server.getSecurityManager()).getConfiguration().addRole("guest", "allowAll");
      Role role = new Role("allowAll", true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getSecurityRepository().addMatch("#", roles);
   }

   @Override
   protected boolean useSecurity() {
      return true;
   }
}
