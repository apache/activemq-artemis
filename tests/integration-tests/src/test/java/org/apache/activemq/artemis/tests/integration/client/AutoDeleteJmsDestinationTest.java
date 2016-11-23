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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AutoDeleteJmsDestinationTest extends JMSTestBase {

   @Test
   public void testAutoDeleteQueue() throws Exception {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue("test");

      MessageProducer producer = session.createProducer(queue);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(mess);
      }

      producer.close();

      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < numMessages - 1; i++) {
         Message m = messageConsumer.receive(5000);
         Assert.assertNotNull(m);
      }

      session.close();

      // ensure the queue is still there
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString("test")).getBindable();
      Assert.assertEquals(1, q.getMessageCount());
      Assert.assertEquals(numMessages, q.getMessagesAdded());

      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      messageConsumer = session.createConsumer(queue);
      Message m = messageConsumer.receive(5000);
      Assert.assertNotNull(m);

      connection.close();

      // ensure the queue was removed
      Assert.assertNull(server.getPostOffice().getBinding(new SimpleString("test")));

      // make sure the JMX control was removed for the JMS queue
      assertNull(server.getManagementService().getResource("test"));
   }

   @Test
   public void testAutoDeleteNegative() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setAutoDeleteQueues(false));
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue("test");

      MessageProducer producer = session.createProducer(queue);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(mess);
      }

      producer.close();

      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < numMessages - 1; i++) {
         Message m = messageConsumer.receive(5000);
         Assert.assertNotNull(m);
      }

      session.close();

      // ensure the queue is still there
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString("test")).getBindable();
      Assert.assertEquals(1, q.getMessageCount());
      Assert.assertEquals(numMessages, q.getMessagesAdded());

      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      messageConsumer = session.createConsumer(queue);
      Message m = messageConsumer.receive(5000);
      Assert.assertNotNull(m);

      connection.close();

      // ensure the queue was not removed
      Assert.assertNotNull(server.getPostOffice().getBinding(new SimpleString("test")));
   }

   @Test
   public void testAutoDeleteTopic() throws Exception {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Topic topic = ActiveMQJMSClient.createTopic("test");

      MessageConsumer messageConsumer = session.createConsumer(topic);
      MessageProducer producer = session.createProducer(topic);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(mess);
      }

      producer.close();
      connection.start();

      for (int i = 0; i < numMessages; i++) {
         Message m = messageConsumer.receive(5000);
         Assert.assertNotNull(m);
      }

      connection.close();

      // ensure the topic was removed
      Assert.assertNull(server.locateQueue(new SimpleString("test")));

      // make sure the JMX control was removed for the JMS topic
      assertNull(server.getManagementService().getResource("jtest"));
   }

   @Test
   public void testAutoDeleteTopicDurableSubscriber() throws Exception {
      Connection connection = cf.createConnection();
      connection.setClientID("myClientID");
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Topic topic = ActiveMQJMSClient.createTopic("test");

      MessageConsumer messageConsumer = session.createDurableConsumer(topic, "mySub");
      MessageProducer producer = session.createProducer(topic);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(mess);
      }

      producer.close();
      connection.start();

      for (int i = 0; i < numMessages; i++) {
         Message m = messageConsumer.receive(5000);
         Assert.assertNotNull(m);
      }

      messageConsumer.close();
      session.unsubscribe("mySub");

      connection.close();

      // ensure the topic was removed
      Assert.assertNull(server.locateQueue(new SimpleString("test")));

      // make sure the JMX control was removed for the JMS topic
      assertNull(server.getManagementService().getResource("test"));
   }
}
