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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Test;

public class AutoDeleteJmsDestinationTest extends JMSTestBase {

   @Override
   protected void extraServerConfig(ActiveMQServer server) {
      super.extraServerConfig(server);
      server.getConfiguration().setAddressQueueScanPeriod(100);
   }

   @Test
   public void testAutoDeleteQueue() throws Exception {
      Connection connection = cf.createConnection();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         javax.jms.Queue queue = ActiveMQJMSClient.createQueue("test");

         MessageProducer producer = session.createProducer(queue);

         final int numMessages = 100;

         for (int i = 0; i < numMessages; i++) {
            TextMessage mess = session.createTextMessage("msg" + i);
            producer.send(mess);
         }

         producer.close();

         final MessageConsumer messageConsumer = session.createConsumer(queue);
         connection.start();

         for (int i = 0; i < numMessages - 1; i++) {
            Message m = messageConsumer.receive(5000);
            assertNotNull(m);
         }

         Wait.waitFor(() -> {
            Binding binding = server.getPostOffice().getBinding(SimpleString.of("test"));
            if (binding == null) {
               return false;
            }
            return binding.getBindable() != null;
         });
         // ensure the queue is still there
         Queue q = (Queue) server.getPostOffice().getBinding(SimpleString.of("test")).getBindable();
         Wait.assertEquals(1, q::getMessageCount);
         assertEquals(numMessages, q.getMessagesAdded());

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageConsumer messageConsumer2 = session.createConsumer(queue);
         messageConsumer.close();
         Message m = messageConsumer2.receive(5000);
         assertNotNull(m);

         connection.close();

         SimpleString qname = SimpleString.of("test");
         Wait.waitFor(() -> server.getPostOffice().getBinding(qname) == null);

         // ensure the queue was removed
         assertNull(server.getPostOffice().getBinding(SimpleString.of("test")));

         // make sure the JMX control was removed for the JMS queue
         assertNull(server.getManagementService().getResource("test"));

         messageConsumer2.close();
      } finally {
         connection.close();
      }
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
         assertNotNull(m);
      }

      session.close();

      // ensure the queue is still there
      Queue q = (Queue) server.getPostOffice().getBinding(SimpleString.of("test")).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(numMessages, q.getMessagesAdded());

      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      messageConsumer = session.createConsumer(queue);
      Message m = messageConsumer.receive(5000);
      assertNotNull(m);

      connection.close();

      // ensure the queue was not removed
      assertNotNull(server.getPostOffice().getBinding(SimpleString.of("test")));
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
         assertNotNull(m);
      }

      connection.close();

      SimpleString qName = SimpleString.of("test");
      Wait.waitFor(() -> server.locateQueue(qName) == null);
      // ensure the topic was removed
      assertNull(server.locateQueue(qName));

      // make sure the JMX control was removed for the JMS topic
      assertNull(server.getManagementService().getResource("jtest"));
   }

   @Test
   public void testAutoDeleteTopicNegative() throws Exception {
      final int numMessages = 100;
      final SimpleString addressName = SimpleString.of("test");
      server.getAddressSettingsRepository().addMatch(addressName.toString(), new AddressSettings().setAutoDeleteAddresses(false));

      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic topic = session.createTopic(addressName.toString());
      MessageConsumer messageConsumer = session.createConsumer(topic);
      MessageProducer producer = session.createProducer(topic);

      for (int i = 0; i < numMessages; i++) {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(mess);
      }

      producer.close();
      // ensure the address was created
      assertNotNull(server.getAddressInfo(addressName));

      connection.start();

      for (int i = 0; i < numMessages; i++) {
         Message m = messageConsumer.receive(5000);
         assertNotNull(m);
      }

      connection.close();
      // ensure the topic was not removed
      assertFalse(Wait.waitFor(() -> server.getAddressInfo(addressName) == null, 2000, 100));
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
         assertNotNull(m);
      }

      messageConsumer.close();
      session.unsubscribe("mySub");

      connection.close();

      // ensure the topic was removed
      assertNull(server.locateQueue(SimpleString.of("test")));

      // make sure the JMX control was removed for the JMS topic
      assertNull(server.getManagementService().getResource("test"));
   }
}
