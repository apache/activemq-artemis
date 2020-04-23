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
package org.apache.activemq.artemis.tests.integration.amqp;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.math.BigInteger;
import java.util.Map;
import java.util.Random;

//adapted from https://issues.apache.org/jira/browse/ARTEMIS-1416
public class QueueAutoCreationTest extends JMSClientTestSupport {

   Queue queue1;
   Random random = new Random();
   ActiveMQConnection testConn;
   ClientSession clientSession;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      String randomSuffix = new BigInteger(130, random).toString(32);
      testConn = (ActiveMQConnection)createCoreConnection();
      clientSession = testConn.getSessionFactory().createSession();
      queue1 = createAddressOnlyAndFakeQueue("queue1_" + randomSuffix);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      testConn.close();
      super.tearDown();
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE";
   }

   @Override
   protected void configureAddressPolicy(ActiveMQServer server) {
      Configuration serverConfig = server.getConfiguration();
      serverConfig.setJournalType(JournalType.NIO);
      Map<String, AddressSettings> map = serverConfig.getAddressesSettings();
      if (map.size() == 0) {
         AddressSettings as = new AddressSettings();
         map.put("#", as);
      }
      Map.Entry<String, AddressSettings> entry = map.entrySet().iterator().next();
      AddressSettings settings = entry.getValue();
      settings.setAutoCreateQueues(true);
      instanceLog.debug("server cofg, isauto? " + entry.getValue().isAutoCreateQueues());
   }


   protected Queue createAddressOnlyAndFakeQueue(final String queueName) throws Exception {
      SimpleString address = SimpleString.toSimpleString(queueName);
      clientSession.createAddress(address, RoutingType.ANYCAST, false);
      return new ActiveMQQueue(queueName);
   }

   @Test(timeout = 30000)
   public void testSmallString() throws Exception {
      sendStringOfSize(1024, false);
   }

   @Test(timeout = 30000)
   public void testHugeString() throws Exception {
      sendStringOfSize(1024 * 1024, false);
   }


   @Test(timeout = 30000)
   // QueueAutoCreationTest was created to validate auto-creation of queues
   // and this test was added to validate a regression: https://issues.apache.org/jira/browse/ARTEMIS-2238
   public void testAutoCreateOnTopic() throws Exception {
      ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:5672");
      Connection connection = factory.createConnection();
      SimpleString addressName = UUIDGenerator.getInstance().generateSimpleStringUUID();
      instanceLog.debug("Address is " + addressName);
      clientSession.createAddress(addressName, RoutingType.ANYCAST, false);
      Topic topic = new ActiveMQTopic(addressName.toString());
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(topic);
      for (int i = 0; i < 10; i++) {
         producer.send(session.createTextMessage("hello"));
      }

      Assert.assertTrue(((ActiveMQDestination) topic).isCreated());
   }

   @Test(timeout = 30000)
   // QueueAutoCreationTest was created to validate auto-creation of queues
   // and this test was added to validate a regression: https://issues.apache.org/jira/browse/ARTEMIS-2238
   public void testAutoCreateOnTopicManySends() throws Exception {
      ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:5672");
      Connection connection = factory.createConnection();
      SimpleString addressName = UUIDGenerator.getInstance().generateSimpleStringUUID();
      instanceLog.debug("Address is " + addressName);
      clientSession.createAddress(addressName, RoutingType.ANYCAST, false);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      for (int i = 0; i < 10; i++) {
         Topic topic = new ActiveMQTopic(addressName.toString());
         MessageProducer producer = session.createProducer(topic);
         producer.send(session.createTextMessage("hello"));
         producer.close();
         Assert.assertTrue(((ActiveMQDestination) topic).isCreated());
      }

   }

   @Test(timeout = 30000)
   // QueueAutoCreationTest was created to validate auto-creation of queues
   // and this test was added to validate a regression: https://issues.apache.org/jira/browse/ARTEMIS-2238
   public void testAutoCreateOnTopicAndConsume() throws Exception {
      ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:5672");
      Connection connection = factory.createConnection();
      SimpleString addressName = UUIDGenerator.getInstance().generateSimpleStringUUID();
      instanceLog.debug("Address is " + addressName);
      clientSession.createAddress(addressName, RoutingType.ANYCAST, false);

      Connection recConnection = factory.createConnection();
      Session recSession = recConnection.createSession(Session.AUTO_ACKNOWLEDGE);
      Topic topicConsumer = recSession.createTopic(addressName.toString());
      MessageConsumer consumer = recSession.createConsumer(topicConsumer);
      recConnection.start();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      for (int i = 0; i < 10; i++) {
         Topic topic = session.createTopic(addressName.toString());
         MessageProducer producer = session.createProducer(topic);
         producer.send(session.createTextMessage("hello"));
         producer.close();
         Assert.assertTrue(((ActiveMQDestination) topic).isCreated());
      }

      for (int i = 0; i < 10; i++) {
         TextMessage message = (TextMessage)consumer.receive(10_000);
         Assert.assertNotNull(message);
         Assert.assertEquals("hello", message.getText());
      }

      Assert.assertNull(consumer.receiveNoWait());

   }

   private void sendStringOfSize(int msgSize, boolean useCoreReceive) throws JMSException {

      Connection conn = this.createConnection();

      try {
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue1);

         TextMessage m = session.createTextMessage();

         m.setJMSDeliveryMode(DeliveryMode.PERSISTENT);

         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < msgSize) {
            buffer.append(UUIDGenerator.getInstance().generateStringUUID());
         }

         final String originalString = buffer.toString();

         m.setText(originalString);

         prod.send(m);

         conn.close();

         if (useCoreReceive) {
            conn = createCoreConnection();
         } else {
            conn = createConnection();
         }

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(queue1);

         conn.start();

         TextMessage rm = (TextMessage) cons.receive(5000);
         Assert.assertNotNull(rm);

         String str = rm.getText();
         Assert.assertEquals(originalString, str);
      } finally {
         if (conn != null) {
            conn.close();
         }
      }
   }
}