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
package org.apache.activemq.artemis.tests.integration.proton;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.proton.plug.AMQPClientConnectionContext;
import org.proton.plug.AMQPClientReceiverContext;
import org.proton.plug.AMQPClientSessionContext;
import org.proton.plug.test.Constants;
import org.proton.plug.test.minimalclient.SimpleAMQPConnector;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ProtonPubSubTest extends ProtonTestBase {
   private final String prefix = "foo.bar.";
   private final String pubAddress = "pubAddress";
   private final String prefixedPubAddress = prefix + "pubAddress";
   private final SimpleString ssPubAddress = new SimpleString(pubAddress);
   private final SimpleString ssprefixedPubAddress = new SimpleString(prefixedPubAddress);
   private Connection connection;
   private JmsConnectionFactory factory;

   @Override
   protected void configureAmqp(Map<String, Object> params) {
      params.put("pubSubPrefix", prefix);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server.createQueue(ssPubAddress, ssPubAddress, new SimpleString("foo=bar"), false, true);
      server.createQueue(ssprefixedPubAddress, ssprefixedPubAddress, new SimpleString("foo=bar"), false, true);
      factory = new JmsConnectionFactory("amqp://localhost:5672");
      factory.setClientID("myClientID");
      connection = factory.createConnection();
      connection.setExceptionListener(new ExceptionListener() {
         @Override
         public void onException(JMSException exception) {
            exception.printStackTrace();
         }
      });

   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         Thread.sleep(250);
         if (connection != null) {
            connection.close();
         }
      }
      finally {
         super.tearDown();
      }
   }

   @Test
   public void testNonDurablePubSub() throws Exception {
      int numMessages = 100;
      Topic topic = createTopic(pubAddress);
      TopicSession session = ((TopicConnection) connection).createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer sub = session.createSubscriber(topic);

      Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sendSession.createProducer(topic);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }
      for (int i = 0; i < numMessages; i++) {
         TextMessage receive = (TextMessage) sub.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
      }
   }

   @Test
   public void testNonDurableMultiplePubSub() throws Exception {
      int numMessages = 100;
      Topic topic = createTopic(pubAddress);
      TopicSession session = ((TopicConnection) connection).createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer sub = session.createSubscriber(topic);
      MessageConsumer sub2 = session.createSubscriber(topic);
      MessageConsumer sub3 = session.createSubscriber(topic);

      Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sendSession.createProducer(topic);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }
      for (int i = 0; i < numMessages; i++) {
         TextMessage receive = (TextMessage) sub.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
         receive = (TextMessage) sub2.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
         receive = (TextMessage) sub3.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
      }
   }


   @Test
   public void testDurablePubSub() throws Exception {
      int numMessages = 100;
      Topic topic = createTopic(pubAddress);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TopicSubscriber sub = session.createDurableSubscriber(topic, "myPubId");

      Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sendSession.createProducer(topic);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }
      for (int i = 0; i < numMessages; i++) {
         TextMessage receive = (TextMessage) sub.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
      }
   }

   @Test
   public void testDurableMultiplePubSub() throws Exception {
      int numMessages = 100;
      Topic topic = createTopic(pubAddress);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TopicSubscriber sub = session.createDurableSubscriber(topic, "myPubId");
      TopicSubscriber sub2 = session.createDurableSubscriber(topic, "myPubId2");
      TopicSubscriber sub3 = session.createDurableSubscriber(topic, "myPubId3");

      Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sendSession.createProducer(topic);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }
      for (int i = 0; i < numMessages; i++) {
         TextMessage receive = (TextMessage) sub.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
         receive = (TextMessage) sub2.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
         receive = (TextMessage) sub3.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
      }
   }

   @Test
   public void testDurablePubSubReconnect() throws Exception {
      int numMessages = 100;
      Topic topic = createTopic(pubAddress);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TopicSubscriber sub = session.createDurableSubscriber(topic, "myPubId");

      Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sendSession.createProducer(topic);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }
      for (int i = 0; i < numMessages; i++) {
         TextMessage receive = (TextMessage) sub.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
      }
      connection.close();
      connection = factory.createConnection();
      connection.setExceptionListener(new ExceptionListener() {
         @Override
         public void onException(JMSException exception) {
            exception.printStackTrace();
         }
      });
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      sub = session.createDurableSubscriber(topic, "myPubId");

      sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      producer = sendSession.createProducer(topic);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }
      for (int i = 0; i < numMessages; i++) {
         TextMessage receive = (TextMessage) sub.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
      }
   }

   @Test
   public void testDurablePubSubUnsubscribe() throws Exception {
      int numMessages = 100;
      Topic topic = createTopic(pubAddress);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TopicSubscriber sub = session.createDurableSubscriber(topic, "myPubId");

      Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sendSession.createProducer(topic);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }
      for (int i = 0; i < numMessages; i++) {
         TextMessage receive = (TextMessage) sub.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals(receive.getText(), "message:" + i);
      }
      sub.close();
      session.unsubscribe("myPubId");
   }


   @Test
   public void testPubSubWithSimpleClient() throws Exception {
      SimpleAMQPConnector connector = new SimpleAMQPConnector();
      connector.start();
      AMQPClientConnectionContext clientConnection = connector.connect("127.0.0.1", Constants.PORT);

      clientConnection.setContainer("myContainerID");

      clientConnection.clientOpen(null);

      AMQPClientSessionContext clientSession = clientConnection.createClientSession();
      AMQPClientReceiverContext receiver = clientSession.createReceiver(prefixedPubAddress);
      int numMessages = 100;
      Topic topic = createTopic(prefixedPubAddress);
      Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sendSession.createProducer(topic);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }

      receiver.flow(100);
      for (int i = 0; i < numMessages; i++) {
         ProtonJMessage protonJMessage = receiver.receiveMessage(5000, TimeUnit.MILLISECONDS);
         assertNotNull(protonJMessage);
         assertEquals(((AmqpValue) protonJMessage.getBody()).getValue(), "message:" + i);
      }

   }


   @Test
   public void testMultiplePubSubWithSimpleClient() throws Exception {
      SimpleAMQPConnector connector = new SimpleAMQPConnector();
      connector.start();
      AMQPClientConnectionContext clientConnection = connector.connect("127.0.0.1", Constants.PORT);

      clientConnection.setContainer("myContainerID");

      clientConnection.clientOpen(null);

      AMQPClientSessionContext clientSession = clientConnection.createClientSession();
      AMQPClientReceiverContext receiver = clientSession.createReceiver("sub1", prefixedPubAddress);
      AMQPClientReceiverContext receiver2 = clientSession.createReceiver("sub2", prefixedPubAddress);
      AMQPClientReceiverContext receiver3 = clientSession.createReceiver("sub3", prefixedPubAddress);
      int numMessages = 100;
      Topic topic = createTopic(prefixedPubAddress);
      Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sendSession.createProducer(topic);
      receiver.flow(100);
      receiver2.flow(100);
      receiver3.flow(100);
      connection.start();
      for (int i = 0; i < numMessages; i++) {
         producer.send(sendSession.createTextMessage("message:" + i));
      }

      for (int i = 0; i < numMessages; i++) {
         ProtonJMessage protonJMessage = receiver.receiveMessage(5000, TimeUnit.MILLISECONDS);
         assertNotNull("did not get message " + i, protonJMessage);
         assertEquals(((AmqpValue) protonJMessage.getBody()).getValue(), "message:" + i);
         protonJMessage = receiver2.receiveMessage(5000, TimeUnit.MILLISECONDS);
         assertNotNull("did not get message " + i, protonJMessage);
         assertEquals(((AmqpValue) protonJMessage.getBody()).getValue(), "message:" + i);
         protonJMessage = receiver3.receiveMessage(5000, TimeUnit.MILLISECONDS);
         assertNotNull("did not get message " + i, protonJMessage);
         assertEquals(((AmqpValue) protonJMessage.getBody()).getValue(), "message:" + i);
      }

   }


   private javax.jms.Topic createTopic(String address) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try {
         return session.createTopic(address);
      }
      finally {
         session.close();
      }
   }
}
