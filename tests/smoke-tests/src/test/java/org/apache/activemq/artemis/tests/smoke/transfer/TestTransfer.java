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

package org.apache.activemq.artemis.tests.smoke.transfer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTransfer extends SmokeTestBase {

   public static final String SERVER_NAME_0 = "transfer1";
   public static final String SERVER_NAME_1 = "transfer2";
   private static final int NUMBER_OF_MESSAGES = 200;
   private static final int PARTIAL_MESSAGES = 10;
   String sourceTransferProtocol = "amqp";
   String targetTransferProtocol = "amqp";
   String senderProtocol = "amqp";
   String consumerProtocol = "amqp";

   public TestTransfer(String sender, String consumer, String source, String target) {
      this.senderProtocol = sender;
      this.consumerProtocol = consumer;
      this.sourceTransferProtocol = source;
      this.targetTransferProtocol = target;
   }

   @Parameterized.Parameters(name = "sender={0}, consumer={1}, sourceOnTransfer={2}, targetOnTransfer={3}")
   public static Collection<Object[]> getParams() {

      String[] protocols = new String[]{"core", "amqp"};

      ArrayList<Object[]> parameters = new ArrayList<>();

      for (int i = 0; i < protocols.length; i++) {
         for (int j = 0; j < protocols.length; j++) {
            // sender and sourceOnTransfer have to be the same
            // consumer and targetOnTransfer have to be the same
            // this is because AMQP Shared Subscription will create a different queue than core
            String[] parameter = new String[]{protocols[i], protocols[j], protocols[i], protocols[j]};
            parameters.add(parameter);
         }
      }

      return parameters;
   }

   private static ConnectionFactory createConnectionFactory(String protocol, String uri) {
      if (protocol.toUpperCase().equals("AMQP")) {

         if (uri.startsWith("tcp://")) {
            // replacing tcp:// by amqp://
            uri = "amqp" + uri.substring(3);

         }
         return new JmsConnectionFactory(uri);
      } else if (protocol.toUpperCase().equals("CORE") || protocol.toUpperCase().equals("ARTEMIS")) {
         return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(uri);
      } else {
         throw new IllegalStateException("Unkown:" + protocol);
      }
   }

   private ConnectionFactory createConsumerCF() {
      return createConnectionFactory(consumerProtocol, "tcp://localhost:61716");
   }

   private ConnectionFactory createSenderCF() {
      return createConnectionFactory(senderProtocol, "tcp://localhost:61616");
   }

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      disableCheckThread();
      startServer(SERVER_NAME_0, 0, 30000);
      startServer(SERVER_NAME_1, 100, 30000);
   }

   @Test
   public void testTransferSimpleQueueCopy() throws Exception {
      internalTransferSimpleQueue(false);
   }

   @Test
   public void testTransferSimpleQueue() throws Exception {
      internalTransferSimpleQueue(true);
   }

   public String getQueueName() {
      return getName();
   }

   public String getTopicName() {
      return "Topic" + getName();
   }

   private void internalTransferSimpleQueue(boolean copy) throws Exception {
      ConnectionFactory factory = createSenderCF();
      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Queue queue = session.createQueue(getQueueName());
      MessageProducer producer = session.createProducer(queue);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         producer.send(session.createTextMessage("hello " + i));
      }

      session.commit();

      String[] argsArray = new String[]{"transfer", "--target-url", "tcp://localhost:61716", "--source-queue", getQueueName(), "--target-queue", getQueueName(), "--source-protocol", sourceTransferProtocol, "--target-protocol", targetTransferProtocol, "--receive-timeout", "0"};

      if (copy) {
         ArrayList<String> copyArgs = new ArrayList<>();
         for (String a : argsArray) {
            copyArgs.add(a);
         }
         if (copy) {
            copyArgs.add("--copy");
         }
         argsArray = copyArgs.toArray(new String[copyArgs.size()]);
      }

      Process transferProcess = ServerUtil.execute(getServerLocation(SERVER_NAME_0), "transfer", argsArray);
      transferProcess.waitFor();

      ConnectionFactory factoryTarget = createConsumerCF();
      Connection connectionTarget = factoryTarget.createConnection();
      connectionTarget.start();
      Session sessionTarget = connectionTarget.createSession(true, Session.SESSION_TRANSACTED);

      Queue queueTarget = sessionTarget.createQueue(getQueueName());
      MessageConsumer consumer = sessionTarget.createConsumer(queueTarget);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         TextMessage received = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(received);
         Assert.assertEquals("hello " + i, received.getText());
      }

      sessionTarget.commit();

      Assert.assertNull(consumer.receiveNoWait());

      MessageConsumer consumerSource = session.createConsumer(queue);
      connection.start();

      if (copy) {
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage received = (TextMessage) consumerSource.receive(1000);
            Assert.assertNotNull(received);
            Assert.assertEquals("hello " + i, received.getText());
         }
      }

      Assert.assertNull(consumerSource.receiveNoWait());

      connection.close();
      connectionTarget.close();
   }

   @Test
   public void testDurableSharedSubscrition() throws Exception {
      ConnectionFactory factory = createSenderCF();
      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Topic topic = session.createTopic(getTopicName());
      MessageConsumer subscription = session.createSharedDurableConsumer(topic, "testSubs");
      MessageProducer producer = session.createProducer(topic);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         producer.send(session.createTextMessage("hello " + i));
      }

      session.commit();
      subscription.close();

      Process transferProcess = ServerUtil.execute(getServerLocation(SERVER_NAME_0), "transfer", "transfer", "--target-url", "tcp://localhost:61716", "--source-topic", getTopicName(), "--shared-durable-subscription", "testSubs", "--target-queue", getQueueName(), "--source-protocol", sourceTransferProtocol, "--target-protocol", targetTransferProtocol, "--receive-timeout", "1000", "--verbose");
      transferProcess.waitFor();

      ConnectionFactory factoryTarget = createConsumerCF();
      Connection connectionTarget = factoryTarget.createConnection();
      connectionTarget.start();
      Session sessionTarget = connectionTarget.createSession(true, Session.SESSION_TRANSACTED);

      Queue queueTarget = sessionTarget.createQueue(getQueueName());
      MessageConsumer consumer = sessionTarget.createConsumer(queueTarget);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         TextMessage received = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(received);
      }

      sessionTarget.commit();

      Assert.assertNull(consumer.receiveNoWait());

      connection.close();
      connectionTarget.close();
   }

   @Test
   public void testSharedSubscrition() throws Exception {
      ConnectionFactory factory = createSenderCF();
      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Topic topic = session.createTopic(getTopicName());
      MessageConsumer subscription = session.createSharedConsumer(topic, "testSubs");
      MessageProducer producer = session.createProducer(topic);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      // I can't just send a few messages.. I have to send more messages that would be stuck on delivering mode
      // since the transfer will share the consumer with the shared consumer
      // and the temporary queue would be removed if i closed it earlier.
      for (int i = 0; i < 2000; i++) {
         producer.send(session.createTextMessage("hello " + i));
      }

      session.commit();

      Process transferProcess = ServerUtil.execute(getServerLocation(SERVER_NAME_0), "transfer", "transfer", "--target-url", "tcp://localhost:61716", "--source-topic", getTopicName(), "--shared-subscription", "testSubs", "--target-queue", getQueueName(), "--source-protocol", sourceTransferProtocol, "--target-protocol", targetTransferProtocol, "--receive-timeout", "0", "--verbose");
      transferProcess.waitFor();

      // this test is a bit tricky as the subscription would be removed when the consumer is gone...
      // I'm adding a test for completion only
      // and the subscription has to be closed only after the transfer,
      // which will not receive all the messages as some messages will be in delivering mode
      subscription.close();

      ConnectionFactory factoryTarget = createConsumerCF();
      Connection connectionTarget = factoryTarget.createConnection();
      connectionTarget.start();
      Session sessionTarget = connectionTarget.createSession(true, Session.SESSION_TRANSACTED);

      Queue queueTarget = sessionTarget.createQueue(getQueueName());
      MessageConsumer consumer = sessionTarget.createConsumer(queueTarget);

      // we are keeping a non durable subscription so the temporary queue still up
      // I'm not going to bother about being too strict about the content, just that some messages arrived
      for (int i = 0; i < PARTIAL_MESSAGES; i++) {
         TextMessage received = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(received);
      }

      sessionTarget.commit();

      connection.close();
      connectionTarget.close();
   }

   @Test
   public void testDurableConsumer() throws Exception {
      ConnectionFactory factory = createSenderCF();
      Connection connection = factory.createConnection();
      connection.setClientID("test");
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Topic topic = session.createTopic(getTopicName());
      MessageConsumer subscription = session.createDurableConsumer(topic, "testSubs");
      MessageProducer producer = session.createProducer(topic);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         producer.send(session.createTextMessage("hello " + i));
      }

      session.commit();
      connection.close();
      subscription.close();

      Process transferProcess = ServerUtil.execute(getServerLocation(SERVER_NAME_0), "transfer", "transfer", "--target-url", "tcp://localhost:61716", "--source-topic", getTopicName(), "--source-client-id", "test", "--durable-consumer", "testSubs", "--target-queue", getQueueName(), "--source-protocol", sourceTransferProtocol, "--target-protocol", targetTransferProtocol, "--receive-timeout", "1000", "--verbose", "--silent");
      transferProcess.waitFor();

      ConnectionFactory factoryTarget = createConsumerCF();
      Connection connectionTarget = factoryTarget.createConnection();
      connectionTarget.start();
      Session sessionTarget = connectionTarget.createSession(true, Session.SESSION_TRANSACTED);

      Queue queueTarget = sessionTarget.createQueue(getQueueName());
      MessageConsumer consumer = sessionTarget.createConsumer(queueTarget);

      // we are keeping a non durable subscription so the temporary queue still up
      // I'm not going to bother about being too strict about the content, just that some messages arrived
      for (int i = 0; i < PARTIAL_MESSAGES; i++) {
         TextMessage received = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(received);
      }

      sessionTarget.commit();

      connectionTarget.close();
   }

}
