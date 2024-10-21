/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.smoke.transfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TransferTest extends SmokeTestBase {

   public static final String SERVER_NAME_0 = "transfer1";
   public static final String SERVER_NAME_1 = "transfer2";
   private static final int NUMBER_OF_MESSAGES = 200;
   private static final int PARTIAL_MESSAGES = 10;
   String sourceTransferProtocol = "amqp";
   String targetTransferProtocol = "amqp";
   String senderProtocol = "amqp";
   String consumerProtocol = "amqp";

   /*
                  <execution>
                  <phase>test-compile</phase>
                  <id>create-transfer-1</id>
                  <goals>
                     <goal>create</goal>
                  </goals>
                  <configuration>
                     <!-- this makes it easier in certain envs -->
                     <allowAnonymous>true</allowAnonymous>
                     <user>admin</user>
                     <password>admin</password>
                     <noWeb>true</noWeb>
                     <instance>${basedir}/target/transfer1</instance>
                  </configuration>
               </execution>
               <execution>
                  <phase>test-compile</phase>
                  <id>create-transfer-2</id>
                  <goals>
                     <goal>create</goal>
                  </goals>
                  <configuration>
                     <!-- this makes it easier in certain envs -->
                     <allowAnonymous>true</allowAnonymous>
                     <user>admin</user>
                     <password>admin</password>
                     <noWeb>true</noWeb>
                     <portOffset>100</portOffset>
                     <instance>${basedir}/target/transfer2</instance>
                  </configuration>
               </execution>

    */


   @BeforeAll
   public static void createServers() throws Exception {

      File server0Location = getFileServerLocation(SERVER_NAME_0);
      deleteDirectory(server0Location);

      File server1Location = getFileServerLocation(SERVER_NAME_1);
      deleteDirectory(server1Location);

      {
         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setUser("admin").setPassword("admin").setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(server0Location);
         cliCreateServer.createServer();
      }

      {
         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setUser("admin").setPassword("admin").setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(server1Location).setPortOffset(100);
         cliCreateServer.createServer();
      }
   }


   public TransferTest(String sender, String consumer, String source, String target) {
      this.senderProtocol = sender;
      this.consumerProtocol = consumer;
      this.sourceTransferProtocol = source;
      this.targetTransferProtocol = target;
   }

   @Parameters(name = "sender={0}, consumer={1}, sourceOnTransfer={2}, targetOnTransfer={3}")
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

   private ConnectionFactory createConsumerCF() {
      return CFUtil.createConnectionFactory(consumerProtocol, "tcp://localhost:61716");
   }

   private ConnectionFactory createSenderCF() {
      return CFUtil.createConnectionFactory(senderProtocol, "tcp://localhost:61616");
   }

   @BeforeEach
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      disableCheckThread();
      startServer(SERVER_NAME_0, 0, 30000);
      startServer(SERVER_NAME_1, 100, 30000);
   }

   @TestTemplate
   public void testTransferSimpleQueueCopy() throws Exception {
      internalTransferSimpleQueue(false);
   }

   @TestTemplate
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
         assertNotNull(received);
         assertEquals("hello " + i, received.getText());
      }

      sessionTarget.commit();

      assertNull(consumer.receiveNoWait());

      MessageConsumer consumerSource = session.createConsumer(queue);
      connection.start();

      if (copy) {
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage received = (TextMessage) consumerSource.receive(1000);
            assertNotNull(received);
            assertEquals("hello " + i, received.getText());
         }
      }

      assertNull(consumerSource.receiveNoWait());

      connection.close();
      connectionTarget.close();
   }

   @TestTemplate
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
         assertNotNull(received);
      }

      sessionTarget.commit();

      assertNull(consumer.receiveNoWait());

      connection.close();
      connectionTarget.close();
   }

   @TestTemplate
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
         assertNotNull(received);
      }

      sessionTarget.commit();

      connection.close();
      connectionTarget.close();
   }

   @TestTemplate
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
         assertNotNull(received);
      }

      sessionTarget.commit();

      connectionTarget.close();
   }

}
