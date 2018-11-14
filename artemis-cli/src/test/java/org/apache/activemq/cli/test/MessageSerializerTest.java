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
package org.apache.activemq.cli.test;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test to validate that the CLI doesn't throw improper exceptions when invoked.
 */
public class MessageSerializerTest extends CliTestBase {

   private Connection connection;
   private ActiveMQConnectionFactory cf;
   private static final int TEST_MESSAGE_COUNT = 10;

   @Before
   @Override
   public void setup() throws Exception {
      setupAuth();
      super.setup();
      startServer();
      cf = getConnectionFactory(61616);
      connection = cf.createConnection("admin", "admin");
   }

   @After
   @Override
   public void tearDown() throws Exception {
      closeConnection(cf, connection);
      super.tearDown();
   }

   private File createMessageFile() throws IOException {
      return temporaryFolder.newFile("messages.xml");
   }

   private List<Message> generateTextMessages(Session session, String address) throws Exception {
      List<Message> messages = new ArrayList<>(TEST_MESSAGE_COUNT);
      for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
         messages.add(session.createTextMessage(RandomUtil.randomString()));
      }

      sendMessages(session, address, messages);

      return messages;
   }

   private List<Message> generateTextMessages(Session session, Destination destination) throws Exception {
      List<Message> messages = new ArrayList<>(TEST_MESSAGE_COUNT);
      for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
         messages.add(session.createTextMessage(RandomUtil.randomString()));
      }

      sendMessages(session, destination, messages);

      return messages;
   }

   private void checkSentMessages(Session session, List<Message> messages, String address) throws Exception {
      boolean fqqn = false;
      if (address.contains("::")) fqqn = true;

      List<Message> recieved = consumeMessages(session, address, TEST_MESSAGE_COUNT, fqqn);
      for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
         assertEquals(((TextMessage) messages.get(i)).getText(), ((TextMessage) recieved.get(i)).getText());
      }
   }

   private void sendMessages(Session session, String address, List<Message> messages) throws Exception {
      MessageProducer producer = session.createProducer(getDestination(address));
      for (Message m : messages) {
         producer.send(m);
      }
   }

   private void sendMessages(Session session, Destination destination, List<Message> messages) throws Exception {
      MessageProducer producer = session.createProducer(destination);
      for (Message m : messages) {
         producer.send(m);
      }
   }

   private void exportMessages(String address, File output) throws Exception {
      exportMessages(address, TEST_MESSAGE_COUNT, false, "test-client", output);
   }

   private void exportMessages(String address, int noMessages, boolean durable, String clientId, File output) throws Exception {
      if (durable) {
         String[] args = {"consumer",
            "--user", "admin",
            "--password", "admin",
            "--destination", address,
            "--message-count", Integer.toString(noMessages),
            "--data", output.getAbsolutePath(),
            "--clientID", clientId,
            "--durable"};
         Artemis.main(args);
      } else {
         String[] args = {"consumer",
            "--user", "admin",
            "--password", "admin",
            "--destination", address,
            "--message-count", Integer.toString(noMessages),
            "--data", output.getAbsolutePath(),
            "--clientID", clientId};
         Artemis.main(args);
      }
   }

   private void importMessages(String address, File input) throws Exception {
      Artemis.main("producer",
              "--user", "admin",
              "--password", "admin",
              "--destination", address,
              "--data", input.getAbsolutePath());
   }

   private void createBothTypeAddress(String address) throws Exception {
      Artemis.main("address", "create",
              "--user", "admin",
              "--password", "admin",
              "--name", address,
              "--anycast", "--multicast");
   }

   @Test
   public void testTextMessageImportExport() throws Exception {
      String address = "test";
      File file = createMessageFile();

      Session session = createSession(connection);

      List<Message> messages = generateTextMessages(session, address);

      exportMessages(address, file);

      // Ensure there's nothing left to consume
      MessageConsumer consumer = session.createConsumer(getDestination(address));
      assertNull(consumer.receive(1000));
      consumer.close();

      importMessages(address, file);

      checkSentMessages(session, messages, address);
   }

   @Test
   public void testObjectMessageImportExport() throws Exception {
      String address = "test";
      File file = createMessageFile();

      Session session = createSession(connection);

      // Send initial messages.
      List<Message> sent = new ArrayList<>(TEST_MESSAGE_COUNT);
      for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
         sent.add(session.createObjectMessage(UUID.randomUUID()));
      }

      sendMessages(session, address, sent);
      exportMessages(address, file);

      // Ensure there's nothing left to consume
      MessageConsumer consumer = session.createConsumer(getDestination(address));
      assertNull(consumer.receive(1000));
      consumer.close();

      importMessages(address, file);
      List<Message> received = consumeMessages(session, address, TEST_MESSAGE_COUNT, false);
      for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
         assertEquals(((ObjectMessage) sent.get(i)).getObject(), ((ObjectMessage) received.get(i)).getObject());
      }
   }

   @Test
   public void testMapMessageImportExport() throws Exception {
      String address = "test";
      String key = "testKey";
      File file = createMessageFile();

      Session session = createSession(connection);

      List<Message> sent = new ArrayList<>(TEST_MESSAGE_COUNT);
      for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
         MapMessage m = session.createMapMessage();
         m.setString(key, RandomUtil.randomString());
         sent.add(m);
      }

      sendMessages(session, address, sent);
      exportMessages(address, file);

      // Ensure there's nothing left to consume
      MessageConsumer consumer = session.createConsumer(getDestination(address));
      assertNull(consumer.receive(1000));
      consumer.close();

      importMessages(address, file);
      List<Message> received = consumeMessages(session, address, TEST_MESSAGE_COUNT, false);
      for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
         assertEquals(((MapMessage) sent.get(i)).getString(key), ((MapMessage) received.get(i)).getString(key));
      }
   }

   @Test
   public void testSendDirectToQueue() throws Exception {

      String address = "test";
      String queue1Name = "queue1";
      String queue2Name = "queue2";

      createQueue("--multicast", address, queue1Name);
      createQueue("--multicast", address, queue2Name);

      try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616"); Connection connection = cf.createConnection("admin", "admin");) {

         // send messages to queue
         Session session = createSession(connection);

         Destination queue1 = session.createQueue(address + "::" + queue1Name);
         Destination queue2 = session.createQueue(address + "::" + queue2Name);

         MessageConsumer consumer1 = session.createConsumer(queue1);
         MessageConsumer consumer2 = session.createConsumer(queue2);

         Artemis.main("producer",
                 "--user", "admin",
                 "--password", "admin",
                 "--destination", "fqqn://" + address + "::" + queue1Name,
                 "--message-count", "5");

         assertNull(consumer2.receive(1000));
         assertNotNull(consumer1.receive(1000));
      }
   }

   @Test
   public void exportFromFQQN() throws Exception {
      String addr = "address";
      String queue = "queue";
      String fqqn = addr + "::" + queue;
      String destination = "fqqn://" + fqqn;

      File file = createMessageFile();

      createQueue("--multicast", addr, queue);

      Session session = createSession(connection);

      Topic topic = session.createTopic(addr);

      List<Message> messages = generateTextMessages(session, topic);

      exportMessages(destination, file);
      importMessages(destination, file);

      checkSentMessages(session, messages, fqqn);
   }

   @Test
   public void testAnycastToMulticastTopic() throws Exception {
      String mAddress = "testMulticast";
      String aAddress = "testAnycast";
      String queueM1Name = "queueM1";
      String queueM2Name = "queueM2";

      File file = createMessageFile();

      createQueue("--multicast", mAddress, queueM1Name);
      createQueue("--multicast", mAddress, queueM2Name);

      Session session = createSession(connection);

      List<Message> messages = generateTextMessages(session, aAddress);

      exportMessages(aAddress, file);
      importMessages("topic://" + mAddress, file);

      checkSentMessages(session, messages, queueM1Name);
      checkSentMessages(session, messages, queueM2Name);
   }

   @Test
   public void testAnycastToMulticastFQQN() throws Exception {
      String mAddress = "testMulticast";
      String aAddress = "testAnycast";
      String queueM1Name = "queueM1";
      String queueM2Name = "queueM2";
      String fqqnMulticast1 = mAddress + "::" + queueM1Name;
      String fqqnMulticast2 = mAddress + "::" + queueM2Name;

      File file = createMessageFile();

      createQueue("--multicast", mAddress, queueM1Name);
      createQueue("--multicast", mAddress, queueM2Name);

      Session session = createSession(connection);

      List<Message> messages = generateTextMessages(session, aAddress);

      exportMessages(aAddress, file);
      importMessages("fqqn://" + fqqnMulticast1, file);

      checkSentMessages(session, messages, fqqnMulticast1);

      MessageConsumer consumer = session.createConsumer(getDestination(fqqnMulticast2));
      assertNull(consumer.receive(1000));
   }

   @Test
   public void testMulticastTopicToAnycastQueueBothAddress() throws Exception {
      String address = "testBoth";
      String clientId = "test-client-id";
      File file = createMessageFile();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      createBothTypeAddress(address);

      exportMessages("topic://" + address, 0, true, clientId, file);

      connection.start();

      List<Message> messages = generateTextMessages(session, getTopicDestination(address));

      exportMessages("topic://" + address, TEST_MESSAGE_COUNT, true, clientId, file);

      importMessages(address, file);

      checkSentMessages(session, messages, address);
   }

   @Test
   public void testAnycastQueueToMulticastTopicBothAddress() throws Exception {
      String address = "testBoth";
      String clientId = "test-client-id";

      File file = createMessageFile();

      connection.setClientID(clientId);
      createBothTypeAddress(address);
      createQueue("--anycast", address, address);
      Session session = createSession(connection);

      TopicSubscriber subscriber = session.createDurableSubscriber(session.createTopic(address), "test-subscriber");

      List<Message> messages = generateTextMessages(session, address);

      exportMessages(address, file);

      importMessages("topic://" + address, file);
      for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
         TextMessage messageReceived = (TextMessage) subscriber.receive(1000);
         assertEquals(((TextMessage) messages.get(i)).getText(), messageReceived.getText());
      }
   }

   //read individual lines from byteStream
   private ArrayList<String> getOutputLines(TestActionContext context, boolean errorOutput) throws IOException {
      byte[] bytes;

      if (errorOutput) {
         bytes = context.getStdErrBytes();
      } else {
         bytes = context.getStdoutBytes();
      }
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
      ArrayList<String> lines = new ArrayList<>();

      String currentLine = bufferedReader.readLine();
      while (currentLine != null) {
         lines.add(currentLine);
         currentLine = bufferedReader.readLine();
      }

      return lines;
   }

}
