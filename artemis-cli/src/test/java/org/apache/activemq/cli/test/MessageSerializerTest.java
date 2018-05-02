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
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test to validate that the CLI doesn't throw improper exceptions when invoked.
 */
public class MessageSerializerTest extends CliTestBase {

   private Connection connection;

   @Before
   @Override
   public void setup() throws Exception {
      setupAuth();
      super.setup();
      startServer();
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
      connection = cf.createConnection("admin", "admin");
   }

   @After
   @Override
   public void tearDown() throws Exception {
      try {
         connection.close();
      } finally {
         stopServer();
         super.tearDown();
      }
   }

   private void setupAuth() throws Exception {
      setupAuth(temporaryFolder.getRoot());
   }

   private void setupAuth(File folder) throws Exception {
      System.setProperty("java.security.auth.login.config", folder.getAbsolutePath() + "/etc/login.config");
   }

   private void startServer() throws Exception {
      File rootDirectory = new File(temporaryFolder.getRoot(), "broker");
      setupAuth(rootDirectory);
      Run.setEmbedded(true);
      Artemis.main("create", rootDirectory.getAbsolutePath(), "--silent", "--no-fsync", "--no-autotune", "--no-web", "--require-login");
      System.setProperty("artemis.instance", rootDirectory.getAbsolutePath());
      Artemis.internalExecute("run");
   }

   private void stopServer() throws Exception {
      Artemis.internalExecute("stop");
      assertTrue(Run.latchRunning.await(5, TimeUnit.SECONDS));
      assertEquals(0, LibaioContext.getTotalMaxIO());
   }

   private File createMessageFile() throws IOException {
      return temporaryFolder.newFile("messages.xml");
   }

   @Test
   public void testTextMessageImportExport() throws Exception {
      String address = "test";
      int noMessages = 10;
      File file = createMessageFile();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      List<Message> sent = new ArrayList<>(noMessages);
      for (int i = 0; i < noMessages; i++) {
         sent.add(session.createTextMessage(RandomUtil.randomString()));
      }

      sendMessages(session, address, sent);
      exportMessages(address, noMessages, file);

      // Ensure there's nothing left to consume
      MessageConsumer consumer = session.createConsumer(getDestination(address));
      assertNull(consumer.receive(1000));
      consumer.close();

      importMessages(address, file);

      List<Message> received = consumeMessages(session, address, noMessages, false);
      for (int i = 0; i < noMessages; i++) {
         assertEquals(((TextMessage) sent.get(i)).getText(), ((TextMessage) received.get(i)).getText());
      }
   }

   @Test
   public void testObjectMessageImportExport() throws Exception {
      String address = "test";
      int noMessages = 10;
      File file = createMessageFile();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      // Send initial messages.
      List<Message> sent = new ArrayList<>(noMessages);
      for (int i = 0; i < noMessages; i++) {
         sent.add(session.createObjectMessage(UUID.randomUUID()));
      }

      sendMessages(session, address, sent);
      exportMessages(address, noMessages, file);

      // Ensure there's nothing left to consume
      MessageConsumer consumer = session.createConsumer(getDestination(address));
      assertNull(consumer.receive(1000));
      consumer.close();

      importMessages(address, file);
      List<Message> received = consumeMessages(session, address, noMessages, false);
      for (int i = 0; i < noMessages; i++) {
         assertEquals(((ObjectMessage) sent.get(i)).getObject(), ((ObjectMessage) received.get(i)).getObject());
      }
   }

   @Test
   public void testMapMessageImportExport() throws Exception {
      String address = "test";
      int noMessages = 10;
      String key = "testKey";
      File file = createMessageFile();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      List<Message> sent = new ArrayList<>(noMessages);
      for (int i = 0; i < noMessages; i++) {
         MapMessage m = session.createMapMessage();
         m.setString(key, RandomUtil.randomString());
         sent.add(m);
      }

      sendMessages(session, address, sent);
      exportMessages(address, noMessages, file);

      // Ensure there's nothing left to consume
      MessageConsumer consumer = session.createConsumer(getDestination(address));
      assertNull(consumer.receive(1000));
      consumer.close();

      importMessages(address, file);
      List<Message> received = consumeMessages(session, address, noMessages, false);
      for (int i = 0; i < noMessages; i++) {
         assertEquals(((MapMessage) sent.get(i)).getString(key), ((MapMessage) received.get(i)).getString(key));
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

   private List<Message> consumeMessages(Session session, String address, int noMessages, boolean fqqn) throws Exception {
      Destination destination = fqqn ? session.createQueue(address) : getDestination(address);
      MessageConsumer consumer = session.createConsumer(destination);

      List<Message> messages = new ArrayList<>();
      for (int i = 0; i < noMessages; i++) {
         Message m = consumer.receive(1000);
         assertNotNull(m);
         messages.add(m);
      }
      return messages;
   }

   private void exportMessages(String address, int noMessages, File output) throws Exception {
      Artemis.main("consumer",
                   "--user", "admin",
                   "--password", "admin",
                   "--destination", address,
                   "--message-count", "" + noMessages,
                   "--data", output.getAbsolutePath());
   }

   private void importMessages(String address, File input) throws Exception {
      Artemis.main("producer",
                   "--user", "admin",
                   "--password", "admin",
                   "--destination", address,
                   "--data", input.getAbsolutePath());
   }

   private void createQueue(String routingTypeOption, String address, String queueName) throws Exception {
      Artemis.main("queue", "create",
                   "--user", "admin",
                   "--password", "admin",
                   "--address", address,
                   "--name", queueName,
                   routingTypeOption,
                   "--durable",
                   "--preserve-on-no-consumers",
                   "--auto-create-address");
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
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         connection.start();

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
      int noMessages = 10;

      createQueue("--multicast", addr, queue);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      Topic topic = session.createTopic(addr);

      List<Message> messages = new ArrayList<>(noMessages);
      for (int i = 0; i < noMessages; i++) {
         messages.add(session.createTextMessage(RandomUtil.randomString()));
      }

      sendMessages(session, topic, messages);

      exportMessages(destination, noMessages, file);
      importMessages(destination, file);

      List<Message> recieved = consumeMessages(session, fqqn, noMessages, true);
      for (int i = 0; i < noMessages; i++) {
         assertEquals(((TextMessage) messages.get(i)).getText(), ((TextMessage) recieved.get(i)).getText());
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

   private void sendMessages(Session session, String queueName, int messageCount) throws JMSException {
      MessageProducer producer = session.createProducer(getDestination(queueName));

      TextMessage message = session.createTextMessage(getTestMessageBody());

      for (int i = 0; i < messageCount; i++) {
         producer.send(message);
      }
   }

   private String getTestMessageBody() {
      return "Sample Message";
   }

   private Destination getDestination(String queueName) {
      return ActiveMQDestination.createDestination("queue://" + queueName, ActiveMQDestination.TYPE.QUEUE);
   }
}
