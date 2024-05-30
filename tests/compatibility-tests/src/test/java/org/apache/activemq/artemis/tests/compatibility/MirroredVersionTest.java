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

package org.apache.activemq.artemis.tests.compatibility;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.tests.compatibility.base.ClasspathBase;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_THIRTYTHREE_ZERO;

@RunWith(Parameterized.class)
public class MirroredVersionTest extends ClasspathBase {

   private static final String QUEUE_NAME = "MirroredQueue";
   private static final String TOPIC_NAME = "MirroredTopic";

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ClassLoader mainClassloader;

   private final ClassLoader backupClassLoader;

   private final boolean useDual;

   @Parameterized.Parameters(name = "BrokerA={0}, BrokerB={1}, dualMirror={2}")
   public static Collection getParameters() {
      List<Object[]> combinations = new ArrayList<>();
      combinations.add(new Object[]{TWO_THIRTYTHREE_ZERO, SNAPSHOT, true});
      combinations.add(new Object[]{SNAPSHOT, TWO_THIRTYTHREE_ZERO, true});
      combinations.add(new Object[]{SNAPSHOT, SNAPSHOT, true});
      return combinations;
   }

   public MirroredVersionTest(String main, String backup, boolean useDual) throws Exception {
      this.mainClassloader = getClasspath(main);

      this.backupClassLoader = getClasspath(backup);

      this.useDual = useDual;
   }

   @After
   public void cleanupServers() {
      try {
         evaluate(mainClassloader, "multiVersionMirror/mainServerStop.groovy");
      } catch (Exception ignored) {
      }
      try {
         evaluate(backupClassLoader, "multiVersionMirror/backupServerStop.groovy");
      } catch (Exception ignored) {
      }
   }

   @Before
   @After
   public void deleteFolders() {
      FileUtil.deleteDirectory(new File(serverFolder.getRoot().getAbsolutePath(), "1"));
      FileUtil.deleteDirectory(new File(serverFolder.getRoot().getAbsolutePath(), "2"));
   }

   private String createBody(int size) {
      StringWriter writer = new StringWriter();
      PrintWriter pw = new PrintWriter(writer);
      for (int i = 0; i < size; i++) {
         pw.print("-");
      }
      return writer.toString();
   }


   @Test
   public void testMirrorReplica() throws Throwable {
      testMirrorReplica(100);
   }

   @Test
   public void testMirrorReplicaLM() throws Throwable {
      testMirrorReplica(300 * 1024);
   }

   public void testMirrorReplica(int stringSize) throws Throwable {
      String body = createBody(stringSize);
      logger.debug("Starting live");
      startMainBroker();

      ConnectionFactory factoryMain = new JmsConnectionFactory("amqp://localhost:61616");

      try (Connection connection = factoryMain.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
         for (int i = 0; i < 10; i++) {
            TextMessage message = session.createTextMessage("hello " + i + body);
            message.setIntProperty("count", i);
            producer.send(message);
         }
         session.commit();
      }

      logger.debug("restarting main server");
      evaluate(mainClassloader, "multiVersionMirror/mainServerStop.groovy");
      startMainBroker();

      logger.debug("starting backup");
      startBackupBroker();

      ConnectionFactory factoryReplica = new JmsConnectionFactory("amqp://localhost:61617");

      try (Connection connection = factoryReplica.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
         connection.start();
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals("hello " + i + body, message.getText());
            Assert.assertEquals(i, message.getIntProperty("count"));
         }
         session.rollback();
      }

      logger.debug("Restarting backup");

      evaluate(backupClassLoader, "multiVersionMirror/backupServerStop.groovy");
      startBackupBroker();

      try (Connection connection = factoryReplica.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
         connection.start();
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals("hello " + i + body, message.getText());
            Assert.assertEquals(i, message.getIntProperty("count"));
         }
         session.commit();
      }
   }

   private void startMainBroker() throws Exception {
      evaluate(mainClassloader, "multiVersionMirror/mainServer.groovy", serverFolder.getRoot().getAbsolutePath(), "1", QUEUE_NAME, TOPIC_NAME);
   }

   private void startBackupBroker() throws Exception {
      evaluate(backupClassLoader, "multiVersionMirror/backupServer.groovy", serverFolder.getRoot().getAbsolutePath(), "2", QUEUE_NAME, TOPIC_NAME, String.valueOf(useDual));
   }

   @Test
   public void testTopic() throws Throwable {
      int stringSize = 100;
      String body = createBody(stringSize);
      logger.debug("Starting live");
      startMainBroker();
      logger.debug("Starting backup");
      startBackupBroker();

      String clientID1 = "CONNECTION_1";
      String clientID2 = "CONNECTION_2";

      String sub1 = "SUB_1";
      String sub2 = "SUB_2";

      ConnectionFactory factoryMain = new JmsConnectionFactory("amqp://localhost:61616");

      try (javax.jms.Connection connection = factoryMain.createConnection()) {
         connection.setClientID(clientID1);
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic(TOPIC_NAME);
         MessageConsumer consumer = session.createDurableConsumer(topic, sub1);
      }
      try (javax.jms.Connection connection = factoryMain.createConnection()) {
         connection.setClientID(clientID2);
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic(TOPIC_NAME);
         MessageConsumer consumer = session.createDurableConsumer(topic, sub2);
      }

      evaluate(backupClassLoader, "multiVersionMirror/backupServerStop.groovy");

      try (Connection connection = factoryMain.createConnection()) {
         Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(TOPIC_NAME);
         MessageProducer producer = session.createProducer(null);
         for (int i = 0; i < 10; i++) {
            TextMessage message = session.createTextMessage("hello " + i + body);
            message.setIntProperty("count", i);
            producer.send(topic, message);
         }
         session.commit();
      }

      evaluate(mainClassloader, "multiVersionMirror/mainServerStop.groovy");
      startBackupBroker();
      startMainBroker();

      ConnectionFactory factoryReplica = new JmsConnectionFactory("amqp://localhost:61617");

      try (Connection connection = factoryReplica.createConnection()) {
         connection.setClientID(clientID1);
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic(TOPIC_NAME);
         connection.start();
         MessageConsumer consumer = session.createDurableConsumer(topic, sub1);
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage)consumer.receive(5000);
            Assert.assertNotNull(message);
         }
         session.rollback();
      }

      logger.debug("Restarting backup");
      evaluate(backupClassLoader, "multiVersionMirror/backupServerStop.groovy");
      startBackupBroker();

      try (Connection connection = factoryReplica.createConnection()) {
         connection.setClientID(clientID1);
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic(TOPIC_NAME);
         connection.start();
         MessageConsumer consumer = session.createDurableConsumer(topic, sub1);
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage)consumer.receive(5000);
            Assert.assertNotNull(message);
         }
         session.commit();
      }

      try (Connection connection = factoryReplica.createConnection()) {
         connection.setClientID(clientID2);
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic(TOPIC_NAME);
         connection.start();
         MessageConsumer consumer = session.createDurableConsumer(topic, sub2);
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage)consumer.receive(5000);
            Assert.assertNotNull(message);
         }
         session.commit();
      }

      if (useDual) {
         SimpleManagement simpleManagementMainServer = new SimpleManagement("tcp://localhost:61616", null, null);
         Wait.assertEquals(0, () -> simpleManagementMainServer.getMessageCountOnQueue(clientID1 + "." + sub1), 5000);
         Wait.assertEquals(0, () -> simpleManagementMainServer.getMessageCountOnQueue(clientID2 + "." + sub2), 5000);
      }
   }

}