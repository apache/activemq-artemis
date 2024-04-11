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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.artemis.tests.compatibility.base.ClasspathBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_TWENTYEIGHT_ZERO;

@RunWith(Parameterized.class)
public class MirroredVersionTest extends ClasspathBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final ClassLoader mainClassloader;

   private final ClassLoader backupClassLoader;


   @Parameterized.Parameters(name = "BrokerA={0}, BrokerB={1}")
   public static Collection getParameters() {
      List<Object[]> combinations = new ArrayList<>();
      combinations.add(new Object[]{TWO_TWENTYEIGHT_ZERO, SNAPSHOT});
      combinations.add(new Object[]{SNAPSHOT, TWO_TWENTYEIGHT_ZERO});
      // The SNAPSHOT/SNAPSHOT is here as a test validation only, like in other cases where SNAPSHOT/SNAPSHOT is used.
      combinations.add(new Object[]{SNAPSHOT, SNAPSHOT});
      return combinations;
   }

   public MirroredVersionTest(String main, String backup) throws Exception {
      this.mainClassloader = getClasspath(main);

      this.backupClassLoader = getClasspath(backup);
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
      testMirrorReplicat(100);
   }

   @Test
   public void testMirrorReplicaLM() throws Throwable {
      testMirrorReplicat(300 * 1024);
   }

   public void testMirrorReplicat(int stringSize) throws Throwable {
      String body = createBody(stringSize);
      logger.debug("Starting live");
      evaluate(mainClassloader, "multiVersionMirror/mainServer.groovy", serverFolder.getRoot().getAbsolutePath(), "1");
      logger.debug("Starting backup");
      evaluate(backupClassLoader, "multiVersionMirror/backupServer.groovy", serverFolder.getRoot().getAbsolutePath(), "2");

      ConnectionFactory factoryMain = new JmsConnectionFactory("amqp://localhost:61616");

      try (Connection connection = factoryMain.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue("TestQueue"));
         for (int i = 0; i < 10; i++) {
            TextMessage message = session.createTextMessage("hello " + i + body);
            message.setIntProperty("count", i);
            producer.send(message);
         }
         session.commit();
      }

      ConnectionFactory factoryReplica = new JmsConnectionFactory("amqp://localhost:61617");

      try (Connection connection = factoryReplica.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = session.createConsumer(session.createQueue("TestQueue"));
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
      evaluate(backupClassLoader, "multiVersionMirror/backupServer.groovy", serverFolder.getRoot().getAbsolutePath(), "2");

      try (Connection connection = factoryReplica.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = session.createConsumer(session.createQueue("TestQueue"));
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
}