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

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.queue.CreateQueue;
import org.apache.activemq.artemis.cli.commands.tools.LockAbstract;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoader;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CliTestBase {

   @Rule
   public TemporaryFolder temporaryFolder;

   @Rule
   public ThreadLeakCheckRule leakCheckRule = new ThreadLeakCheckRule();

   private String original = System.getProperty("java.security.auth.login.config");

   public CliTestBase() {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Before
   public void setup() throws Exception {
      Run.setEmbedded(true);
      PropertiesLoader.resetUsersAndGroupsCache();
   }

   @After
   public void tearDown() throws Exception {
      ActiveMQClient.clearThreadPools();
      System.clearProperty("artemis.instance");
      System.clearProperty("artemis.instance.etc");
      Run.setEmbedded(false);

      if (original == null) {
         System.clearProperty("java.security.auth.login.config");
      } else {
         System.setProperty("java.security.auth.login.config", original);
      }

      LockAbstract.unlock();
   }

   protected Object startServer() throws Exception {
      File rootDirectory = new File(temporaryFolder.getRoot(), "broker");
      setupAuth(rootDirectory);
      Run.setEmbedded(true);
      Artemis.main("create", rootDirectory.getAbsolutePath(), "--silent", "--no-fsync", "--no-autotune", "--no-web", "--require-login", "--disable-persistence");
      System.setProperty("artemis.instance", rootDirectory.getAbsolutePath());
      return Artemis.internalExecute("run");
   }

   void setupAuth() {
      setupAuth(temporaryFolder.getRoot());
   }

   void setupAuth(File folder) {
      System.setProperty("java.security.auth.login.config", folder.getAbsolutePath() + "/etc/login.config");
   }

   protected void stopServer() throws Exception {
      Artemis.internalExecute("stop");
      assertTrue(Run.latchRunning.await(5, TimeUnit.SECONDS));
      assertEquals(0, LibaioContext.getTotalMaxIO());
   }

   protected ActiveMQConnectionFactory getConnectionFactory(int serverPort) {
      return new ActiveMQConnectionFactory("tcp://localhost:" + String.valueOf(serverPort));
   }

   protected void createQueue(RoutingType routingType, String address, String queueName) throws Exception {
      new CreateQueue()
         .setAddress(address)
         .setName(queueName)
         .setAnycast(RoutingType.ANYCAST.equals(routingType))
         .setMulticast(RoutingType.MULTICAST.equals(routingType))
         .setDurable(true)
         .setPreserveOnNoConsumers(true)
         .setAutoCreateAddress(true)
         .setUser("admin")
         .setPassword("admin")
         .execute(new TestActionContext());
   }

   void closeConnection(ActiveMQConnectionFactory cf, Connection connection) throws Exception {
      try {
         connection.close();
         cf.close();
      } finally {
         stopServer();
      }
   }

   protected Session createSession(Connection connection) throws JMSException {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
      return session;
   }

   protected List<Message> consumeMessages(Session session, String address, int noMessages, boolean fqqn) throws Exception {
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

   Destination getDestination(String queueName) {
      return ActiveMQDestination.createDestination("queue://" + queueName, ActiveMQDestination.TYPE.QUEUE);
   }

   Destination getTopicDestination(String queueName) {
      return ActiveMQDestination.createDestination("topic://" + queueName, ActiveMQDestination.TYPE.TOPIC);
   }

}
