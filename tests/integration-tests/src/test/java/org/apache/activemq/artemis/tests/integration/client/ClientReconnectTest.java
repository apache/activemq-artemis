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

package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerProducer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.version.impl.VersionImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.SpawnedVMCheck;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.VersionLoader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


public class ClientReconnectTest extends ActiveMQTestBase {

   @Rule
   public SpawnedVMCheck spawnedVMCheck = new SpawnedVMCheck();

   private ActiveMQServer server;

   private ServerLocator locator;

   private Process serverProcess;

   private final SimpleString QUEUE = new SimpleString("TestQueue");

   private boolean isNetty() {
      return true;
   }

   private boolean isDurable() {
      return true;
   }

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(isDurable(), isNetty());
      server.start();

      locator = createFactory(isNetty());
      locator.setReconnectAttempts(10);
      locator.setConfirmationWindowSize(1024);
   }

   @After
   public void cleanup() {
      SpawnedVMSupport.forceKill();
   }

   @Test
   public void testProducersRecreatedOnReconnect() throws Exception {
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession(true, true);
      session.createQueue(new QueueConfiguration(QUEUE).setDurable(isDurable()));

      ClientProducer producer = session.createProducer(QUEUE);
      producer.send(session.createMessage(true));

      Queue serverQueue = server.locateQueue(QUEUE);
      Collection<ServerProducer> serverProducers = server.getSessions().iterator().next().getServerProducers();

      Assert.assertEquals(1, serverQueue.getMessageCount());
      Assert.assertEquals(1, serverProducers.size());

      restartServer(server);

      producer.send(session.createMessage(true));

      serverQueue = server.locateQueue(QUEUE);
      serverProducers = server.getSessions().iterator().next().getServerProducers();
      Assert.assertEquals(2, serverQueue.getMessageCount());
      Assert.assertEquals(1, serverProducers.size());
   }

   @Test
   public void testNoStrayConfirmationsAfterReconnect() throws Exception {
      AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler();
      runAfter(() -> loggerHandler.close());

      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession(true, true);
      session.createQueue(new QueueConfiguration(QUEUE).setDurable(isDurable()));
      session.start();

      ClientProducer producer = session.createProducer(QUEUE);
      ClientConsumer consumer = session.createConsumer(QUEUE);
      producer.send(session.createMessage(isDurable()));

      Assert.assertNotNull(consumer.receive(1000));

      restartServer(server);

      producer.send(session.createMessage(isDurable()));
      Assert.assertNotNull(consumer.receive(1000));

      //Force flush to find "missing" packets faster
      sf.getConnection().flush();
      Assert.assertFalse(loggerHandler.findText("AMQ212036"));
   }

   @Test(timeout = 20000)
   public void testIncompatibleVersionAfterReconnect() throws Exception {
      String propertiesFileName = "reconnect-activemq-version.properties";
      int clientVersion = VersionLoader.getVersion().getIncrementingVersion();

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
      cf.setReconnectAttempts(-1);
      cf.setConfirmationWindowSize(102400);

      Connection connection = cf.createConnection();
      connection.start();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = session.createQueue(QUEUE.toString());

      MessageProducer producer = session.createProducer(queue);
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = session.createTextMessage("Message");

      producer.send(message);
      Assert.assertNotNull(consumer.receive(1000));

      server.stop(true);
      waitForServerToStop(server);

      setServerVersionProperties(propertiesFileName, clientVersion - 1);
      serverProcess = SpawnedVMSupport.spawnVM("org.apache.activemq.artemis.tests.integration.client.ClientReconnectTest", new String[]{"-D" + VersionLoader.VERSION_PROP_FILE_KEY + "=" + propertiesFileName}, new String[]{});

      producer.send(message);
      Assert.assertNotNull(consumer.receive(1000));

      session.close();
      connection.close();
      cf.close();

      Assert.assertEquals(0, serverProcess.waitFor());
   }

   private void runPreviousVersionServer() throws Exception {
      Configuration config = createDefaultConfig(isNetty());
      config.setPersistenceEnabled(false);

      ActiveMQServer previousVersionServer = new ActiveMQServerImpl(config);
      previousVersionServer.start();

      Wait.assertTrue(() -> previousVersionServer.locateQueue(QUEUE) != null);

      Queue queue = previousVersionServer.locateQueue(QUEUE);
      Wait.assertEquals(1, () -> queue.getMessagesAcknowledged());
      Wait.assertEquals(0, () -> queue.getConsumerCount());

      previousVersionServer.stop(true);
   }

   private void restartServer(ActiveMQServer server) throws Exception {
      server.stop(true);
      waitForServerToStop(server);
      server.start();
      waitForServerToStart(server);
   }

   private void setServerVersionProperties(String fileName, int version) throws IOException {
      Properties versionProperties = new Properties();

      InputStream in = VersionImpl.class.getClassLoader().getResourceAsStream("activemq-version.properties");
      versionProperties.load(in);

      versionProperties.setProperty("activemq.version.compatibleVersionList", Integer.toString(version));
      versionProperties.setProperty("activemq.version.incrementingVersion", Integer.toString(version));
      versionProperties.store(new FileOutputStream("target/test-classes/" + fileName), null);
   }

   public static void main(String[] args) throws Exception {
      ClientReconnectTest clientReconnectTest = new ClientReconnectTest();
      clientReconnectTest.runPreviousVersionServer();
   }

}
