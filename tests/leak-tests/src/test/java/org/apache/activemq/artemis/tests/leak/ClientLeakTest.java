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
package org.apache.activemq.artemis.tests.leak;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;

import io.github.checkleak.core.CheckLeak;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.apache.qpid.proton.engine.impl.ReceiverImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.tests.leak.MemoryAssertions.assertMemory;

// This test spawns the server as a separate VM
// as we need to count exclusively client objects from qpid-proton
public class ClientLeakTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String LEAK_SERVER = "LEAK-SERVER-STARTED";
   Process serverProcess;

   public static void main(String[] arg) {

      try {
         ConfigurationImpl configuration = new ConfigurationImpl().setSecurityEnabled(false).setJournalMinFiles(2).setJournalFileSize(100 * 1024).setJournalType(getDefaultJournalType()).setJournalDirectory("./data/journal").setBindingsDirectory("./data/binding").setPagingDirectory("./data/page").setLargeMessagesDirectory("./data/lm").setJournalCompactMinFiles(0).setJournalCompactPercentage(0).setClusterPassword(CLUSTER_PASSWORD).setJournalDatasync(false);
         configuration.addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, new HashMap<String, Object>(), "netty", new HashMap<String, Object>()));
         ActiveMQServer server = ActiveMQServers.newActiveMQServer(configuration, false);
         server.start();
         System.out.println(LEAK_SERVER);
      } catch (Throwable e) {
         e.printStackTrace();
         System.exit(-1);
      }

   }

   @BeforeClass
   public static void beforeClass() throws Exception {
      Assume.assumeTrue(CheckLeak.isLoaded());
   }

   @Override
   @Before
   public void setUp() throws Exception {
      serverProcess = SpawnedVMSupport.spawnVM(ClientLeakTest.class.getName());
      runAfter(serverProcess::destroyForcibly);

      boolean success = false;
      long time = System.currentTimeMillis() + 5_000;

      // this loop will keep trying a connection until the serer has started
      do {
         try {
            ConnectionFactory cf = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
            try (Connection connection = cf.createConnection()) {
               success = true;
            }
         } catch (Throwable e) {
            logger.debug(e.getMessage(), e);
            Thread.sleep(100);
         }

      }
      while (success == false && System.currentTimeMillis() < time);
      Assert.assertTrue(success);
   }

   @After
   public void stopServer() throws Exception {
      serverProcess.destroyForcibly();
   }

   @Test
   public void testRepeatAMQPSessions() throws Exception {
      CheckLeak checkLeak = new CheckLeak();

      ConnectionFactory cf = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
      Connection connection = cf.createConnection();
      for (int i = 0; i < 10; i++) {
         for (int j = 0; j < 10; j++) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(session.createQueue("test"));
            producer.send(session.createTextMessage("test"));
            session.commit();
            session.close();
         }

         for (int j = 0; j < 10; j++) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(session.createQueue("test"));
            connection.start();
            Message message = consumer.receive(1000);
            Assert.assertNotNull(message);
            session.commit();
            // consumer.close(); // uncomment this and the test will pass.
            session.close();
         }
         assertMemory(checkLeak, 0, 5, 5, ReceiverImpl.class.getName());
      }
      connection.close();
   }

}