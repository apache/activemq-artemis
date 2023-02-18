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
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.core.protocol.core.impl.RemotingConnectionImpl;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ServerStatus;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import io.github.checkleak.core.CheckLeak;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.tests.leak.MemoryAssertions.assertMemory;
import static org.apache.activemq.artemis.tests.leak.MemoryAssertions.basicMemoryAsserts;

public class ConnectionLeakTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   ActiveMQServer server;

   @BeforeClass
   public static void beforeClass() throws Exception {
      Assume.assumeTrue(CheckLeak.isLoaded());
   }

   @After
   public void validateServer() throws Exception {
      CheckLeak checkLeak = new CheckLeak();

      // I am doing this check here because the test method might hold a client connection
      // so this check has to be done after the test, and before the server is stopped
      assertMemory(checkLeak, 0, RemotingConnectionImpl.class.getName());

      server.stop();

      server = null;

      clearServers();
      ServerStatus.clear();

      assertMemory(checkLeak, 0, ActiveMQServerImpl.class.getName());
   }

   @Override
   @Before
   public void setUp() throws Exception {
      server = createServer(true, createDefaultConfig(1, true));
      server.getConfiguration().setJournalPoolFiles(4).setJournalMinFiles(2);
      server.start();
   }

   @Test
   public void testAMQP() throws Exception {
      doTest("AMQP");
   }

   @Test
   public void testCore() throws Exception {
      doTest("CORE");
   }

   @Test
   public void testOpenWire() throws Exception {
      doTest("OPENWIRE");
   }

   private void doTest(String protocol) throws Exception {
      int REPEATS = 100;
      int MESSAGES = 20;
      basicMemoryAsserts();

      ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      try (Connection producerConnection = cf.createConnection(); Connection consumerConnection = cf.createConnection()) {

         Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);

         Session consumerSession = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
         consumerConnection.start();

         for (int i = 0; i < REPEATS; i++) {
            {
               Destination source = producerSession.createQueue("source");
               try (MessageProducer sourceProducer = producerSession.createProducer(source)) {
                  for (int msg = 0; msg < MESSAGES; msg++) {
                     Message message = producerSession.createTextMessage("hello " + msg);
                     message.setIntProperty("i", msg);
                     sourceProducer.send(message);
                  }
                  producerSession.commit();
               }
            }
            {
               Destination source = consumerSession.createQueue("source");
               Destination target = consumerSession.createQueue("target");
               // notice I am not closing the consumer directly, just relying on the connection closing
               MessageProducer targetProducer = consumerSession.createProducer(target);
               // I am receiving messages, and pushing them to a different queue
               try (MessageConsumer sourceConsumer = consumerSession.createConsumer(source)) {
                  for (int msg = 0; msg < MESSAGES; msg++) {

                     TextMessage m = (TextMessage) sourceConsumer.receive(5000);
                     Assert.assertNotNull(m);
                     Assert.assertEquals("hello " + msg, m.getText());
                     Assert.assertEquals(msg, m.getIntProperty("i"));
                     targetProducer.send(m);
                  }
                  Assert.assertNull(sourceConsumer.receiveNoWait());
               }
               consumerSession.commit();
            }
         }
      }

      // this is just to drain the messages
      try (Connection targetConnection = cf.createConnection(); Connection consumerConnection = cf.createConnection()) {
         Session targetSession = targetConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = targetSession.createConsumer(targetSession.createQueue("target"));
         targetConnection.start();

         for (int msgI = 0; msgI < REPEATS * MESSAGES; msgI++) {
            Assert.assertNotNull(consumer.receive(5000));
         }

         Assert.assertNull(consumer.receiveNoWait());
      }

      Queue sourceQueue = server.locateQueue("source");
      Queue targetQueue = server.locateQueue("target");

      Wait.assertEquals(0, sourceQueue::getMessageCount);
      Wait.assertEquals(0, targetQueue::getMessageCount);

      if (cf instanceof ActiveMQConnectionFactory) {
         ((ActiveMQConnectionFactory)cf).close();
      }

      basicMemoryAsserts();

   }
}