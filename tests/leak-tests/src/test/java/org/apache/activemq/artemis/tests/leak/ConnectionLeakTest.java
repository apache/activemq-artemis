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

import io.github.checkleak.core.CheckLeak;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.protocol.core.impl.RemotingConnectionImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl;
import org.apache.activemq.artemis.core.server.impl.ServerStatus;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.collections.LinkedListImpl;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
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
      server = createServer(false, createDefaultConfig(1, true));
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
      CheckLeak checkLeak = new CheckLeak();
      // Some protocols may create ServerConsumers
      int originalConsumers = checkLeak.getAllObjects(ServerConsumerImpl.class).length;
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
                  consumerSession.commit();

                  Wait.assertTrue(() -> validateClosedConsumers(checkLeak));
               }
            }
         }
      }

      assertMemory(new CheckLeak(), 0, ServerConsumerImpl.class.getName());


      // this is just to drain the messages
      try (Connection targetConnection = cf.createConnection(); Connection consumerConnection = cf.createConnection()) {
         Session targetSession = targetConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = targetSession.createConsumer(targetSession.createQueue("target"));
         targetConnection.start();

         for (int msgI = 0; msgI < REPEATS * MESSAGES; msgI++) {
            Assert.assertNotNull(consumer.receive(5000));
         }

         Assert.assertNull(consumer.receiveNoWait());
         assertMemory(new CheckLeak(), 0, DeliveryImpl.class.getName());
         Wait.assertTrue(() -> validateClosedConsumers(checkLeak));
         consumer = null;
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

   @Test
   public void testCheckIteratorsAMQP() throws Exception {
      testCheckIterators("AMQP");
   }

   @Test
   public void testCheckIteratorsOpenWire() throws Exception {
      testCheckIterators("OPENWIRE");
   }

   @Test
   public void testCheckIteratorsCORE() throws Exception {
      testCheckIterators("CORE");
   }

   public void testCheckIterators(String protocol) throws Exception {
      CheckLeak checkLeak = new CheckLeak();

      String queueName = getName();

      Queue queue = server.createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));

      ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      for (int i = 0; i < 10; i++) {
         Connection connection = cf.createConnection();
         connection.start();
         for (int j = 0; j < 10; j++) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(session.createQueue(queueName));
            producer.send(session.createTextMessage("test"));
            session.commit();
            session.close();
         }

         for (int j = 0; j < 10; j++) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
            consumer.receiveNoWait(); // it doesn't matter if it received or not, just doing something in the queue to kick the iterators
            session.commit();
         }
         connection.close();

         assertMemory(checkLeak, 0, 1, 1, ServerConsumerImpl.class.getName());
         assertMemory(checkLeak, 0, 2, 1, LinkedListImpl.Iterator.class.getName());
      }
   }


   private boolean validateClosedConsumers(CheckLeak checkLeak) throws Exception {
      Object[] objecs = checkLeak.getAllObjects(ServerConsumerImpl.class);
      for (Object obj : objecs) {
         ServerConsumerImpl consumer = (ServerConsumerImpl) obj;
         if (consumer.isClosed()) {
            logger.info("References to closedConsumer {}\n{}", consumer, checkLeak.exploreObjectReferences(3, 1, true, consumer));
            return false;
         }
      }
      return true;
   }
}