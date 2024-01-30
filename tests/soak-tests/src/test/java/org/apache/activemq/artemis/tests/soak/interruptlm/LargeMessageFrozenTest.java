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

package org.apache.activemq.artemis.tests.soak.interruptlm;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Locale;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.TcpProxy;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test various scenarios with broker communication in large message */
public class LargeMessageFrozenTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   TcpProxy proxy;

   ActiveMQServer server;

   @Before
   public void startServer() throws Exception {
      server = createServer(true, true);
      server.getConfiguration().addAcceptorConfiguration("alternate", "tcp://localhost:44444?amqpIdleTimeout=100");
      server.start();
   }

   private void startProxy() {
      proxy = new TcpProxy("localhost", 44444, 33333, false);
      proxy.startProxy();
      runAfter(proxy::stopProxy);

      proxy.tryCore(null, null);
   }

   @Test
   public void testFreezeCore() throws Exception {
      testFreeze("CORE");
   }

   @Test
   public void testFreezeAMQP() throws Exception {
      testFreeze("AMQP");
   }

   public void testFreeze(String protocol) throws Exception {
      startProxy();

      ConnectionFactory factory;
      switch (protocol.toUpperCase(Locale.ROOT)) {
         case "CORE":
            ActiveMQConnectionFactory artemisfactory = new ActiveMQConnectionFactory("tcp://localhost:33333?connectionTTL=1000&clientFailureCheckPeriod=100&consumerWindowSize=1000");
            Assert.assertEquals(100, artemisfactory.getServerLocator().getClientFailureCheckPeriod());
            Assert.assertEquals(1000, artemisfactory.getServerLocator().getConnectionTTL());
            Assert.assertEquals(1000, artemisfactory.getServerLocator().getConsumerWindowSize());
            factory = artemisfactory;
            break;
         case "AMQP":
            JmsConnectionFactory qpidFactory = new JmsConnectionFactory("amqp://localhost:33333?amqp.idleTimeout=1000&jms.prefetchPolicy.all=2");
            factory = qpidFactory;
            break;
         default:
            factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:33333");
      }

      org.apache.activemq.artemis.core.server.Queue serverQueue = server.createQueue(new QueueConfiguration(getName()).setRoutingType(RoutingType.ANYCAST).setDurable(true));

      Connection connection = factory.createConnection();
      runAfter(connection::close);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Queue queue = session.createQueue(getName());

      Assert.assertEquals(1, proxy.getInboundHandlers().size());
      Assert.assertEquals(1, proxy.getOutbounddHandlers().size());

      String body;
      {
         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < 10 * 1024 * 1024) {
            buffer.append("Not so big, but big!!");
         }
         body = buffer.toString();
      }

      int NUMBER_OF_MESSAGES = 10;

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         producer.send(session.createTextMessage(body));
      }
      session.commit();

      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      boolean failed = false;

      for (int repeat = 0; repeat < 5; repeat++) {
         try {
            for (int i = 0; i < 1; i++) {
               Assert.assertNotNull(consumer.receive(1000));
            }
            proxy.stopAllHandlers();
            consumer.receive(100);
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE); // just to force an exception
         } catch (Exception expected) {
            logger.info(expected.getMessage(), expected);
            failed = true;
         }

         Assert.assertTrue(failed);
         server.getRemotingService().getConnections().forEach(r -> r.fail(new ActiveMQException("forced failure")));

         connection = factory.createConnection();
         connection.start();
         runAfter(connection::close);
         session = connection.createSession(true, Session.SESSION_TRANSACTED);
         consumer = session.createConsumer(queue);
      }

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         TextMessage message = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals(body, message.getText());
         session.commit();
      }

      Wait.assertEquals(0, () -> {
         System.gc();
         return server.getConfiguration().getLargeMessagesLocation().listFiles().length;
      });
   }

   @Test
   public void testRemoveConsumerCORE() throws Exception {
      testRemoveConsumer("CORE");
   }

   @Test
   public void testRemoveConsumerAMQP() throws Exception {
      testRemoveConsumer("AMQP");
   }

   @Test
   public void testRemoveConsumerOpenWire() throws Exception {
      testRemoveConsumer("OPENWIRE");
   }

   public void testRemoveConsumer(String protocol) throws Exception {

      ConnectionFactory factory;
      switch (protocol.toUpperCase(Locale.ROOT)) {
         case "CORE":
            ActiveMQConnectionFactory artemisfactory = new ActiveMQConnectionFactory("tcp://localhost:44444?connectionTTL=1000&clientFailureCheckPeriod=100&consumerWindowSize=1000");
            Assert.assertEquals(100, artemisfactory.getServerLocator().getClientFailureCheckPeriod());
            Assert.assertEquals(1000, artemisfactory.getServerLocator().getConnectionTTL());
            Assert.assertEquals(1000, artemisfactory.getServerLocator().getConsumerWindowSize());
            factory = artemisfactory;
            break;
         case "AMQP":
            JmsConnectionFactory qpidFactory = new JmsConnectionFactory("amqp://localhost:44444?amqp.idleTimeout=300&jms.prefetchPolicy.all=10");
            factory = qpidFactory;
            break;
         default:
            factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:44444");
      }

      org.apache.activemq.artemis.core.server.Queue serverQueue = server.createQueue(new QueueConfiguration(getName()).setRoutingType(RoutingType.ANYCAST).setDurable(true));

      Connection connection = factory.createConnection();
      runAfter(connection::close);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Queue queue = session.createQueue(getName());

      String body;
      {
         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < 300 * 1024) {
            buffer.append("Not so big, but big!!");
         }
         body = buffer.toString();
      }

      int NUMBER_OF_MESSAGES = 10;

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         producer.send(session.createTextMessage(body));
      }
      session.commit();

      ArrayList<MessageReference> queueMessages = new ArrayList<>();

      Wait.assertEquals(NUMBER_OF_MESSAGES, serverQueue::getMessageCount);

      serverQueue.forEach(queueMessages::add);

      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      Assert.assertEquals(1, serverQueue.getConsumers().size());
      ServerConsumerImpl serverConsumer = (ServerConsumerImpl) serverQueue.getConsumers().iterator().next();


      TextMessage message = (TextMessage) consumer.receive(100);
      Assert.assertNotNull(message);
      Assert.assertEquals(body, message.getText());

      serverConsumer.errorProcessing(new Exception("Dumb error"), queueMessages.get(0));

      try {
         consumer.receiveNoWait();
      } catch (Exception e) {
         e.printStackTrace();
      }
      server.getRemotingService().getConnections().forEach(r -> r.fail(new ActiveMQException("forced failure")));

      connection = factory.createConnection();
      runAfter(connection::close);

      session = connection.createSession(true, Session.SESSION_TRANSACTED);
      consumer = session.createConsumer(queue);
      connection.start();

      long recCount = serverQueue.getMessageCount();

      for (int i = 0; i < recCount; i++) {
         TextMessage recMessage = (TextMessage)consumer.receive(5000);
         Assert.assertNotNull(recMessage);
         Assert.assertEquals(body, recMessage.getText());
         session.commit();
      }

      Assert.assertNull(consumer.receiveNoWait());

      // I could have done this assert before the loop
      // but I also wanted to see a condition where messages get damaged
      Assert.assertEquals(NUMBER_OF_MESSAGES, recCount);

      Wait.assertEquals(0, serverQueue::getMessageCount);

      Wait.assertEquals(0, () -> {
         System.gc();
         return server.getConfiguration().getLargeMessagesLocation().listFiles().length;
      });
   }

   @Test
   public void testFreezeAutoAckAMQP() throws Exception {
      testFreezeAutoAck("AMQP");
   }

   public void testFreezeAutoAck(String protocol) throws Exception {

      startProxy();
      ConnectionFactory factory;
      switch (protocol.toUpperCase(Locale.ROOT)) {
         case "CORE":
            ActiveMQConnectionFactory artemisfactory = new ActiveMQConnectionFactory("tcp://localhost:33333?connectionTTL=1000&clientFailureCheckPeriod=100&consumerWindowSize=1000");
            Assert.assertEquals(100, artemisfactory.getServerLocator().getClientFailureCheckPeriod());
            Assert.assertEquals(1000, artemisfactory.getServerLocator().getConnectionTTL());
            Assert.assertEquals(1000, artemisfactory.getServerLocator().getConsumerWindowSize());
            factory = artemisfactory;
            break;
         case "AMQP":
            JmsConnectionFactory qpidFactory = new JmsConnectionFactory("amqp://localhost:33333?amqp.idleTimeout=1000&jms.prefetchPolicy.all=2");
            factory = qpidFactory;
            break;
         default:
            factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:33333");
      }

      org.apache.activemq.artemis.core.server.Queue serverQueue = server.createQueue(new QueueConfiguration(getName()).setRoutingType(RoutingType.ANYCAST).setDurable(true));

      Connection connection = factory.createConnection();
      runAfter(connection::close);
      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = sessionConsumer.createQueue(getName());

      Assert.assertEquals(1, proxy.getInboundHandlers().size());
      Assert.assertEquals(1, proxy.getOutbounddHandlers().size());

      String body;
      {
         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < 10 * 1024 * 1024) {
            buffer.append("Not so big, but big!!");
         }
         body = buffer.toString();
      }

      int NUMBER_OF_MESSAGES = 40;

      try (Session sessionProducer = connection.createSession(true, Session.AUTO_ACKNOWLEDGE)) {
         MessageProducer producer = sessionProducer.createProducer(queue);
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            producer.send(sessionConsumer.createTextMessage(body));
         }
         sessionProducer.commit();
      }

      MessageConsumer consumer = sessionConsumer.createConsumer(queue);
      connection.start();

      boolean failed = false;

      try {
         for (int i = 0; i < 10; i++) {
            consumer.receive(5000);
         }
         proxy.stopAllHandlers();
         consumer.receive(100);
         connection.createSession(false, Session.AUTO_ACKNOWLEDGE); // just to force an exception
      } catch (Exception expected) {
         logger.info(expected.getMessage(), expected);
         failed = true;
      }

      Wait.assertEquals(0, () -> server.getActiveMQServerControl().getConnectionCount());

      long numberOfMessages = serverQueue.getMessageCount();

      Assert.assertTrue(failed);

      connection = factory.createConnection();
      connection.start();
      runAfter(connection::close);
      sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queue = sessionConsumer.createQueue(getName());
      consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numberOfMessages; i++) {
         TextMessage message = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals(body, message.getText());
      }

      Assert.assertNull(consumer.receiveNoWait());

      Assert.assertEquals(0L, serverQueue.getMessageCount());

      Wait.assertEquals(0, () -> {
         System.gc();
         return server.getConfiguration().getLargeMessagesLocation().listFiles().length;
      });
   }
}
