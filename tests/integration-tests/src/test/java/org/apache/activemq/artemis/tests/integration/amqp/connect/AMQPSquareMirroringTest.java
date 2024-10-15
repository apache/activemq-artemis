/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AMQPSquareMirroringTest extends AmqpClientTestSupport {

   protected static final int AMQP_PORT_2 = 5673;
   protected static final int AMQP_PORT_3 = 5674;
   protected static final int AMQP_PORT_4 = 5675;

   ActiveMQServer server_2;
   ActiveMQServer server_3;
   ActiveMQServer server_4;

   @Override
   protected ActiveMQServer createServer() throws Exception {
      return createServer(AMQP_PORT, false);
   }

   protected String getConfiguredProtocols() {
      return "AMQP,CORE,OPENWIRE";
   }

   @Test
   public void testSquare() throws Exception {
      server_2 = createServer(AMQP_PORT_2, false);
      server_3 = createServer(AMQP_PORT_3, false);
      server_4 = createServer(AMQP_PORT_4, false);

      // name the servers, for convenience during debugging
      server.getConfiguration().setName("1");
      server_2.getConfiguration().setName("2");
      server_3.getConfiguration().setName("3");
      server_4.getConfiguration().setName("4");

      /**
       *
       * Setup the mirroring topology to be a square:
       *
       * 1 <- - -> 2
       * ^         ^       The link between 1 and 2 and the
       * |         |       link between 3 and 4 are noForward
       * v         v       links in both directions.
       * 4 <- - -> 3
       */

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "1to2", "tcp://localhost:" + AMQP_PORT_2).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
         server.getConfiguration().addAMQPConnection(amqpConnection);
         amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "1to4", "tcp://localhost:" + AMQP_PORT_4).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
         server.getConfiguration().addAMQPConnection(amqpConnection);
      }

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "2to1", "tcp://localhost:" + AMQP_PORT).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
         server_2.getConfiguration().addAMQPConnection(amqpConnection);
         amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "2to3", "tcp://localhost:" + AMQP_PORT_3).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
         server_2.getConfiguration().addAMQPConnection(amqpConnection);
      }

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "3to2", "tcp://localhost:" + AMQP_PORT_2).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
         server_3.getConfiguration().addAMQPConnection(amqpConnection);
         amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "3to4", "tcp://localhost:" + AMQP_PORT_4).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
         server_3.getConfiguration().addAMQPConnection(amqpConnection);
      }

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "4to1", "tcp://localhost:" + AMQP_PORT).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
         server_4.getConfiguration().addAMQPConnection(amqpConnection);
         amqpConnection = new AMQPBrokerConnectConfiguration(getTestMethodName() + "4to3", "tcp://localhost:" + AMQP_PORT_3).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setNoForward(true));
         server_4.getConfiguration().addAMQPConnection(amqpConnection);
      }

      server.start();
      server_2.start();
      server_3.start();
      server_4.start();

      createAddressAndQueues(server);
      Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server_2.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server_3.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server_4.locateQueue(getQueueName()) != null);

      Queue q1 = server.locateQueue(getQueueName());
      assertNotNull(q1);

      Queue q2 = server.locateQueue(getQueueName());
      assertNotNull(q2);

      Queue q3 = server.locateQueue(getQueueName());
      assertNotNull(q3);

      Queue q4 = server.locateQueue(getQueueName());
      assertNotNull(q4);

      ConnectionFactory factory = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT);
      ConnectionFactory factory2 = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT_2);
      ConnectionFactory factory3 = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT_3);
      ConnectionFactory factory4 = CFUtil.createConnectionFactory(randomProtocol(), "tcp://localhost:" + AMQP_PORT_4);

      try (Connection conn = factory4.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }
      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 10; i < 20; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }
      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 20; i < 30; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }
      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
         for (int i = 30; i < 40; i++) {
            producer.send(session.createTextMessage("message " + i));
         }
      }

      Thread.sleep(100); // some time to allow eventual loops

      Wait.assertEquals(40L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(40L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(40L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(40L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }

      Wait.assertEquals(30L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(30L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(30L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(30L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 10; i < 20; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }

      Wait.assertEquals(20L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(20L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(20L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(20L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 20; i < 30; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }

      Wait.assertEquals(10L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(10L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory4.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         for (int i = 30; i < 40; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            assertNotNull(message);
            assertEquals("message " + i, message.getText());
         }
         consumer.close();
      }

      Wait.assertEquals(0L, q1::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q2::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q3::getMessageCount, 1000, 100);
      Wait.assertEquals(0L, q4::getMessageCount, 1000, 100);

      try (Connection conn = factory.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

      try (Connection conn = factory2.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

      try (Connection conn = factory3.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

      try (Connection conn = factory4.createConnection()) {
         Session session = conn.createSession();
         conn.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(getQueueName()));
         assertNull(consumer.receiveNoWait());
         consumer.close();
      }

   }
}