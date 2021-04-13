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

package org.apache.activemq.artemis.tests.integration.amqp.connect;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.PrintStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.cli.commands.tools.PrintData;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.StringPrintStream;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BrokerInSyncTest extends AmqpClientTestSupport {

   public static final int TIME_BEFORE_RESTART = 1000;
   protected static final int AMQP_PORT_2 = 5673;
   protected static final int AMQP_PORT_3 = 5674;
   private static final Logger logger = Logger.getLogger(BrokerInSyncTest.class);
   ActiveMQServer server_2;

   @Before
   public void startLogging() {
      AssertionLoggerHandler.startCapture();
   }

   @After
   public void stopLogging() {
      try {
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ222214"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      return createServer(AMQP_PORT, false);
   }

   @Test
   public void testSyncOnCreateQueues() throws Exception {
      server.setIdentity("Server1");
      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("connectTowardsServer2", "tcp://localhost:" + AMQP_PORT_2).setReconnectAttempts(3).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setDurable(true));
         server.getConfiguration().addAMQPConnection(amqpConnection);
      }
      server.start();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("Server2");

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("connectTowardsServer1", "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(-1).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setDurable(true));
         server_2.getConfiguration().addAMQPConnection(amqpConnection);
      }

      server_2.start();

      server_2.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false));
      server_2.createQueue(new QueueConfiguration("sometest").setDurable(true));

      Wait.assertTrue(() -> server_2.locateQueue("sometest") != null);
      Wait.assertTrue(() -> server.locateQueue("sometest") != null);

      server.addAddressInfo(new AddressInfo("OnServer1").setAutoCreated(false));
      server.createQueue(new QueueConfiguration("OnServer1").setDurable(true));

      Wait.assertTrue(() -> server.locateQueue("OnServer1") != null);
      Wait.assertTrue("Sync is not working on the way back", () -> server_2.locateQueue("OnServer1") != null, 2000);

      Wait.assertTrue(() -> server_2.locateQueue("sometest") != null);
      Wait.assertTrue(() -> server.locateQueue("sometest") != null);

      for (int i = 0; i < 10; i++) {
         final int queueID = i;
         server_2.createQueue(new QueueConfiguration("test2_" + i).setDurable(true));
         server.createQueue(new QueueConfiguration("test1_" + i).setDurable(true));
         Wait.assertTrue(() -> server.locateQueue("test2_" + queueID) != null);
         Wait.assertTrue(() -> server.locateQueue("test1_" + queueID) != null);
         Wait.assertTrue(() -> server_2.locateQueue("test2_" + queueID) != null);
         Wait.assertTrue(() -> server_2.locateQueue("test1_" + queueID) != null);
      }

      server_2.stop();
      server.stop();
   }

   @Test
   public void testSyncData() throws Exception {
      int NUMBER_OF_MESSAGES = 100;
      server.setIdentity("Server1");
      server.getConfiguration().setBrokerMirrorId((short) 1);
      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("connectTowardsServer2", "tcp://localhost:" + AMQP_PORT_2).setReconnectAttempts(3).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setDurable(true).setTargetMirrorId((short)2));
         server.getConfiguration().addAMQPConnection(amqpConnection);
      }
      server.start();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("Server2");
      server_2.getConfiguration().setBrokerMirrorId((short) 2);

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("connectTowardsServer1", "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(-1).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setDurable(true).setTargetMirrorId((short)1));
         server_2.getConfiguration().addAMQPConnection(amqpConnection);
      }

      server_2.start();

      server_2.addAddressInfo(new AddressInfo(getQueueName()).setAutoCreated(false).addRoutingType(RoutingType.ANYCAST));
      server_2.createQueue(new QueueConfiguration(getQueueName()).setDurable(true).setRoutingType(RoutingType.ANYCAST));

      Wait.assertTrue(() -> server_2.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);

      ConnectionFactory cf1 = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);
      Connection connection1 = cf1.createConnection();
      Session session1 = connection1.createSession(true, Session.SESSION_TRANSACTED);
      connection1.start();

      ConnectionFactory cf2 = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT_2);
      Connection connection2 = cf2.createConnection();
      Session session2 = connection2.createSession(true, Session.SESSION_TRANSACTED);
      connection2.start();

      Queue queue = session1.createQueue(getQueueName());

      MessageProducer producerServer1 = session1.createProducer(queue);
      MessageProducer producerServer2 = session2.createProducer(queue);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         TextMessage message = session1.createTextMessage("test " + i);
         message.setIntProperty("i", i);
         message.setStringProperty("server", server.getIdentity());
         producerServer1.send(message);
      }
      session1.commit();

      org.apache.activemq.artemis.core.server.Queue queueOnServer1 = server.locateQueue(getQueueName());
      org.apache.activemq.artemis.core.server.Queue queueOnServer2 = server_2.locateQueue(getQueueName());
      Assert.assertNotNull(queueOnServer1);
      Assert.assertNotNull(queueOnServer2);

      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer2::getMessageCount);

      for (int i = NUMBER_OF_MESSAGES; i < NUMBER_OF_MESSAGES * 2; i++) {
         TextMessage message = session1.createTextMessage("test " + i);
         message.setIntProperty("i", i);
         message.setStringProperty("server", server_2.getIdentity());
         producerServer2.send(message);
      }
      session2.commit();

      if (logger.isDebugEnabled() && !Wait.waitFor(() -> queueOnServer1.getMessageCount() == NUMBER_OF_MESSAGES * 2)) {
         debugData();
      }

      Wait.assertEquals(NUMBER_OF_MESSAGES * 2, queueOnServer1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES * 2, queueOnServer2::getMessageCount);

      MessageConsumer consumerOn1 = session1.createConsumer(queue);
      for (int i = 0; i < NUMBER_OF_MESSAGES * 2; i++) {
         TextMessage message = (TextMessage) consumerOn1.receive(5000);
         logger.debug("### Client acking message(" + i + ") on server 1, a message that was original sent on " + message.getStringProperty("server") + " text = " + message.getText());
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getIntProperty("i"));
         Assert.assertEquals("test " + i, message.getText());
         session1.commit();
      }

      boolean bothConsumed = Wait.waitFor(() -> {
         long q1 = queueOnServer1.getMessageCount();
         long q2 = queueOnServer2.getMessageCount();
         logger.debug("Queue on Server 1 = " + q1);
         logger.debug("Queue on Server 2 = " + q2);
         return q1 == 0 && q2 == 0;
      }, 5_000, 1000);

      if (logger.isDebugEnabled() && !bothConsumed) {
         debugData();
         Assert.fail("q1 = " + queueOnServer1.getMessageCount() + ", q2 = " + queueOnServer2.getMessageCount());
      }

      Assert.assertEquals(0, queueOnServer1.getMessageCount());
      Assert.assertEquals(0, queueOnServer2.getConsumerCount());

      System.out.println("Queue on Server 1 = " + queueOnServer1.getMessageCount());
      System.out.println("Queue on Server 2 = " + queueOnServer2.getMessageCount());

      server_2.stop();
      server.stop();
   }

   private void debugData() throws Exception {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream out = stringPrintStream.newStream();
      org.apache.activemq.artemis.core.server.Queue queueToDebugOn1 = server.locateQueue(getQueueName());
      org.apache.activemq.artemis.core.server.Queue queueToDebugOn2 = server_2.locateQueue(getQueueName());
      out.println("*******************************************************************************************************************************");
      out.println("Queue on Server 1 with count = " + queueToDebugOn1.getMessageCount());
      queueToDebugOn1.forEach((r) -> out.println("Server1 has reference " + r.getMessage()));
      out.println("*******************************************************************************************************************************");
      out.println("Queue on Server 2 with count = " + queueToDebugOn2.getMessageCount());
      queueToDebugOn2.forEach((r) -> out.println("Server2 has reference " + r.getMessage()));
      out.println("*******************************************************************************************************************************");
      out.println("PrintData Server 1");
      PrintData.printMessages(server.getConfiguration().getJournalLocation(), out, false, false, true, false);
      out.println("*******************************************************************************************************************************");
      out.println("PrintData Server 2");
      PrintData.printMessages(server_2.getConfiguration().getJournalLocation(), out, false, false, true, false);
      logger.debug("Data Available on Servers:\n" + stringPrintStream.toString());
   }

   @Test
   public void testSyncDataNoSuppliedID() throws Exception {
      int NUMBER_OF_MESSAGES = 100;
      server.setIdentity("Server1");

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("connectTowardsServer2", "tcp://localhost:" + AMQP_PORT_2).setReconnectAttempts(3).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setDurable(true).setTargetMirrorId((short) 2));
         server.getConfiguration().addAMQPConnection(amqpConnection);
      }
      server.getConfiguration().setBrokerMirrorId((short) 1);
      server.start();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.getConfiguration().setBrokerMirrorId((short) 2);
      server_2.setIdentity("Server2");

      {
         AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("connectTowardsServer1", "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(-1).setRetryInterval(100);
         amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement().setDurable(true).setTargetMirrorId((short) 1));
         server_2.getConfiguration().addAMQPConnection(amqpConnection);
      }

      server_2.start();

      server_2.addAddressInfo(new AddressInfo(getQueueName()).setAutoCreated(false).addRoutingType(RoutingType.ANYCAST));
      server_2.createQueue(new QueueConfiguration(getQueueName()).setDurable(true).setRoutingType(RoutingType.ANYCAST));

      Wait.assertTrue(() -> server_2.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);

      AmqpClient cf1 = new AmqpClient(new URI("tcp://localhost:" + AMQP_PORT), null, null);
      AmqpConnection connection1 = cf1.createConnection();
      connection1.connect();
      AmqpSession session1 = connection1.createSession();

      AmqpClient cf2 = new AmqpClient(new URI("tcp://localhost:" + AMQP_PORT_2), null, null);
      AmqpConnection connection2 = cf2.createConnection();
      connection2.connect();
      AmqpSession session2 = connection2.createSession();

      AmqpSender producerServer1 = session1.createSender(getQueueName());
      AmqpSender producerServer2 = session2.createSender(getQueueName());

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         AmqpMessage message = new AmqpMessage();
         message.setDurable(true);
         message.setApplicationProperty("i", i);
         producerServer1.send(message);
      }

      org.apache.activemq.artemis.core.server.Queue queueOnServer1 = server.locateQueue(getQueueName());
      org.apache.activemq.artemis.core.server.Queue queueOnServer2 = server_2.locateQueue(getQueueName());
      Assert.assertNotNull(queueOnServer1);
      Assert.assertNotNull(queueOnServer2);

      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer2::getMessageCount);

      for (int i = NUMBER_OF_MESSAGES; i < NUMBER_OF_MESSAGES * 2; i++) {
         AmqpMessage message = new AmqpMessage();
         message.setDurable(true);
         message.setApplicationProperty("i", i);
         producerServer2.send(message);
      }

      Wait.assertEquals(NUMBER_OF_MESSAGES * 2, queueOnServer1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES * 2, queueOnServer2::getMessageCount);

      AmqpReceiver consumerOn1 = session1.createReceiver(getQueueName());
      consumerOn1.flow(NUMBER_OF_MESSAGES * 2 + 1);
      for (int i = 0; i < NUMBER_OF_MESSAGES * 2; i++) {
         AmqpMessage message = consumerOn1.receive(5, TimeUnit.SECONDS);
         Assert.assertNotNull(message);
         message.accept();
         Assert.assertEquals(i, (int) message.getApplicationProperty("i"));
      }

      Wait.assertEquals(0, queueOnServer1::getMessageCount);
      Wait.assertEquals(0, () -> {
         System.out.println(queueOnServer2.getMessageCount());
         return queueOnServer2.getMessageCount();
      });

      connection1.close();
      connection2.close();
      server_2.stop();
      server.stop();
   }

}