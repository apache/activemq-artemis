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
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AMQPReplicaTest extends AmqpClientTestSupport {

   protected static final int AMQP_PORT_2 = 5673;
   protected static final int AMQP_PORT_3 = 5674;
   public static final int TIME_BEFORE_RESTART = 1000;

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
   public void testReplicaCatchupOnQueueCreates() throws Exception {
      server.setIdentity("Server1");
      server.stop();

      server_2 = createServer(AMQP_PORT_2, false);

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server_2.start();

      server_2.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false));
      server_2.createQueue(new QueueConfiguration("sometest").setDurable(true));

      Wait.assertTrue(() -> server_2.locateQueue("sometest") != null);

      server_2.stop();

      server.start();
      Assert.assertTrue(server.locateQueue("sometest") == null);
      Wait.assertTrue(server::isActive);
      server_2.start();
      // if this does not succeed the catch up did not arrive at the other server
      Wait.assertTrue(() -> server.locateQueue("sometest") != null);
      server_2.stop();
      server.stop();
   }

   @Test
   public void testDoNotSendDelete() throws Exception {
      testDoNotSendStuff(false);
   }

   @Test
   public void testDoNotSendCreate() throws Exception {
      testDoNotSendStuff(true);
   }

   private void testDoNotSendStuff(boolean sendCreate) throws Exception {
      boolean ignoreCreate = false;
      server.start();

      final SimpleString ADDRESS_NAME = SimpleString.toSimpleString("address");

      server_2 = createServer(AMQP_PORT_2, false);

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      AMQPMirrorBrokerConnectionElement mirror = new AMQPMirrorBrokerConnectionElement();
      if (ignoreCreate) {
         mirror.setQueueCreation(false);
      } else {
         mirror.setQueueCreation(true);
         mirror.setQueueRemoval(false);
      }
      amqpConnection.addElement(mirror);
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server_2.start();
      Wait.assertTrue(server::isActive);

      server_2.addAddressInfo(new AddressInfo(ADDRESS_NAME).addRoutingType(RoutingType.ANYCAST));
      server_2.createQueue(new QueueConfiguration(ADDRESS_NAME).setDurable(true).setAddress(ADDRESS_NAME));

      if (!ignoreCreate) {
         Wait.assertTrue(() -> server.locateQueue(ADDRESS_NAME) != null);
         Wait.assertTrue(() -> server.getAddressInfo(ADDRESS_NAME) != null);
      }

      if (ignoreCreate) {
         Thread.sleep(500); // things are asynchronous, I need to wait some time to make sure things are transferred over
         Assert.assertTrue(server.locateQueue(ADDRESS_NAME) == null);
         Assert.assertTrue(server.getAddressInfo(ADDRESS_NAME) == null);
      }
      server_2.stop();
      server.stop();
   }

   @Test
   public void testReplicaCatchupOnQueueCreatesAndDeletes() throws Exception {
      server.start();
      server.setIdentity("Server1");
      server.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false).addRoutingType(RoutingType.MULTICAST));
      // This queue will disappear from the source, so it should go
      server.createQueue(new QueueConfiguration("ToBeGone").setDurable(true).setRoutingType(RoutingType.MULTICAST));
      server.stop();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server_2.start();

      server_2.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false).addRoutingType(RoutingType.MULTICAST));
      server_2.createQueue(new QueueConfiguration("sometest").setDurable(true).setRoutingType(RoutingType.MULTICAST));

      Wait.assertTrue(() -> server_2.locateQueue("sometest") != null);

      server_2.stop();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");

      amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server.start();
      Assert.assertTrue(server.locateQueue("sometest") == null);
      Assert.assertTrue(server.locateQueue("ToBeGone") != null);
      Wait.assertTrue(server::isActive);
      server_2.start();
      // if this does not succeed the catch up did not arrive at the other server
      Wait.assertTrue(() -> server.locateQueue("sometest") != null);
      Wait.assertTrue(() -> server.locateQueue("ToBeGone") == null);
      server_2.stop();
      server.stop();
   }


   @Test
   public void testReplicaWithDurable() throws Exception {
      server.start();
      server.setIdentity("Server1");
      server.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false).addRoutingType(RoutingType.MULTICAST));
      // This queue will disappear from the source, so it should go
      server.createQueue(new QueueConfiguration("ToBeGone").setDurable(true).setRoutingType(RoutingType.MULTICAST));
      server.stop();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server_2.start();

      server_2.addAddressInfo(new AddressInfo("sometest").setAutoCreated(false).addRoutingType(RoutingType.MULTICAST));
      server_2.createQueue(new QueueConfiguration("sometest").setDurable(true).setRoutingType(RoutingType.MULTICAST));

      Wait.assertTrue(() -> server_2.locateQueue("sometest") != null);

      server_2.stop();

      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");

      amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT);
      amqpConnection.addElement(new AMQPMirrorBrokerConnectionElement());
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server.start();
      Assert.assertTrue(server.locateQueue("sometest") == null);
      Assert.assertTrue(server.locateQueue("ToBeGone") != null);
      Wait.assertTrue(server::isActive);
      server_2.start();
      // if this does not succeed the catch up did not arrive at the other server
      Wait.assertTrue(() -> server.locateQueue("sometest") != null);
      Wait.assertTrue(() -> server.locateQueue("ToBeGone") == null);
      server_2.stop();
      server.stop();
   }

   @Test
   public void testReplicaLargeMessages() throws Exception {
      replicaTest(true, true, false, false, false, false, false);
   }

   @Test
   public void testReplicaLargeMessagesPagingEverywhere() throws Exception {
      replicaTest(true, true, true, true, false, false, false);
   }

   @Test
   public void testReplica() throws Exception {
      replicaTest(false, true, false, false, false, false, false);
   }

   @Test
   public void testReplicaRestartBrokerConnection() throws Exception {
      replicaTest(false, true, false, false, false, false, true);
   }

   @Test
   public void testReplicaRestart() throws Exception {
      replicaTest(false, true, false, false, false, true, false);
   }

   @Test
   public void testReplicaDeferredStart() throws Exception {
      replicaTest(false, true, false, false, true, false, false);
   }

   @Test
   public void testReplicaCopyOnly() throws Exception {
      replicaTest(false, false, false, false, false, false, false);
   }

   @Test
   public void testReplicaPagedTarget() throws Exception {
      replicaTest(false, true, true, false, false, false, false);
   }

   @Test
   public void testReplicaPagingEverywhere() throws Exception {
      replicaTest(false, true, true, true, false, false, false);
   }

   private String getText(boolean large, int i) {
      if (!large) {
         return "Text " + i;
      } else {
         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < 110 * 1024) {
            buffer.append("Text " + i + " ");
         }
         return buffer.toString();
      }
   }

   /**
    * This test is validating that annotations sent to the original broker are not translated to the receiving side.
    * Also annotations could eventually damage the body if the broker did not take that into consideration.
    * So, this test is sending delivery annotations on messages.
    * @throws Exception
    */
   @Test
   public void testLargeMessagesWithDeliveryAnnotations() throws Exception {
      server.setIdentity("targetServer");
      server.start();
      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(-1).setRetryInterval(100);
      AMQPMirrorBrokerConnectionElement replica = new AMQPMirrorBrokerConnectionElement().setMessageAcknowledgements(true);
      amqpConnection.addElement(replica);
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      int NUMBER_OF_MESSAGES = 20;

      server_2.start();
      Wait.assertTrue(server_2::isStarted);

      // We create the address to avoid auto delete on the queue
      server_2.addAddressInfo(new AddressInfo(getQueueName()).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false));
      server_2.createQueue(new QueueConfiguration(getQueueName()).setRoutingType(RoutingType.ANYCAST).setAddress(getQueueName()).setAutoCreated(false));

      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ222214"));

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = locateQueue(server_2, getQueueName());
      final Queue queueViewReplica = locateQueue(server_2, getQueueName());

      { // sender
         AmqpClient client = new AmqpClient(new URI("tcp://localhost:" + AMQP_PORT_2), null, null);
         AmqpConnection connection = addConnection(client.connect());
         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createSender(getQueueName());

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            AmqpMessage message = new AmqpMessage();
            message.setDeliveryAnnotation("gone", "test");
            message.setText(getText(true, i));
            sender.send(message);
         }
         sender.close();
         connection.close();
      }

      Wait.assertEquals(NUMBER_OF_MESSAGES, queueView::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queueViewReplica::getMessageCount);

      { // receiver on replica
         AmqpClient client = new AmqpClient(new URI("tcp://localhost:" + AMQP_PORT), null, null);
         AmqpConnection connection = addConnection(client.connect());
         AmqpSession session = connection.createSession();
         // Now try and get the message

         AmqpReceiver receiver = session.createReceiver(getQueueName());
         receiver.flow(NUMBER_OF_MESSAGES + 1);
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(received);
            Assert.assertEquals(getText(true, i), received.getText());
            Assert.assertNull(received.getDeliveryAnnotation("gone"));
         }
         Assert.assertNull(receiver.receiveNoWait());

         connection.close();
      }
   }


   /** This is setting delivery annotations and sending messages with no address.
    * The broker should know how to deal with the annotations and no address on the message. */
   @Test
   public void testNoAddressWithAnnotations() throws Exception {
      server.setIdentity("targetServer");
      server.start();
      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(-1).setRetryInterval(100);
      AMQPMirrorBrokerConnectionElement replica = new AMQPMirrorBrokerConnectionElement().setMessageAcknowledgements(true);
      amqpConnection.addElement(replica);
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      int NUMBER_OF_MESSAGES = 20;

      server_2.start();
      Wait.assertTrue(server_2::isStarted);

      // We create the address to avoid auto delete on the queue
      server_2.addAddressInfo(new AddressInfo(getQueueName()).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false));
      server_2.createQueue(new QueueConfiguration(getQueueName()).setRoutingType(RoutingType.ANYCAST).setAddress(getQueueName()).setAutoCreated(false));

      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ222214"));

      { // sender
         AmqpClient client = new AmqpClient(new URI("tcp://localhost:" + AMQP_PORT_2), null, null);
         AmqpConnection connection = addConnection(client.connect());
         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createSender(getQueueName());

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            AmqpMessage message = new AmqpMessage();
            message.setDeliveryAnnotation("gone", "test");
            message.setText(getText(false, i));
            sender.send(message);
         }
         sender.close();
         connection.close();
      }

      { // receiver on replica
         AmqpClient client = new AmqpClient(new URI("tcp://localhost:" + AMQP_PORT), null, null);
         AmqpConnection connection = addConnection(client.connect());
         AmqpSession session = connection.createSession();
         // Now try and get the message

         AmqpReceiver receiver = session.createReceiver(getQueueName());
         receiver.flow(NUMBER_OF_MESSAGES + 1);
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(received);
            Assert.assertEquals(getText(false, i), received.getText());
            Assert.assertNull(received.getDeliveryAnnotation("gone"));
         }
         Assert.assertNull(receiver.receiveNoWait());

         connection.close();
      }
   }

   @Test
   public void testRouteSurviving() throws Exception {
      testRouteSurvivor(false);
   }

   @Test
   public void testRouteSurvivingStop() throws Exception {
      testRouteSurvivor(true);
   }


   private void testRouteSurvivor(boolean server1Stopped) throws Exception {
      if (!server1Stopped) {
         server.start();
      }
      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");
      server_2.getConfiguration().setName("thisone");

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("OtherSide", "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(-1).setRetryInterval(100);
      AMQPMirrorBrokerConnectionElement replica = new AMQPMirrorBrokerConnectionElement().setSourceMirrorAddress("TheSource");
      amqpConnection.addElement(replica);
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      server_2.start();

      // We create the address to avoid auto delete on the queue
      server_2.addAddressInfo(new AddressInfo(getQueueName()).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false));
      server_2.createQueue(new QueueConfiguration(getQueueName()).setRoutingType(RoutingType.ANYCAST).setAddress(getQueueName()).setAutoCreated(false));

      int NUMBER_OF_MESSAGES = 200;

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT_2);
      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         producer.send(session.createTextMessage("i=" + i));
      }

      connection.close();

      {
         if (!server1Stopped) {
            Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);
            Queue queueServer1 = server.locateQueue(getQueueName());
            Wait.assertEquals(NUMBER_OF_MESSAGES, queueServer1::getMessageCount);
         }
         Wait.assertTrue(() -> server_2.locateQueue(getQueueName()) != null);
         Queue queueServer2 = server_2.locateQueue(getQueueName());
         Wait.assertEquals(NUMBER_OF_MESSAGES, queueServer2::getMessageCount);
      }

      if (!server1Stopped) {
         server.stop();
      }
      server_2.stop();

      server.start();
      server_2.start();


      Wait.assertTrue(() -> server.locateQueue(getQueueName()) != null);
      Wait.assertTrue(() -> server_2.locateQueue(getQueueName()) != null);
      Queue queueServer1 = server.locateQueue(getQueueName());
      Queue queueServer2 = server_2.locateQueue(getQueueName());

      Wait.assertEquals(NUMBER_OF_MESSAGES, queueServer1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queueServer2::getMessageCount);
   }


   private void replicaTest(boolean largeMessage,
                            boolean acks,
                            boolean pagingTarget,
                            boolean pagingSource,
                            boolean deferredStart,
                            boolean restartAndDisconnect,
                            boolean restartBrokerConnection) throws Exception {

      String brokerConnectionName = "brokerConnectionName:" + UUIDGenerator.getInstance().generateStringUUID();
      server.setIdentity("targetServer");
      if (deferredStart) {
         server.stop();
      } else {
         server.start();
      }
      server_2 = createServer(AMQP_PORT_2, false);
      server_2.setIdentity("server_2");
      server_2.getConfiguration().setName("thisone");

      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(brokerConnectionName, "tcp://localhost:" + AMQP_PORT).setReconnectAttempts(-1).setRetryInterval(100);
      AMQPMirrorBrokerConnectionElement replica = new AMQPMirrorBrokerConnectionElement().setMessageAcknowledgements(acks);
      amqpConnection.addElement(replica);
      server_2.getConfiguration().addAMQPConnection(amqpConnection);

      int NUMBER_OF_MESSAGES = 200;

      server_2.start();
      Wait.assertTrue(server_2::isStarted);

      // We create the address to avoid auto delete on the queue
      server_2.addAddressInfo(new AddressInfo(getQueueName()).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false));
      server_2.createQueue(new QueueConfiguration(getQueueName()).setRoutingType(RoutingType.ANYCAST).setAddress(getQueueName()).setAutoCreated(false));

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT_2);
      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      if (!deferredStart) {
         Queue queueOnServer1 = locateQueue(server, getQueueName());

         if (pagingTarget) {
            queueOnServer1.getPagingStore().startPaging();
         }
      }

      if (pagingSource) {
         Queue queueOnServer2 = server_2.locateQueue(getQueueName());
         queueOnServer2.getPagingStore().startPaging();
      }

      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ222214"));


      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         Message message = session.createTextMessage(getText(largeMessage, i));
         message.setIntProperty("i", i);
         producer.send(message);
      }

      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ222214"));

      Queue queueOnServer1;
      if (deferredStart) {
         Thread.sleep(TIME_BEFORE_RESTART);
         server.start();
         Wait.assertTrue(server::isActive);
         queueOnServer1 = locateQueue(server, getQueueName());
         if (pagingTarget) {
            queueOnServer1.getPagingStore().startPaging();
         }
      } else {
         queueOnServer1 = locateQueue(server, getQueueName());
      }
      Queue snfreplica = server_2.locateQueue(replica.getSourceMirrorAddress());

      Assert.assertNotNull(snfreplica);

      Wait.assertEquals(0, snfreplica::getMessageCount);

      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer1::getMessageCount);
      Queue queueOnServer2 = locateQueue(server_2, getQueueName());
      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queueOnServer2::getMessageCount);

      if (restartBrokerConnection) {
         // stop and start the broker connection, making sure we wouldn't duplicate the mirror
         server_2.stopBrokerConnection(brokerConnectionName);
         Thread.sleep(1000);
         server_2.startBrokerConnection(brokerConnectionName);
      }

      if (pagingTarget) {
         assertTrue(queueOnServer1.getPagingStore().isPaging());
      }

      if (acks) {
         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES / 2 - 1, AMQP_PORT_2, false);
         // Replica is async, so we need to wait acks to arrive before we finish consuming there
         Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queueOnServer1::getMessageCount);
         // we consume on replica, as half the messages were acked
         consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT, true); // We consume on both servers as this is currently replicated
         consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT_2, false);

         if (largeMessage) {
            validateNoFilesOnLargeDir(server.getConfiguration().getLargeMessagesDirectory(), 0);
            //validateNoFilesOnLargeDir(server_2.getConfiguration().getLargeMessagesDirectory(), 50); // we kept half of the messages
         }
      } else {

         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES - 1, AMQP_PORT_2, true);
         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES - 1, AMQP_PORT, true);
         if (largeMessage) {
            validateNoFilesOnLargeDir(server.getConfiguration().getLargeMessagesDirectory(), 0);
            validateNoFilesOnLargeDir(server_2.getConfiguration().getLargeMessagesDirectory(), 0); // we kept half of the messages
         }
      }

      if (restartAndDisconnect) {
         server.stop();

         Thread.sleep(TIME_BEFORE_RESTART);

         server.start();
         Wait.assertTrue(server::isActive);

         consumeMessages(largeMessage, 0, -1, AMQP_PORT_2, true);
         consumeMessages(largeMessage, 0, -1, AMQP_PORT, true);
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            Message message = session.createTextMessage(getText(largeMessage, i));
            message.setIntProperty("i", i);
            producer.send(message);
         }

         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES - 1, AMQP_PORT, true);
         consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES - 1, AMQP_PORT_2, true);
      }
   }

   @Test
   public void testDualStandardRestartBrokerConnection() throws Exception {
      dualReplica(false, false, false, true);
   }

   @Test
   public void testDualStandard() throws Exception {
      dualReplica(false, false, false, false);
   }

   @Test
   public void testDualRegularPagedTargets() throws Exception {
      dualReplica(false, false, true, false);
   }

   @Test
   public void testDualRegularPagedEverything() throws Exception {
      dualReplica(false, true, true, false);
   }

   @Test
   public void testDualRegularLarge() throws Exception {
      dualReplica(true, false, false, false);
   }

   public Queue locateQueue(ActiveMQServer server, String queueName) throws Exception {
      Wait.waitFor(() -> server.locateQueue(queueName) != null);
      return server.locateQueue(queueName);
   }

   private void dualReplica(boolean largeMessage, boolean pagingSource, boolean pagingTarget, boolean restartBC) throws Exception {
      server.setIdentity("server_1");
      server.start();

      ActiveMQServer server_3 = createServer(AMQP_PORT_3, false);
      server_3.setIdentity("server_3");
      server_3.start();
      Wait.assertTrue(server_3::isStarted);

      ConnectionFactory factory_3 = CFUtil.createConnectionFactory("amqp", "tcp://localhost:" + AMQP_PORT_3);
      factory_3.createConnection().close();

      server_2 = createServer(AMQP_PORT_2, false);

      String brokerConnectionOne = "brokerConnection1:" + UUIDGenerator.getInstance().generateStringUUID();
      String brokerConnectionTwo = "brokerConnection2:" + UUIDGenerator.getInstance().generateStringUUID();

      AMQPBrokerConnectConfiguration amqpConnection1 = new AMQPBrokerConnectConfiguration(brokerConnectionOne, "tcp://localhost:" + AMQP_PORT);
      AMQPMirrorBrokerConnectionElement replica1 = new AMQPMirrorBrokerConnectionElement().setType(AMQPBrokerConnectionAddressType.MIRROR);
      amqpConnection1.addElement(replica1);
      server_2.getConfiguration().addAMQPConnection(amqpConnection1);

      AMQPBrokerConnectConfiguration amqpConnection3 = new AMQPBrokerConnectConfiguration(brokerConnectionTwo, "tcp://localhost:" + AMQP_PORT_3);
      AMQPMirrorBrokerConnectionElement replica2 = new AMQPMirrorBrokerConnectionElement().setType(AMQPBrokerConnectionAddressType.MIRROR);
      amqpConnection3.addElement(replica2);
      server_2.getConfiguration().addAMQPConnection(amqpConnection3);

      int NUMBER_OF_MESSAGES = 200;

      server_2.start();
      Wait.assertTrue(server_2::isStarted);

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT_2);
      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(session.createQueue(getQueueName()));
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      Queue queue_server_2 = locateQueue(server_2, getQueueName());
      Queue queue_server_1 = locateQueue(server, getQueueName());
      Queue queue_server_3 = locateQueue(server_3, getQueueName());

      if (pagingSource) {
         queue_server_2.getPagingStore().startPaging();
      }

      if (pagingTarget) {
         queue_server_1.getPagingStore().startPaging();
         queue_server_3.getPagingStore().startPaging();
      }

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         Message message = session.createTextMessage(getText(largeMessage, i));
         message.setIntProperty("i", i);
         producer.send(message);

         if (i == NUMBER_OF_MESSAGES / 2) {
            if (restartBC) {
               // half we restart it
               Wait.assertEquals(NUMBER_OF_MESSAGES / 2 + 1, queue_server_2::getMessageCount);
               Wait.assertEquals(NUMBER_OF_MESSAGES / 2 + 1, queue_server_3::getMessageCount);
               Wait.assertEquals(NUMBER_OF_MESSAGES / 2 + 1, queue_server_1::getMessageCount);
               server_2.stopBrokerConnection(brokerConnectionOne);
               server_2.stopBrokerConnection(brokerConnectionTwo);
               Thread.sleep(1000);
               server_2.startBrokerConnection(brokerConnectionOne);
               server_2.startBrokerConnection(brokerConnectionTwo);
            }
         }
      }

      Wait.assertEquals(NUMBER_OF_MESSAGES, queue_server_2::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queue_server_3::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, queue_server_1::getMessageCount);

      Queue replica1Queue = server_2.locateQueue(replica1.getSourceMirrorAddress());
      Queue replica2Queue = server_2.locateQueue(replica2.getSourceMirrorAddress());

      Wait.assertEquals(0L, replica2Queue.getPagingStore()::getAddressSize, 1000, 100);
      Wait.assertEquals(0L, replica1Queue.getPagingStore()::getAddressSize, 1000, 100);

      if (pagingTarget) {
         Assert.assertTrue(queue_server_1.getPagingStore().isPaging());
         Assert.assertTrue(queue_server_3.getPagingStore().isPaging());
      }

      if (pagingSource) {
         Assert.assertTrue(queue_server_2.getPagingStore().isPaging());
      }

      consumeMessages(largeMessage, 0, NUMBER_OF_MESSAGES / 2 - 1, AMQP_PORT_2, false);

      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queue_server_1::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queue_server_2::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queue_server_3::getMessageCount);

      // Replica is async, so we need to wait acks to arrive before we finish consuming there
      Wait.assertEquals(NUMBER_OF_MESSAGES / 2, queue_server_1::getMessageCount);

      consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT, true); // We consume on both servers as this is currently replicated
      consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT_3, true); // We consume on both servers as this is currently replicated
      consumeMessages(largeMessage, NUMBER_OF_MESSAGES / 2, NUMBER_OF_MESSAGES - 1, AMQP_PORT_2, true); // We consume on both servers as this is currently replicated

      validateNoFilesOnLargeDir(server.getConfiguration().getLargeMessagesDirectory(), 0);
      validateNoFilesOnLargeDir(server_3.getConfiguration().getLargeMessagesDirectory(), 0);
      validateNoFilesOnLargeDir(server_2.getConfiguration().getLargeMessagesDirectory(), 0);

   }

   /**
    * this might be helpful for debugging
    */
   private void printMessages(String printInfo, Queue queue) {
      System.out.println("*******************************************************************************************************************************");
      System.out.println(printInfo);
      System.out.println();
      LinkedListIterator<MessageReference> referencesIterator = queue.browserIterator();
      while (referencesIterator.hasNext()) {
         System.out.println("message " + referencesIterator.next().getMessage());
      }
      referencesIterator.close();
      System.out.println("*******************************************************************************************************************************");
   }

   private void consumeMessages(boolean largeMessage,
                                int START_ID,
                                int LAST_ID,
                                int port,
                                boolean assertNull) throws JMSException {
      ConnectionFactory cf = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + port);
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      conn.start();

      MessageConsumer consumer = sess.createConsumer(sess.createQueue(getQueueName()));
      for (int i = START_ID; i <= LAST_ID; i++) {
         Message message = consumer.receive(3000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getIntProperty("i"));
         if (message instanceof TextMessage) {
            Assert.assertEquals(getText(largeMessage, i), ((TextMessage) message).getText());
         }
      }
      if (assertNull) {
         Assert.assertNull(consumer.receiveNoWait());
      }
      conn.close();
   }

}
