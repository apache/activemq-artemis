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
package org.apache.activemq.artemis.tests.integration.proton;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.QueueBrowser;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.VersionLoader;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assume.assumeTrue;
import org.proton.plug.AMQPClientConnectionContext;
import org.proton.plug.AMQPClientReceiverContext;
import org.proton.plug.AMQPClientSenderContext;
import org.proton.plug.AMQPClientSessionContext;
import org.proton.plug.context.server.ProtonServerReceiverContext;
import org.proton.plug.test.Constants;
import org.proton.plug.test.minimalclient.SimpleAMQPConnector;

@RunWith(Parameterized.class)
public class ProtonTest extends ActiveMQTestBase {

   private static final String amqpConnectionUri = "amqp://localhost:5672";

   private static final String tcpAmqpConnectionUri = "tcp://localhost:5672";

   private static final String userName = "guest";

   private static final String password = "guest";


   private static final String brokerName = "my-broker";

   private static final long maxSizeBytes = 1 * 1024 * 1024;

   private static final long maxSizeBytesRejectThreshold = 2 * 1024 * 1024;

   private int messagesSent = 0;

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameterized.Parameters(name = "{0}")
   public static Collection getParameters() {

      // these 3 are for comparison
      return Arrays.asList(new Object[][]{{"AMQP", 0}, {"ActiveMQ (InVM)", 1}, {"ActiveMQ (Netty)", 2}, {"AMQP_ANONYMOUS", 3}});
   }

   ConnectionFactory factory;

   private final int protocol;

   public ProtonTest(String name, int protocol) {
      this.coreAddress = "jms.queue.exampleQueue";
      this.protocol = protocol;
      if (protocol == 0 || protocol == 3) {
         this.address = coreAddress;
      }
      else {
         this.address = "exampleQueue";
      }
   }

   private ActiveMQServer server;
   private final String coreAddress;
   private final String address;
   private Connection connection;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      disableCheckThread();

      server = this.createServer(true, true);
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, "5672");
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "AMQP");
      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      server.getConfiguration().getAcceptorConfigurations().add(transportConfiguration);
      server.getConfiguration().setName(brokerName);

      // Default Page
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      server.getConfiguration().getAddressesSettings().put("#", addressSettings);

      server.start();
      server.createQueue(new SimpleString(coreAddress), new SimpleString(coreAddress), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "1"), new SimpleString(coreAddress + "1"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "2"), new SimpleString(coreAddress + "2"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "3"), new SimpleString(coreAddress + "3"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "4"), new SimpleString(coreAddress + "4"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "5"), new SimpleString(coreAddress + "5"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "6"), new SimpleString(coreAddress + "6"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "7"), new SimpleString(coreAddress + "7"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "8"), new SimpleString(coreAddress + "8"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "9"), new SimpleString(coreAddress + "9"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "10"), new SimpleString(coreAddress + "10"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic"), new SimpleString("amqp_testtopic"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "1"), new SimpleString("amqp_testtopic" + "1"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "2"), new SimpleString("amqp_testtopic" + "2"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "3"), new SimpleString("amqp_testtopic" + "3"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "4"), new SimpleString("amqp_testtopic" + "4"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "5"), new SimpleString("amqp_testtopic" + "5"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "6"), new SimpleString("amqp_testtopic" + "6"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "7"), new SimpleString("amqp_testtopic" + "7"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "8"), new SimpleString("amqp_testtopic" + "8"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "9"), new SimpleString("amqp_testtopic" + "9"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "10"), new SimpleString("amqp_testtopic" + "10"), null, true, false);
      connection = createConnection();

   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         Thread.sleep(250);
         if (connection != null) {
            connection.close();
         }

         server.stop();
      }
      finally {
         super.tearDown();
      }
   }

   @Test
   public void testBrokerContainerId() throws Exception {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol

      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      try {
         assertTrue(brokerName.equals(amqpConnection.getEndpoint().getRemoteContainer()));
      }
      finally {
         amqpConnection.close();
      }
   }

   @Test
   public void testBrokerConnectionProperties() throws Exception {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol

      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      try {
         Map<Symbol, Object> properties = amqpConnection.getEndpoint().getRemoteProperties();
         assertTrue(properties != null);
         if (properties != null) {
            assertTrue("apache-activemq-artemis".equals(properties.get(Symbol.valueOf("product"))));
            assertTrue(VersionLoader.getVersion().getFullVersion().equals(properties.get(Symbol.valueOf("version"))));
         }
      }
      finally {
         amqpConnection.close();
      }
   }

   @Test
   public void testCreditsAreAllocatedOnlyOnceOnLinkCreate() throws Exception {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol

      // Only allow 1 credit to be submitted at a time.
      Field maxCreditAllocation = ProtonServerReceiverContext.class.getDeclaredField("maxCreditAllocation");
      maxCreditAllocation.setAccessible(true);
      int originalMaxCreditAllocation = maxCreditAllocation.getInt(null);
      maxCreditAllocation.setInt(null, 1);

      String destinationAddress = address + 1;
      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      try {
         AmqpSession session = amqpConnection.createSession();
         AmqpSender sender = session.createSender(destinationAddress);
         assertTrue(sender.getSender().getCredit() == 1);
      }
      finally {
         amqpConnection.close();
         maxCreditAllocation.setInt(null, originalMaxCreditAllocation);
      }
   }

   @Test
   public void testTemporaryQueue() throws Throwable {

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TemporaryQueue queue = session.createTemporaryQueue();
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      p.send(message);

      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      message = (TextMessage) cons.receive(5000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testCommitProducer() throws Throwable {

      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      javax.jms.Queue queue = createQueue(address);
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("Message:" + i);
         p.send(message);
      }
      session.commit();
      session.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();
      Assert.assertEquals(q.getMessageCount(), 10);
   }

   @Test
   public void testRollbackProducer() throws Throwable {

      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      javax.jms.Queue queue = createQueue(address);
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("Message:" + i);
         p.send(message);
      }
      session.rollback();
      session.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();
      Assert.assertEquals(q.getMessageCount(), 0);
   }

   @Test
   public void testCommitConsumer() throws Throwable {

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = createQueue(address);
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("Message:" + i);
         p.send(message);
      }
      session.close();

      session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < 10; i++) {
         TextMessage message = (TextMessage) cons.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals("Message:" + i, message.getText());
      }
      session.commit();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();
      Assert.assertEquals(q.getMessageCount(), 0);
   }


   @Test
   public void testRollbackConsumer() throws Throwable {

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = createQueue(address);
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("Message:" + i);
         p.send(message);
      }
      session.close();

      session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < 10; i++) {
         TextMessage message = (TextMessage) cons.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals("Message:" + i, message.getText());
      }
      session.rollback();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();
      Assert.assertEquals(q.getMessageCount(), 10);
   }

   @Test
   public void testResourceLimitExceptionOnAddressFull() throws Exception {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol
      setAddressFullBlockPolicy();
      String destinationAddress = address + 1;
      fillAddress(destinationAddress);

      long addressSize = server.getPagingManager().getPageStore(new SimpleString(destinationAddress)).getAddressSize();
      assertTrue(addressSize >= maxSizeBytesRejectThreshold);
   }

   @Test
   public void testAddressIsBlockedForOtherProdudersWhenFull() throws Exception {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol
      setAddressFullBlockPolicy();

      String destinationAddress = address + 1;
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination d = session.createQueue(destinationAddress);
      MessageProducer p = session.createProducer(d);

      fillAddress(destinationAddress);

      Exception e = null;
      try {
         p.send(session.createBytesMessage());
      }
      catch (ResourceAllocationException rae) {
         e = rae;
      }
      assertTrue(e instanceof ResourceAllocationException);
      assertTrue(e.getMessage().contains("resource-limit-exceeded"));

      long addressSize = server.getPagingManager().getPageStore(new SimpleString(destinationAddress)).getAddressSize();
      assertTrue(addressSize >= maxSizeBytesRejectThreshold);
   }

   @Test
   public void testCreditsAreNotAllocatedWhenAddressIsFull() throws Exception {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol
      setAddressFullBlockPolicy();

      // Only allow 1 credit to be submitted at a time.
      Field maxCreditAllocation = ProtonServerReceiverContext.class.getDeclaredField("maxCreditAllocation");
      maxCreditAllocation.setAccessible(true);
      int originalMaxCreditAllocation = maxCreditAllocation.getInt(null);
      maxCreditAllocation.setInt(null, 1);

      String destinationAddress = address + 1;
      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      try {
         AmqpSession session = amqpConnection.createSession();
         AmqpSender sender = session.createSender(destinationAddress);

         // Use blocking send to ensure buffered messages do not interfere with credit.
         sender.setSendTimeout(-1);
         sendUntilFull(sender);

         // This should be -1. A single message is buffered in the client, and 0 credit has been allocated.
         assertTrue(sender.getSender().getCredit() == -1);

         long addressSize = server.getPagingManager().getPageStore(new SimpleString(destinationAddress)).getAddressSize();
         assertTrue(addressSize >= maxSizeBytes && addressSize <= maxSizeBytesRejectThreshold);
      }
      finally {
         amqpConnection.close();
         maxCreditAllocation.setInt(null, originalMaxCreditAllocation);
      }
   }

   @Test
   public void testCreditsAreRefreshedWhenAddressIsUnblocked() throws Exception {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol
      setAddressFullBlockPolicy();

      String destinationAddress = address + 1;
      fillAddress(destinationAddress);

      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = amqpConnection = client.connect();
      try {
         AmqpSession session = amqpConnection.createSession();
         AmqpSender sender = session.createSender(destinationAddress);

         // Wait for a potential flow frame.
         Thread.sleep(500);
         assertEquals(0, sender.getSender().getCredit());

         // Empty Address except for 1 message used later.
         AmqpReceiver receiver = session.createReceiver(destinationAddress);
         receiver.flow(100);

         AmqpMessage m;
         for (int i = 0; i < messagesSent - 1; i++) {
            m = receiver.receive();
            m.accept();
         }

         // Wait for address to unblock and flow frame to arrive
         Thread.sleep(500);

         assertTrue(sender.getSender().getCredit() >= 0);
      }
      finally {
         amqpConnection.close();
      }
   }

   @Test
   public void testNewLinkAttachAreNotAllocatedCreditsWhenAddressIsBlocked() throws Exception {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol
      setAddressFullBlockPolicy();

      fillAddress(address + 1);
      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      try {
         AmqpSession session = amqpConnection.createSession();
         AmqpSender sender = session.createSender(address + 1);
         // Wait for a potential flow frame.
         Thread.sleep(1000);
         assertEquals(0, sender.getSender().getCredit());
      }
      finally {
         amqpConnection.close();
      }
   }

   @Test
   public void testTxIsRolledBackOnRejectedPreSettledMessage() throws Throwable {
      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol
      setAddressFullBlockPolicy();

      // Create the link attach before filling the address to ensure the link is allocated credit.
      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();

      AmqpSession session = amqpConnection.createSession();
      AmqpSender sender = session.createSender(address);
      sender.setPresettle(true);

      fillAddress(address);

      final AmqpMessage message = new AmqpMessage();
      byte[] payload = new byte[50 * 1024];
      message.setBytes(payload);

      Exception expectedException = null;
      try {
         session.begin();
         sender.send(message);
         session.commit();
      }
      catch (Exception e) {
         expectedException = e;
      }
      finally {
         amqpConnection.close();
      }

      assertNotNull(expectedException);
      assertTrue(expectedException.getMessage().contains("resource-limit-exceeded"));
      assertTrue(expectedException.getMessage().contains("Address is full: " + address));
   }

   /**
    * Fills an address.  Careful when using this method.  Only use when rejected messages are switched on.
    * @param address
    * @return
    * @throws Exception
    */
   private void fillAddress(String address) throws Exception {
      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      Exception exception = null;
      try {
         AmqpSession session = amqpConnection.createSession();
         AmqpSender sender = session.createSender(address);
         sendUntilFull(sender);
      }
      catch (Exception e) {
         exception = e;
      }
      finally {
         amqpConnection.close();
      }

      // Should receive a rejected error
      assertNotNull(exception);
      assertTrue(exception.getMessage().contains("amqp:resource-limit-exceeded"));
   }

   private void sendUntilFull(final AmqpSender sender) throws Exception {
      final AmqpMessage message = new AmqpMessage();
      byte[] payload = new byte[50 * 1024];
      message.setBytes(payload);

      final int maxMessages = 50;
      final AtomicInteger sentMessages = new AtomicInteger(0);
      final Exception[] errors = new Exception[1];
      final CountDownLatch timeout = new CountDownLatch(1);

      Runnable sendMessages = new Runnable() {
         @Override
         public void run() {
            try {
               for (int i = 0; i < maxMessages; i++) {
                  sender.send(message);
                  sentMessages.getAndIncrement();
               }
               timeout.countDown();
            }
            catch (IOException e) {
               errors[0] = e;
            }
         }
      };

      Thread t = new Thread(sendMessages);
      t.start();

      timeout.await(5, TimeUnit.SECONDS);

      messagesSent = sentMessages.get();
      if (errors[0] != null) {
         throw errors[0];
      }
   }

   @Test
   public void testLinkDetatchErrorIsCorrectWhenQueueDoesNotExists() throws Exception {
      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      AmqpSession session = amqpConnection.createSession();

      Exception expectedException = null;
      try {
         session.createSender("AnAddressThatDoesNotExist");
      }
      catch (Exception e) {
         expectedException = e;
      }

      assertNotNull(expectedException);
      assertTrue(expectedException.getMessage().contains("amqp:not-found"));
      assertTrue(expectedException.getMessage().contains("target address does not exist"));
   }

   @Test
   public void testSendingAndReceivingToQueueWithDifferentAddressAndQueueName() throws Exception {

      String queueName = "TestQueueName";
      String address = "TestAddress";

      server.createQueue(new SimpleString(address), new SimpleString(queueName), null, true, false);

      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      AmqpSession session = amqpConnection.createSession();
      AmqpSender sender = session.createSender(address);
      AmqpReceiver receiver = session.createReceiver(queueName);
      receiver.flow(1);

      AmqpMessage message = new AmqpMessage();
      message.setText("TestPayload");
      sender.send(message);

      AmqpMessage receivedMessage = receiver.receive();
      assertNotNull(receivedMessage);
   }

   @Test
   public void testManagementQueryOverAMQP() throws Throwable {
      assumeTrue(protocol == 0 || protocol == 3); // Only run this test for AMQP protocol

      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);
      AmqpConnection amqpConnection = client.connect();
      try {
         String destinationAddress = address + 1;
         AmqpSession session = amqpConnection.createSession();
         AmqpSender sender = session.createSender("jms.queue.activemq.management");
         AmqpReceiver receiver = session.createReceiver(destinationAddress);
         receiver.flow(10);

         //create request message for getQueueNames query
         AmqpMessage request = new AmqpMessage();
         request.setApplicationProperty("_AMQ_ResourceName", "core.server");
         request.setApplicationProperty("_AMQ_OperationName", "getQueueNames");
         request.setApplicationProperty("JMSReplyTo", destinationAddress);
         request.setText("[]");

         sender.send(request);
         AmqpMessage response = receiver.receive();
         assertNotNull(response);
         Object section = response.getWrappedMessage().getBody();
         assertTrue(section instanceof AmqpValue);
         Object value = ((AmqpValue) section).getValue();
         assertTrue(value instanceof String);
         assertTrue(((String) value).length() > 0);
         assertTrue(((String) value).contains(destinationAddress));
         response.accept();
      }
      finally {
         amqpConnection.close();
      }
   }

   @Test
   public void testReplyTo() throws Throwable {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TemporaryQueue queue = session.createTemporaryQueue();
      MessageProducer p = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      message.setJMSReplyTo(createQueue(address));
      p.send(message);

      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      message = (TextMessage) cons.receive(5000);
      Destination jmsReplyTo = message.getJMSReplyTo();
      Assert.assertNotNull(jmsReplyTo);
      Assert.assertNotNull(message);
   }

   @Test
   public void testReplyToNonJMS() throws Throwable {

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TemporaryQueue queue = session.createTemporaryQueue();
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      message.setJMSReplyTo(createQueue("someaddress"));
      p.send(message);

      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      message = (TextMessage) cons.receive(5000);
      Destination jmsReplyTo = message.getJMSReplyTo();
      Assert.assertNotNull(jmsReplyTo);
      Assert.assertNotNull(message);

   }

   /*
   // Uncomment testLoopBrowser to validate the hunging on the test
   @Test
   public void testLoopBrowser() throws Throwable {
      for (int i = 0 ; i < 1000; i++) {
         System.out.println("#test " + i);
         testBrowser();
         tearDown();
         setUp();
      }
   } */

   /**
    * This test eventually fails because of: https://issues.apache.org/jira/browse/QPID-4901
    *
    * @throws Throwable
    */
   //@Test // TODO: re-enable this when we can get a version free of QPID-4901 bug
   public void testBrowser() throws Throwable {

      boolean success = false;

      for (int i = 0; i < 10; i++) {
         // As this test was hunging, we added a protection here to fail it instead.
         // it seems something on the qpid client, so this failure belongs to them and we can ignore it on
         // our side (ActiveMQ)
         success = runWithTimeout(new RunnerWithEX() {
            @Override
            public void run() throws Throwable {
               int numMessages = 50;
               javax.jms.Queue queue = createQueue(address);
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               MessageProducer p = session.createProducer(queue);
               for (int i = 0; i < numMessages; i++) {
                  TextMessage message = session.createTextMessage();
                  message.setText("msg:" + i);
                  p.send(message);
               }

               connection.close();
               Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();

               connection = createConnection();
               session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

               QueueBrowser browser = session.createBrowser(queue);
               Enumeration enumeration = browser.getEnumeration();
               int count = 0;
               while (enumeration.hasMoreElements()) {
                  Message msg = (Message) enumeration.nextElement();
                  Assert.assertNotNull("" + count, msg);
                  Assert.assertTrue("" + msg, msg instanceof TextMessage);
                  String text = ((TextMessage) msg).getText();
                  Assert.assertEquals(text, "msg:" + count++);
               }
               Assert.assertEquals(count, numMessages);
               connection.close();
               Assert.assertEquals(getMessageCount(q), numMessages);
            }
         }, 5000);

         if (success) {
            break;
         }
         else {
            System.err.println("Had to make it fail!!!");
            tearDown();
            setUp();
         }
      }

      // There is a bug on the qpid client library currently, we can expect having to interrupt the thread on browsers.
      // but we can't have it on 10 iterations... something must be broken if that's the case
      Assert.assertTrue("Test had to interrupt on all occasions.. this is beyond the expected for the test", success);
   }

   @Test
   public void testReceiveImmediate() throws Exception {
      testReceiveImmediate(1000, 1000);
   }

   @Test
   public void testReceiveImmediateMoreCredits() throws Exception {
      testReceiveImmediate(1000, 100);
   }

   @Test
   public void testReceiveImmediateMoreMessages() throws Exception {
      testReceiveImmediate(100, 1000);
   }

   public void testReceiveImmediate(int noCredits, int noMessages) throws Exception {

      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = createQueue(address);
      MessageProducer p = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      for (int i = 0; i < noMessages; i++) {
         message.setText("msg:" + i);
         p.send(message);
      }

      SimpleAMQPConnector connector = new SimpleAMQPConnector();
      connector.start();
      AMQPClientConnectionContext clientConnection = connector.connect("127.0.0.1", Constants.PORT);

      clientConnection.clientOpen(null);

      AMQPClientSessionContext csession = clientConnection.createClientSession();
      AMQPClientReceiverContext receiver = csession.createReceiver(address);
      receiver.drain(noCredits);

      int expectedNumberMessages = noCredits > noMessages ? noMessages : noCredits;
      for (int i = 0; i < expectedNumberMessages; i++) {
         ProtonJMessage protonJMessage = receiver.receiveMessage(500, TimeUnit.SECONDS);
         Assert.assertNotNull(protonJMessage);
      }
      ProtonJMessage protonJMessage = receiver.receiveMessage(500, TimeUnit.MILLISECONDS);
      Assert.assertNull(protonJMessage);

      assertFalse(receiver.isDraining());
      if (noCredits >= noMessages) {
         assertEquals(noCredits - noMessages, receiver.drained());
      }
      else {
         assertEquals(0, receiver.drained());
      }
   }

   @Test
   public void testDynamicSenderLink() throws Exception {

      if (protocol != 0 && protocol != 3) return; // Only run this test for AMQP protocol

      SimpleAMQPConnector connector = new SimpleAMQPConnector();
      connector.start();
      AMQPClientConnectionContext clientConnection = connector.connect("127.0.0.1", Constants.PORT);

      clientConnection.clientOpen(null);

      AMQPClientSessionContext csession = clientConnection.createClientSession();
      AMQPClientSenderContext sender = csession.createDynamicSender(false);

      String address = sender.getAddress();

      AMQPClientReceiverContext receiver = csession.createReceiver(address);
      receiver.flow(1);

      // Send one on the dynamic address
      MessageImpl message = (MessageImpl) org.apache.qpid.proton.message.Message.Factory.create();

      Properties props = new Properties();
      Map<Object, Object> map = new HashMap<>();

      map.put("some-property", 1);
      AmqpValue value = new AmqpValue(map);
      message.setBody(value);
      message.setProperties(props);
      sender.send(message);

      ProtonJMessage protonJMessage = receiver.receiveMessage(500, TimeUnit.MILLISECONDS);
      Assert.assertNotNull(protonJMessage);
   }

   @Test
   public void testConnection() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createConsumer(createQueue(address));

      org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(SimpleString.toSimpleString(coreAddress));

      assertEquals(1, serverQueue.getConsumerCount());

      cons.close();

      for (int i = 0; i < 100 && serverQueue.getConsumerCount() != 0; i++) {
         Thread.sleep(500);
      }

      assertEquals(0, serverQueue.getConsumerCount());

      session.close();

   }

   @Test
   public void testMessagesSentTransactional() throws Exception {
      int numMessages = 1000;
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      session.commit();
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();
      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && getMessageCount(q) != numMessages; ) {
         Thread.sleep(1);
      }
      Assert.assertEquals(numMessages, getMessageCount(q));
   }

   @Test
   public void testMessagesSentTransactionalRolledBack() throws Exception {
      int numMessages = 1;
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      session.close();
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();
      Assert.assertEquals(getMessageCount(q), 0);
   }

   @Test
   public void testCancelMessages() throws Exception {
      int numMessages = 10;
      long time = System.currentTimeMillis();
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();

      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && getMessageCount(q) != numMessages; ) {
         Thread.sleep(1);
      }

      Assert.assertEquals(numMessages, getMessageCount(q));
      //now create a new connection and receive
      connection = createConnection();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queue);
      Thread.sleep(100);
      consumer.close();
      connection.close();
      Assert.assertEquals(numMessages, getMessageCount(q));
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testClientAckMessages() throws Exception {
      int numMessages = 10;
      long time = System.currentTimeMillis();
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();

      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && getMessageCount(q) != numMessages; ) {
         Thread.sleep(1);
      }
      Assert.assertEquals(numMessages, getMessageCount(q));
      //now create a new connection and receive
      connection = createConnection();
      session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < numMessages; i++) {
         Message msg = consumer.receive(5000);
         if (msg == null) {
            System.out.println("ProtonTest.testManyMessages");
         }
         Assert.assertNotNull("" + i, msg);
         Assert.assertTrue("" + msg, msg instanceof TextMessage);
         String text = ((TextMessage) msg).getText();
         //System.out.println("text = " + text);
         Assert.assertEquals(text, "msg:" + i);
         msg.acknowledge();
      }

      consumer.close();
      connection.close();

      // Wait for Acks to be processed and message removed from queue.
      Thread.sleep(500);

      Assert.assertEquals(0, getMessageCount(q));
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testMessagesReceivedInParallel() throws Throwable {
      final int numMessages = 50000;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            Connection connectionConsumer = null;
            try {
               // TODO the test may starve if using the same connection (dead lock maybe?)
               connectionConsumer = createConnection();
               //               connectionConsumer = connection;
               connectionConsumer.start();
               Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
               final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

               long n = 0;
               int count = numMessages;
               while (count > 0) {
                  try {
                     if (++n % 1000 == 0) {
                        System.out.println("received " + n + " messages");
                     }

                     Message m = consumer.receive(5000);
                     Assert.assertNotNull("Could not receive message count=" + count + " on consumer", m);
                     count--;
                  }
                  catch (JMSException e) {
                     e.printStackTrace();
                     break;
                  }
               }
            }
            catch (Throwable e) {
               exceptions.add(e);
               e.printStackTrace();
            }
            finally {
               try {
                  // if the createconnecion wasn't commented out
                  if (connectionConsumer != connection) {
                     connectionConsumer.close();
                  }
               }
               catch (Throwable ignored) {
                  // NO OP
               }
            }
         }
      });

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      t.start();

      MessageProducer p = session.createProducer(queue);
      p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      for (int i = 0; i < numMessages; i++) {
         BytesMessage message = session.createBytesMessage();
         message.writeUTF("Hello world!!!!" + i);
         message.setIntProperty("count", i);
         p.send(message);
      }
      t.join();

      for (Throwable e : exceptions) {
         throw e;
      }
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();

      connection.close();
      Assert.assertEquals(0, getMessageCount(q));

      long taken = (System.currentTimeMillis() - time);
      System.out.println("Microbenchamrk ran in " + taken + " milliseconds, sending/receiving " + numMessages);

      double messagesPerSecond = ((double) numMessages / (double) taken) * 1000;

      System.out.println(((int) messagesPerSecond) + " messages per second");

   }

   @Test
   public void testSimpleBinary() throws Throwable {
      final int numMessages = 500;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++) {
         bytes[i] = (byte) i;
      }

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         BytesMessage message = session.createBytesMessage();

         message.writeBytes(bytes);
         message.setIntProperty("count", i);
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         BytesMessage m = (BytesMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         m.reset();

         long size = m.getBodyLength();
         byte[] bytesReceived = new byte[(int) size];
         m.readBytes(bytesReceived);

         System.out.println("Received " + ByteUtil.bytesToHex(bytesReceived, 1) + " count - " + m.getIntProperty("count"));

         Assert.assertArrayEquals(bytes, bytesReceived);
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSimpleDefault() throws Throwable {
      final int numMessages = 500;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++) {
         bytes[i] = (byte) i;
      }

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         Message message = session.createMessage();

         message.setIntProperty("count", i);
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         Message m = consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSimpleMap() throws Throwable {
      final int numMessages = 100;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         MapMessage message = session.createMapMessage();

         message.setInt("i", i);
         message.setIntProperty("count", i);
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         MapMessage m = (MapMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         Assert.assertEquals(i, m.getInt("i"));
         Assert.assertEquals(i, m.getIntProperty("count"));
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSimpleStream() throws Throwable {
      final int numMessages = 100;
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         StreamMessage message = session.createStreamMessage();
         message.writeInt(i);
         message.writeBoolean(true);
         message.writeString("test");
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         StreamMessage m = (StreamMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         Assert.assertEquals(i, m.readInt());
         Assert.assertEquals(true, m.readBoolean());
         Assert.assertEquals("test", m.readString());
      }

   }

   @Test
   public void testSimpleText() throws Throwable {
      final int numMessages = 100;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         TextMessage message = session.createTextMessage("text" + i);
         message.setStringProperty("text", "text" + i);
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         TextMessage m = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);
         Assert.assertEquals("text" + i, m.getText());
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSimpleObject() throws Throwable {
      final int numMessages = 1;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         ObjectMessage message = session.createObjectMessage(new AnythingSerializable(i));
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         ObjectMessage msg = (ObjectMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", msg);

         AnythingSerializable someSerialThing = (AnythingSerializable) msg.getObject();
         Assert.assertEquals(i, someSerialThing.getCount());
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSelector() throws Exception {
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      TextMessage message = session.createTextMessage();
      message.setText("msg:0");
      p.send(message);
      message = session.createTextMessage();
      message.setText("msg:1");
      message.setStringProperty("color", "RED");
      p.send(message);
      connection.start();
      MessageConsumer messageConsumer = session.createConsumer(queue, "color = 'RED'");
      TextMessage m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      Assert.assertEquals("msg:1", m.getText());
      Assert.assertEquals(m.getStringProperty("color"), "RED");
      connection.close();
   }

   @Test
   public void testProperties() throws Exception {
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      TextMessage message = session.createTextMessage();
      message.setText("msg:0");
      message.setBooleanProperty("true", true);
      message.setBooleanProperty("false", false);
      message.setStringProperty("foo", "bar");
      message.setDoubleProperty("double", 66.6);
      message.setFloatProperty("float", 56.789f);
      message.setIntProperty("int", 8);
      message.setByteProperty("byte", (byte) 10);
      p.send(message);
      p.send(message);
      connection.start();
      MessageConsumer messageConsumer = session.createConsumer(queue);
      TextMessage m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      Assert.assertEquals("msg:0", m.getText());
      Assert.assertEquals(m.getBooleanProperty("true"), true);
      Assert.assertEquals(m.getBooleanProperty("false"), false);
      Assert.assertEquals(m.getStringProperty("foo"), "bar");
      Assert.assertEquals(m.getDoubleProperty("double"), 66.6, 0.0001);
      Assert.assertEquals(m.getFloatProperty("float"), 56.789f, 0.0001);
      Assert.assertEquals(m.getIntProperty("int"), 8);
      Assert.assertEquals(m.getByteProperty("byte"), (byte) 10);
      m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      connection.close();
   }

   private javax.jms.Queue createQueue(String address) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try {
         return session.createQueue(address);
      }
      finally {
         session.close();
      }
   }

   private javax.jms.Connection createConnection() throws JMSException {
      Connection connection;
      if (protocol == 3) {
         factory = new JmsConnectionFactory(amqpConnectionUri);
         connection = factory.createConnection();
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
               exception.printStackTrace();
            }
         });
         connection.start();
      }
      else if (protocol == 0) {
         factory = new JmsConnectionFactory(userName, password, amqpConnectionUri);
         connection = factory.createConnection();
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
               exception.printStackTrace();
            }
         });
         connection.start();
      }
      else {
         TransportConfiguration transport;

         if (protocol == 1) {
            transport = new TransportConfiguration(INVM_CONNECTOR_FACTORY);
            factory = new ActiveMQConnectionFactory("vm:/0");
         }
         else {
            factory = new ActiveMQConnectionFactory();
         }

         connection = factory.createConnection(userName, password);
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
               exception.printStackTrace();
            }
         });
         connection.start();
      }

      return connection;
   }

   private void setAddressFullBlockPolicy() {
      // For BLOCK tests
      AddressSettings addressSettings = server.getAddressSettingsRepository().getMatch("#");
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      addressSettings.setMaxSizeBytes(maxSizeBytes);
      addressSettings.setMaxSizeBytesRejectThreshold(maxSizeBytesRejectThreshold);
      server.getAddressSettingsRepository().addMatch("#", addressSettings);
   }

   public static class AnythingSerializable implements Serializable {

      private int count;

      public AnythingSerializable(int count) {
         this.count = count;
      }

      public int getCount() {
         return count;
      }
   }
}
