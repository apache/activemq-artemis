/*
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

package org.apache.activemq.artemis.tests.extras.protocols.hornetq;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.utils.UUIDGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HornetQProtocolTest extends ActiveMQTestBase {

   protected ActiveMQServer server;

   private static final Logger LOG = LoggerFactory.getLogger(HornetQProtocolTest.class);

   @Override
   @Before
   public void setUp() throws Exception {
      HashMap<String, Object> params = new HashMap<>();
      params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, "" + 5445);
      params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PROTOCOLS_PROP_NAME, "HORNETQ");
      TransportConfiguration transportConfig = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      super.setUp();
      server = createServer(true, true);
      server.getConfiguration().getAcceptorConfigurations().add(transportConfig);
      LOG.info("Added connector {} to broker", "HornetQ");
      server.start();
      waitForServerToStart(server);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      org.hornetq.core.client.impl.ServerLocatorImpl.clearThreadPools();
      super.tearDown();
   }

   @Test
   public void testMessagePropertiesAreTransformedBetweenCoreAndHQProtocols() throws Exception {
      org.hornetq.api.core.client.ClientSession hqSession = createHQClientSession();
      ClientSession coreSession = createCoreClientSession();

      // Create Queue
      String queueName = "test.hq.queue";
      hqSession.createQueue(queueName, queueName, true);

      // HornetQ Client Objects
      hqSession.start();
      org.hornetq.api.core.client.ClientProducer hqProducer = hqSession.createProducer(queueName);
      org.hornetq.api.core.client.ClientConsumer hqConsumer = hqSession.createConsumer(queueName);

      // Core Client Objects
      coreSession.start();
      ClientConsumer coreConsumer = coreSession.createConsumer(queueName);

      // Check that HornetQ Properties are correctly converted to core properties.
      for (int i = 0; i < 2; i++) {
         hqProducer.send(createHQTestMessage(hqSession));
      }

      ClientMessage coreMessage1 = coreConsumer.receive(1000);
      System.err.println("Messages::==" + coreMessage1);
      assertTrue(coreMessage1.containsProperty(Message.HDR_DUPLICATE_DETECTION_ID));
      coreSession.close();

      // Check that HornetQ Properties are correctly transformed from then to HornetQ properties
      org.hornetq.api.core.client.ClientMessage hqMessage1 = hqConsumer.receive(1000);
      assertTrue(hqMessage1.containsProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID));

      hqSession.close();
   }


   @Test
   public void testCreateConsumerHQSelectorIsTransformed() throws Exception {
      try (org.hornetq.api.core.client.ClientSession hqSession = createHQClientSession()) {

         // Create Queue
         String queueName = "test.hq.queue";
         hqSession.createQueue(queueName, queueName, true);

         hqSession.start();

         // Send message with UserID set
         org.hornetq.utils.UUID userID = UUIDGenerator.getInstance().generateUUID();
         try (org.hornetq.api.core.client.ClientProducer hqProducer = hqSession.createProducer(queueName)) {
            org.hornetq.api.core.client.ClientMessage message = createHQTestMessage(hqSession);
            message.setUserID(userID);
            hqProducer.send(message);
         }

         // Verify that selector using AMQ field works
         verifyConsumerWithSelector(hqSession, queueName,
                 String.format("AMQUserID = 'ID:%s'", userID.toString()), userID);

         // Verify that selector using HornetQ field works
         verifyConsumerWithSelector(hqSession, queueName,
                 String.format("HQUserID = 'ID:%s'", userID.toString()), userID);
      }
   }

   @Test
   public void testCreateQueueHQFilterIsTransformed() throws Exception {
      testQueueWithHQFilter(false);
   }

   @Test
   public void testCreateTemporaryQueueHQFilterIsTransformed() throws Exception {
      testQueueWithHQFilter(true);
   }

   @Test
   public void testLargeMessagesOverHornetQClients() throws Exception {
      org.hornetq.api.core.client.ClientSession hqSession = createHQClientSession();

      // Create Queue
      String queueName = "test.hq.queue";
      hqSession.createQueue(queueName, queueName, true);

      // HornetQ Client Objects
      hqSession.start();
      org.hornetq.api.core.client.ClientProducer hqProducer = hqSession.createProducer(queueName);
      org.hornetq.api.core.client.ClientConsumer hqConsumer = hqSession.createConsumer(queueName);

      for (int i = 0; i < 2; i++) {
         org.hornetq.api.core.client.ClientMessage hqMessage = hqSession.createMessage(true);
         hqMessage.setBodyInputStream(ActiveMQTestBase.createFakeLargeStream(10 * 1024));
         hqProducer.send(hqMessage);
      }
      hqSession.commit();

      for (int i = 0; i < 2; i++) {
         org.hornetq.api.core.client.ClientMessage coreMessage1 = hqConsumer.receive(1000);
         coreMessage1.acknowledge();
         System.err.println("Messages::==" + coreMessage1);
         for (int j = 0; j < 10 * 1024; j++) {
            Assert.assertEquals(ActiveMQTestBase.getSamplebyte(j), coreMessage1.getBodyBuffer().readByte());
         }

      }

      hqSession.close();
   }

   @Test
   public void testDuplicateIDPropertyWithHornetQProtocol() throws Exception {
      org.hornetq.api.core.client.ClientSession session = createHQClientSession();

      String queueName = "test.hq.queue";
      session.createQueue(queueName, queueName, true);

      org.hornetq.api.core.client.ClientProducer producer = session.createProducer(queueName);
      org.hornetq.api.core.client.ClientConsumer consumer = session.createConsumer(queueName);
      org.hornetq.api.core.client.ClientMessage message = session.createMessage(false);

      String messageId = UUID.randomUUID().toString();
      message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), messageId);

      session.start();
      producer.send(message);
      org.hornetq.api.core.client.ClientMessage m = consumer.receive(1000);
      assertTrue(m.containsProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID));
      assertNotNull(m);

      producer.send(message);
      m = consumer.receive(1000);
      assertNull(m);

      producer.send(message);
      m = consumer.receive(1000);
      assertNull(m);

      session.close();
   }

   @Test
   public void testDuplicateIDPropertyWithHornetQAndCoreProtocol() throws Exception {
      org.hornetq.api.core.client.ClientSession hqSession = createHQClientSession();

      String queueName = "test.hq.queue";
      hqSession.createQueue(queueName, queueName, true);

      org.hornetq.api.core.client.ClientProducer hqProducer = hqSession.createProducer(queueName);
      org.hornetq.api.core.client.ClientMessage message = hqSession.createMessage(false);

      String messageId = UUID.randomUUID().toString();
      message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), messageId);

      ClientSession coreSession = createCoreClientSession();
      ClientConsumer coreConsumer = coreSession.createConsumer(queueName);

      hqSession.start();
      coreSession.start();

      hqProducer.send(message);
      Message m = coreConsumer.receive(1000);
      assertTrue(m.containsProperty(Message.HDR_DUPLICATE_DETECTION_ID));
      assertNotNull(m);

      hqProducer.send(message);
      m = coreConsumer.receive(1000);
      assertNull(m);

      hqProducer.send(message);
      m = coreConsumer.receive(1000);
      assertNull(m);
   }

   private org.hornetq.api.core.client.ClientMessage createHQTestMessage(org.hornetq.api.core.client.ClientSession session) {
      org.hornetq.api.core.client.ClientMessage message = session.createMessage(true);
      String v = UUID.randomUUID().toString();
      message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), v);
      return message;
   }

   private ClientMessage createCoreTestMessage(ClientSession session) {
      ClientMessage message = session.createMessage(true);
      String v = UUID.randomUUID().toString();
      message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), v);
      return message;
   }

   private org.hornetq.api.core.client.ClientSession createHQClientSession() throws Exception {
      Map<String, Object> map = new HashMap<>();
      map.put("host", "localhost");
      map.put("port", 5445);

      org.hornetq.api.core.client.ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new org.hornetq.api.core.TransportConfiguration(org.hornetq.core.remoting.impl.netty.NettyConnectorFactory.class.getName(), map));
      org.hornetq.api.core.client.ClientSessionFactory sf = serverLocator.createSessionFactory();

      return sf.createSession();
   }

   private ClientSession createCoreClientSession() throws Exception {
      Map<String, Object> map = new HashMap<>();
      map.put("host", "localhost");
      map.put("port", 61616);

      ServerLocator serverLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), map));
      ClientSessionFactory sf = serverLocator.createSessionFactory();

      return sf.createSession();
   }

   private static void verifyConsumerWithSelector(org.hornetq.api.core.client.ClientSession hqSession, String queueName,
                                                  String selector, org.hornetq.utils.UUID expectedUserID)
           throws HornetQException {
      try (org.hornetq.api.core.client.ClientConsumer hqConsumer =
                   hqSession.createConsumer(queueName, selector, true)) {
         org.hornetq.api.core.client.ClientMessage message = hqConsumer.receive(1000);

         Assert.assertNotNull(message);
         Assert.assertEquals(expectedUserID, message.getUserID());
      }
   }

   private void testQueueWithHQFilter(boolean temporary) throws Exception {
      try (org.hornetq.api.core.client.ClientSession hqSession = createHQClientSession()) {

         org.hornetq.utils.UUID userID = UUIDGenerator.getInstance().generateUUID();

         // Create queue with filter
         String queueName = "test.hq.queue";
         String filter = String.format("HQUserID = 'ID:%s'", userID.toString());
         if (temporary) {
            hqSession.createTemporaryQueue(queueName, queueName, filter);
         } else {
            hqSession.createQueue(queueName, queueName, filter, true);
         }

         hqSession.start();

         // Send two messages with different UserIDs
         try (org.hornetq.api.core.client.ClientProducer hqProducer = hqSession.createProducer(queueName)) {
            org.hornetq.api.core.client.ClientMessage message = createHQTestMessage(hqSession);
            message.setUserID(userID);
            hqProducer.send(message);

            message = createHQTestMessage(hqSession);
            message.setUserID(UUIDGenerator.getInstance().generateUUID());
            hqProducer.send(message);
         }

         // Only the message matching the queue filter should be present
         try (org.hornetq.api.core.client.ClientConsumer hqConsumer =
                      hqSession.createConsumer(queueName, true)) {
            org.hornetq.api.core.client.ClientMessage message = hqConsumer.receiveImmediate();

            Assert.assertNotNull(message);
            Assert.assertEquals(userID, message.getUserID());

            message = hqConsumer.receiveImmediate();
            Assert.assertNull(message);
         }
      }
   }
}
