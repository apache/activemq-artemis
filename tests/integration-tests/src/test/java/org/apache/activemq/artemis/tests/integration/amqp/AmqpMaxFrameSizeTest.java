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
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpMaxFrameSizeTest extends AmqpClientTestSupport {

   protected static final Logger LOG = LoggerFactory.getLogger(AmqpMaxFrameSizeTest.class);

   private boolean maxFrameSizeConfigSet = false;
   private static final int CONFIGURED_FRAME_SIZE = 4321;

   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      if ("testBrokerAdvertisedConfiguredMaxFrameSize".equals(getTestName())) {
         maxFrameSizeConfigSet = true;
         params.put("maxFrameSize", CONFIGURED_FRAME_SIZE);
      }
   }

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      // Make the journal file size larger than the frame+message sizes used in the tests,
      // since it is by default for external brokers and it changes the behaviour.
      server.getConfiguration().setJournalFileSize(2 * 1024 * 1024);
   }

   @Test(timeout = 60000)
   public void testBrokerAdvertisedDefaultMaxFrameSize() throws Exception {
      assertFalse("maxFrameSize should not be explicitly configured", maxFrameSizeConfigSet);

      AmqpClient client = createAmqpClient();
      assertNotNull(client);

      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Connection connection) {
            int brokerMaxFrameSize = connection.getTransport().getRemoteMaxFrameSize();
            if (brokerMaxFrameSize != AmqpSupport.MAX_FRAME_SIZE_DEFAULT) {
               markAsInvalid("Broker did not send the expected max Frame Size");
            }
         }
      });

      AmqpConnection connection = addConnection(client.connect());
      try {
         assertNotNull(connection);
         connection.getStateInspector().assertValid();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testBrokerAdvertisedConfiguredMaxFrameSize() throws Exception {
      assertTrue("maxFrameSize should be explicitly configured", maxFrameSizeConfigSet);

      AmqpClient client = createAmqpClient();
      assertNotNull(client);

      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Connection connection) {
            int brokerMaxFrameSize = connection.getTransport().getRemoteMaxFrameSize();
            if (brokerMaxFrameSize != CONFIGURED_FRAME_SIZE) {
               markAsInvalid("Broker did not send the expected max Frame Size");
            }
         }
      });

      AmqpConnection connection = addConnection(client.connect());
      try {
         assertNotNull(connection);
         connection.getStateInspector().assertValid();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testManyMultiFrameTransfersWithClientMaxFrameSizeSmallerThanBrokers() throws Exception {
      final int clientMaxFrameSize = 1024;
      final int brokerMaxFrameSize = AmqpSupport.MAX_FRAME_SIZE_DEFAULT;
      final int messageSize = 2 * AmqpSupport.MAX_FRAME_SIZE_DEFAULT + 5;

      assertTrue("Client maxFrameSize should be smaller than brokers", clientMaxFrameSize < brokerMaxFrameSize);

      doManyMultiFrameTransfersTestImpl(clientMaxFrameSize, messageSize, brokerMaxFrameSize);
   }

   @Test(timeout = 60000)
   public void testManyMultiFrameTransfersWithClientMaxFrameSizeLargerThanBrokers() throws Exception {
      final int clientMaxFrameSize = 2 * AmqpSupport.MAX_FRAME_SIZE_DEFAULT;
      final int brokerMaxFrameSize = AmqpSupport.MAX_FRAME_SIZE_DEFAULT;
      final int messageSize = 2 * AmqpSupport.MAX_FRAME_SIZE_DEFAULT + 5;

      assertTrue("Client maxFrameSize should be larger than brokers", clientMaxFrameSize > brokerMaxFrameSize);

      doManyMultiFrameTransfersTestImpl(clientMaxFrameSize, messageSize, brokerMaxFrameSize);
   }

   private void doManyMultiFrameTransfersTestImpl(int maxFrameSize, int payloadSize, int brokerMaxFrameSize) throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      int numMsgs = 200;
      String testQueueName = getTestName();

      AmqpClient client = createAmqpClient();

      client.setValidator(new AmqpValidator() {
         @Override
         public void inspectOpenedResource(Connection connection) {
            int brokerMaxFrameSize = connection.getTransport().getRemoteMaxFrameSize();
            if (brokerMaxFrameSize != AmqpSupport.MAX_FRAME_SIZE_DEFAULT) {
               markAsInvalid("Broker did not send the expected max Frame Size");
            }
         }
      });

      AmqpConnection connection = client.createConnection();
      connection.setMaxFrameSize(maxFrameSize);

      connection.connect();
      addConnection(connection);

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(testQueueName);

         for (int i = 0; i < numMsgs; ++i) {
            AmqpMessage message = createAmqpMessage(payloadSize);
            sender.send(message);
         }

         Wait.assertEquals(numMsgs, () -> getMessageCount(server.getPostOffice(), testQueueName), 5000, 10);

         AmqpReceiver receiver = session.createReceiver(testQueueName);
         receiver.flow(numMsgs);

         for (int i = 1; i <= numMsgs; ++i) {
            AmqpMessage receivedMessage = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull("Did not recieve message " + i, receivedMessage);

            verifyMessage(receivedMessage, payloadSize);

            LOG.trace("received : message " + i);
            receivedMessage.accept();
         }

      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testSingleAndMultiFrameTransferClientMaxFrameSizeSmallerThanBrokers() throws Exception {
      final int clientMaxFrameSize = 1024;
      final int brokerMaxFrameSize = AmqpSupport.MAX_FRAME_SIZE_DEFAULT;

      assertTrue("Client maxFrameSize should be smaller than brokers", clientMaxFrameSize < brokerMaxFrameSize);

      doSingleAndMultiFrameTransferTestImpl(clientMaxFrameSize, brokerMaxFrameSize);
   }

   @Test(timeout = 60000)
   public void testSingleAndMultiFrameTransferWithClientMaxFrameSizeLargerThanBrokers() throws Exception {
      final int clientMaxFrameSize = 2 * AmqpSupport.MAX_FRAME_SIZE_DEFAULT;
      final int brokerMaxFrameSize = AmqpSupport.MAX_FRAME_SIZE_DEFAULT;

      assertTrue("Client maxFrameSize should be larger than brokers", clientMaxFrameSize > brokerMaxFrameSize);

      doSingleAndMultiFrameTransferTestImpl(clientMaxFrameSize, brokerMaxFrameSize);
   }

   private void doSingleAndMultiFrameTransferTestImpl(int maxFrameSize, int brokerMaxFrameSize) throws Exception {
      final int messageSize1 = 128;
      final int messageSize2 = 2 * AmqpSupport.MAX_FRAME_SIZE_DEFAULT + 5;

      assertTrue("messageSize1 should be much smaller than both of the maxFrameSizes",
                 messageSize1 < maxFrameSize / 2 && messageSize1 < brokerMaxFrameSize / 2);
      assertTrue("messageSize2 should be larger than one of the maxFrameSizes",
                 messageSize2 > maxFrameSize || messageSize2 > brokerMaxFrameSize);

      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      String testQueueName = getTestName();

      AmqpClient client = createAmqpClient();

      client.setValidator(new AmqpValidator() {
         @Override
         public void inspectOpenedResource(Connection connection) {
            int brokerMaxFrameSize = connection.getTransport().getRemoteMaxFrameSize();
            if (brokerMaxFrameSize != AmqpSupport.MAX_FRAME_SIZE_DEFAULT) {
               markAsInvalid("Broker did not send the expected max Frame Size");
            }
         }
      });

      AmqpConnection connection = client.createConnection();
      connection.setMaxFrameSize(maxFrameSize);

      connection.connect();
      addConnection(connection);

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(testQueueName);

         AmqpMessage message1 = createAmqpMessage(messageSize1);
         AmqpMessage message2 = createAmqpMessage(messageSize2);
         sender.send(message1);
         sender.send(message2);

         Wait.assertEquals(2, () -> getMessageCount(server.getPostOffice(), testQueueName), 5000, 10);

         AmqpReceiver receiver = session.createReceiver(testQueueName);
         receiver.flow(2);

         AmqpMessage receivedMessage1 = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull("Did not recieve message 1", receivedMessage1);
         verifyMessage(receivedMessage1, messageSize1);
         receivedMessage1.accept();

         AmqpMessage receivedMessage2 = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull("Did not recieve message 2", receivedMessage2);
         verifyMessage(receivedMessage2, messageSize2);

         receivedMessage2.accept();
      } finally {
         connection.close();
      }
   }

   private AmqpMessage createAmqpMessage(final int payloadSize) {
      AmqpMessage message = new AmqpMessage();
      byte[] payload = new byte[payloadSize];
      for (int i = 0; i < payload.length; i++) {
         // An odd number of digit characters
         int offset = i % 7;
         payload[i] = (byte) (48 + offset);
      }
      message.setBytes(payload);
      return message;
   }

   private void verifyMessage(final AmqpMessage message, final int payloadSize) {
      MessageImpl wrapped = (MessageImpl) message.getWrappedMessage();

      assertNotNull("Message has no body", wrapped.getBody());
      assertTrue("Unexpected body type: " + wrapped.getBody().getClass(), wrapped.getBody() instanceof Data);

      Data data = (Data) wrapped.getBody();
      Binary binary = data.getValue();
      assertNotNull("Data section has no content", binary);
      assertEquals("Unexpected payload length", payloadSize, binary.getLength());

      byte[] binaryContent = binary.getArray();
      int offset = binary.getArrayOffset();
      for (int i = 0; i < payloadSize; i++) {
         byte expected = (byte) (48 + (i % 7));
         assertEquals("Unexpected content at payload index " + i, expected, binaryContent[i + offset]);
      }
   }
}
