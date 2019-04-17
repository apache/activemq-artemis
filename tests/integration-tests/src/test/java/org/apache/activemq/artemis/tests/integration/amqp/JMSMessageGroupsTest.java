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
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.concurrent.atomic.AtomicInteger;

import java.util.function.BiConsumer;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSMessageGroupsTest extends JMSClientTestSupport {

   protected static final Logger LOG = LoggerFactory.getLogger(JMSMessageGroupsTest.class);

   private static final int ITERATIONS = 10;
   private static final int MESSAGE_COUNT = 10;
   private static final int MESSAGE_SIZE = 10 * 1024;
   private static final int RECEIVE_TIMEOUT = 1000;
   private static final String JMSX_GROUP_ID = "JmsGroupsTest";

   private ConnectionSupplier AMQPConnection = () -> createConnection();
   private ConnectionSupplier CoreConnection = () -> createCoreConnection();
   private ConnectionSupplier OpenWireConnection = () -> createOpenWireConnection();

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Override
   protected void configureAddressPolicy(ActiveMQServer server) {
      super.configureAddressPolicy(server);

      AddressSettings addressSettings = new AddressSettings();

      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.setAutoCreateQueues(isAutoCreateQueues());
      addressSettings.setAutoCreateAddresses(isAutoCreateAddresses());
      addressSettings.setDeadLetterAddress(SimpleString.toSimpleString(getDeadLetterAddress()));
      addressSettings.setExpiryAddress(SimpleString.toSimpleString(getDeadLetterAddress()));
      addressSettings.setDefaultGroupFirstKey(SimpleString.toSimpleString("JMSXFirstInGroupID"));


      server.getConfiguration().getAddressesSettings().put("GroupFirst.#", addressSettings);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsAMQPProducerAMQPConsumer() throws Exception {
      testMessageGroups(AMQPConnection, AMQPConnection);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsCoreProducerCoreConsumer() throws Exception {
      testMessageGroups(CoreConnection, CoreConnection);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsCoreProducerAMQPConsumer() throws Exception {
      testMessageGroups(CoreConnection, AMQPConnection);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsAMQPProducerCoreConsumer() throws Exception {
      testMessageGroups(AMQPConnection, CoreConnection);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsOpenWireProducerOpenWireConsumer() throws Exception {
      testMessageGroups(OpenWireConnection, OpenWireConnection);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsCoreProducerOpenWireConsumer() throws Exception {
      testMessageGroups(CoreConnection, OpenWireConnection);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsOpenWireProducerCoreConsumer() throws Exception {
      testMessageGroups(OpenWireConnection, CoreConnection);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsAMQPProducerOpenWireConsumer() throws Exception {
      testMessageGroups(AMQPConnection, OpenWireConnection);
   }

   @Test(timeout = 60000)
   public void testMessageGroupsOpenWireProducerAMQPConsumer() throws Exception {
      testMessageGroups(OpenWireConnection, AMQPConnection);
   }


   public void testMessageGroups(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      testGroupSeqIsNeverLost(producerConnectionSupplier, consumerConnectionSupplier);
      testGroupSeqCloseGroup(producerConnectionSupplier, consumerConnectionSupplier);
      testGroupFirst(producerConnectionSupplier, consumerConnectionSupplier);
      testGroupFirstDefaultOff(producerConnectionSupplier, consumerConnectionSupplier);
   }


   public void testGroupSeqCloseGroup(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      final QueueBinding queueBinding = (QueueBinding) server.getPostOffice().getBinding(SimpleString.toSimpleString(getQueueName()));

      try (Connection producerConnection = producerConnectionSupplier.createConnection();
           Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           MessageProducer producer = producerSession.createProducer(producerSession.createQueue(getQueueName()));

           Connection consumerConnection = producerConnectionSupplier.createConnection();
           Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           MessageConsumer consumer1 = consumerSession.createConsumer(consumerSession.createQueue(getQueueName()));
           MessageConsumer consumer2 = consumerSession.createConsumer(consumerSession.createQueue(getQueueName()));
           MessageConsumer consumer3 = consumerSession.createConsumer(consumerSession.createQueue(getQueueName()))) {

         producerConnection.start();
         consumerConnection.start();

         //Ensure group and close group, ensuring group is closed
         sendAndConsumeAndThenCloseGroup(producerSession, producer, consumer1, consumer2, consumer3, queueBinding);

         //Ensure round robin on group to consumer assignment (consumer2 now), then close group again
         sendAndConsumeAndThenCloseGroup(producerSession, producer, consumer2, consumer3, consumer1, queueBinding);

         //Ensure round robin on group to consumer assignment (consumer3 now), then close group again
         sendAndConsumeAndThenCloseGroup(producerSession, producer, consumer3, consumer1, consumer1, queueBinding);


      }
   }

   private void sendAndConsumeAndThenCloseGroup(Session producerSession, MessageProducer producer, MessageConsumer expectedGroupConsumer, MessageConsumer consumerA, MessageConsumer consumerB, QueueBinding queueBinding) throws JMSException {

      for (int j = 1; j <= MESSAGE_COUNT; j++) {
         TextMessage message = producerSession.createTextMessage();
         message.setStringProperty("JMSXGroupID", JMSX_GROUP_ID);
         message.setIntProperty("JMSXGroupSeq", j);
         message.setText("Message" + j);

         producer.send(message);
      }

      //Group should have been reset and next consumer chosen, as such all msgs should now go to the second consumer (round robin'd)
      for (int j = 1; j <= MESSAGE_COUNT; j++) {
         TextMessage tm = (TextMessage) expectedGroupConsumer.receive(RECEIVE_TIMEOUT);
         assertNotNull(tm);
         assertEquals(JMSX_GROUP_ID, tm.getStringProperty("JMSXGroupID"));
         assertEquals(j, tm.getIntProperty("JMSXGroupSeq"));
         assertEquals("Message" + j, tm.getText());

         assertNull(consumerA.receiveNoWait());
         assertNull(consumerB.receiveNoWait());
      }

      assertEquals(1, queueBinding.getQueue().getGroupCount());

      TextMessage message = producerSession.createTextMessage();
      message.setStringProperty("JMSXGroupID", JMSX_GROUP_ID);
      //Close Group using -1 JMSXGroupSeq
      message.setIntProperty("JMSXGroupSeq", -1);
      message.setText("Message" + " group close");

      producer.send(message);

      TextMessage receivedGroupCloseMessage = (TextMessage) expectedGroupConsumer.receive(RECEIVE_TIMEOUT);
      assertNotNull(receivedGroupCloseMessage);
      assertEquals(JMSX_GROUP_ID, receivedGroupCloseMessage.getStringProperty("JMSXGroupID"));
      assertEquals(-1, receivedGroupCloseMessage.getIntProperty("JMSXGroupSeq"));
      assertEquals("group close should goto the existing group consumer", "Message" + " group close", receivedGroupCloseMessage.getText());

      assertNull(consumerA.receiveNoWait());
      assertNull(consumerB.receiveNoWait());

      assertEquals(0, queueBinding.getQueue().getGroupCount());

   }


   public void testGroupSeqIsNeverLost(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      AtomicInteger sequenceCounter = new AtomicInteger();
      AtomicInteger consumedSequenceCounter = new AtomicInteger();
      String queueName = getQueueName();

      for (int i = 0; i < ITERATIONS; ++i) {
         try (Connection producerConnection = producerConnectionSupplier.createConnection();
              Connection consumerConnection = consumerConnectionSupplier.createConnection()) {
            sendMessagesToBroker(queueName, producerConnection, MESSAGE_COUNT, sequenceCounter);
            readMessagesOnBroker(queueName, consumerConnection, MESSAGE_COUNT, consumedSequenceCounter, null);
         }
      }
   }

   public void testGroupFirst(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      AtomicInteger sequenceCounter = new AtomicInteger();
      AtomicInteger consumedSequenceCounter = new AtomicInteger();
      //Use a queue that IS pre-fixed with GroupFirst so should full under Group First address settings
      String queueName = "GroupFirst." + getQueueName();

      for (int i = 0; i < ITERATIONS; ++i) {
         try (Connection producerConnection = producerConnectionSupplier.createConnection();
              Connection consumerConnection = consumerConnectionSupplier.createConnection()) {
            sendMessagesToBroker(queueName, producerConnection, MESSAGE_COUNT, sequenceCounter);
            readMessagesOnBroker(queueName, consumerConnection, MESSAGE_COUNT, consumedSequenceCounter, this::groupFirstCheck);
         }
      }
   }

   private void groupFirstCheck(int i, Message message) {
      try {
         if (i == 0) {
            assertTrue("Message should be marked with first in Group", message.getBooleanProperty("JMSXFirstInGroupID"));
         } else {
            assertFalse("Message should NOT be marked with first in Group", message.propertyExists("JMSXFirstInGroupID"));
         }
      } catch (JMSException e) {
         fail(e.getMessage());
      }
   }

   public void testGroupFirstDefaultOff(ConnectionSupplier producerConnectionSupplier, ConnectionSupplier consumerConnectionSupplier) throws Exception {
      AtomicInteger sequenceCounter = new AtomicInteger();
      AtomicInteger consumedSequenceCounter = new AtomicInteger();
      //Use a queue that IS NOT pre-fixed with GroupFirst so should full under default address settings.
      String queueName = getQueueName();

      for (int i = 0; i < ITERATIONS; ++i) {
         try (Connection producerConnection = producerConnectionSupplier.createConnection();
              Connection consumerConnection = consumerConnectionSupplier.createConnection()) {
            sendMessagesToBroker(queueName, producerConnection, MESSAGE_COUNT, sequenceCounter);
            readMessagesOnBroker(queueName, consumerConnection, MESSAGE_COUNT, consumedSequenceCounter, this::groupFirstOffCheck);
         }
      }
   }

   private void groupFirstOffCheck(int i, Message message) {
      try {
         assertFalse("Message should NOT be marked with first in Group", message.propertyExists("JMSXFirstInGroupID"));
      } catch (JMSException e) {
         fail(e.getMessage());
      }
   }



   protected void readMessagesOnBroker(String queueName, Connection connection, int count, AtomicInteger sequence, BiConsumer<Integer, Message> additionalCheck) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(queueName);
      MessageConsumer consumer = session.createConsumer(queue);

      for (int i = 0; i < MESSAGE_COUNT; ++i) {
         Message message = consumer.receive(RECEIVE_TIMEOUT);
         assertNotNull(message);
         LOG.debug("Read message #{}: type = {}", i, message.getClass().getSimpleName());
         String gid = message.getStringProperty("JMSXGroupID");
         int seq = message.getIntProperty("JMSXGroupSeq");
         LOG.debug("Message assigned JMSXGroupID := {}", gid);
         LOG.debug("Message assigned JMSXGroupSeq := {}", seq);
         assertEquals("Sequence order should match", sequence.incrementAndGet(), seq);
         if (additionalCheck != null) {
            additionalCheck.accept(i, message);
         }
      }

      session.close();
   }



   protected void sendMessagesToBroker(String queueName, Connection connection, int count, AtomicInteger sequence) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(queueName);
      MessageProducer producer = session.createProducer(queue);

      byte[] buffer = new byte[MESSAGE_SIZE];
      for (count = 0; count < MESSAGE_SIZE; count++) {
         String s = String.valueOf(count % 10);
         Character c = s.charAt(0);
         int value = c.charValue();
         buffer[count] = (byte) value;
      }

      LOG.debug("Sending {} messages to destination: {}", MESSAGE_COUNT, queue);
      for (int i = 1; i <= MESSAGE_COUNT; i++) {
         BytesMessage message = session.createBytesMessage();
         message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
         message.setStringProperty("JMSXGroupID", JMSX_GROUP_ID);
         message.setIntProperty("JMSXGroupSeq", sequence.incrementAndGet());
         message.writeBytes(buffer);
         producer.send(message);
      }

      session.close();
   }
}
