/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.amqp.connect;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerTarget;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonAbstractReceiver;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PagedMirrorTest extends ActiveMQTestBase {

   private static final Logger logger = Logger.getLogger(PagedMirrorTest.class);
   ActiveMQServer server1;

   ActiveMQServer server2;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      server1 = createServer(true, createDefaultConfig(0, true), 1024, 10 * 1024, -1, -1);
      server1.getConfiguration().getAcceptorConfigurations().clear();
      server1.getConfiguration().addAcceptorConfiguration("server", "tcp://localhost:61616");
      AMQPBrokerConnectConfiguration brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("other", "tcp://localhost:61617").setReconnectAttempts(-1).setRetryInterval(1000);
      brokerConnectConfiguration.addElement(new AMQPMirrorBrokerConnectionElement());
      server1.getConfiguration().addAMQPConnection(brokerConnectConfiguration);

      server2 = createServer(true, createDefaultConfig(1, true), 1024, 10 * 1024, -1, -1);
      server2.getConfiguration().getAcceptorConfigurations().clear();
      server2.getConfiguration().addAcceptorConfiguration("server", "tcp://localhost:61617");
      brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("other", "tcp://localhost:61616").setReconnectAttempts(-1).setRetryInterval(1000);
      brokerConnectConfiguration.addElement(new AMQPMirrorBrokerConnectionElement());
      server2.getConfiguration().addAMQPConnection(brokerConnectConfiguration);

      server1.start();
      server2.start();
   }

   @Test
   public void testPaged() throws Throwable {
      String sendURI = "tcp://localhost:61616";
      String consumeURI = "tcp://localhost:61616";
      String secondConsumeURI = "tcp://localhost:61617";

      Wait.waitFor(() -> server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_other") != null);
      Wait.waitFor(() -> server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_other") != null);

      org.apache.activemq.artemis.core.server.Queue snf1 = server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_other");
      Assert.assertNotNull(snf1);

      org.apache.activemq.artemis.core.server.Queue snf2 = server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_other");
      Assert.assertNotNull(snf2);

      File countJournalLocation = server1.getConfiguration().getJournalLocation();
      Assert.assertTrue(countJournalLocation.exists() && countJournalLocation.isDirectory());
      String protocol = "amqp";

      ConnectionFactory sendCF = CFUtil.createConnectionFactory(protocol, sendURI);
      ConnectionFactory consumeCF = CFUtil.createConnectionFactory(protocol, consumeURI);
      ConnectionFactory secondConsumeCF = CFUtil.createConnectionFactory(protocol, secondConsumeURI);

      String bodyBuffer;
      {
         StringBuffer buffer = new StringBuffer();
         for (int i = 0; i < 1024; i++) {
            buffer.append("*");
         }
         bodyBuffer = buffer.toString();
      }

      int NUMBER_OF_MESSAGES = 200;
      int ACK_I = 77;

      try (Connection sendConnecton = sendCF.createConnection()) {
         Session sendSession = sendConnecton.createSession(true, Session.SESSION_TRANSACTED);
         Queue jmsQueue = sendSession.createQueue("someQueue");
         MessageProducer producer = sendSession.createProducer(jmsQueue);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = sendSession.createTextMessage(bodyBuffer);
            message.setIntProperty("i", i);
            producer.send(message);
         }
         sendSession.commit();
      }

      Wait.assertEquals(0, snf1::getMessageCount);
      Wait.assertEquals(0, snf2::getMessageCount);

      try (Connection consumeConnection = consumeCF.createConnection()) {
         Session consumeSession = consumeConnection.createSession(false, 101); // individual ack
         Queue jmsQueue = consumeSession.createQueue("someQueue");
         MessageConsumer consumer = consumeSession.createConsumer(jmsQueue);
         consumeConnection.start();
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = (TextMessage) consumer.receive(6000);
            if (message.getIntProperty("i") == ACK_I) {
               message.acknowledge();
            }
         }
         Assert.assertNull(consumer.receiveNoWait());
      }
      Wait.assertEquals(0, snf1::getMessageCount);
      Wait.assertEquals(0, snf2::getMessageCount);
      Wait.assertEquals(1, () -> acksCount(countJournalLocation), 5000, 1000);

      HashSet<Integer> receivedIDs = new HashSet<>();

      try (Connection consumeConnection = secondConsumeCF.createConnection()) {
         Session consumeSession = consumeConnection.createSession(true, Session.SESSION_TRANSACTED);
         Queue jmsQueue = consumeSession.createQueue("someQueue");
         MessageConsumer consumer = consumeSession.createConsumer(jmsQueue);
         consumeConnection.start();

         for (int i = 0; i < NUMBER_OF_MESSAGES - 1; i++) {
            TextMessage message = (TextMessage) consumer.receive(6000);
            Assert.assertNotNull(message);
            Assert.assertNotEquals(ACK_I, message.getIntProperty("i"));
            receivedIDs.add(message.getIntProperty("i"));
         }
         Assert.assertNull(consumer.receiveNoWait());


         Assert.assertEquals(NUMBER_OF_MESSAGES - 1, receivedIDs.size());

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            if (i != ACK_I) {
               Assert.assertTrue(receivedIDs.contains(i));
            }
         }
      }
   }


   @Test
   public void testAckWithScan() throws Throwable {
      String sendURI = "tcp://localhost:61616";
      String consumeURI = "tcp://localhost:61617";

      Wait.waitFor(() -> server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_other") != null);
      Wait.waitFor(() -> server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_other") != null);

      org.apache.activemq.artemis.core.server.Queue snf1 = server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_other");
      Assert.assertNotNull(snf1);

      org.apache.activemq.artemis.core.server.Queue snf2 = server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_other");
      Assert.assertNotNull(snf2);

      File countJournalLocation = server1.getConfiguration().getJournalLocation();
      Assert.assertTrue(countJournalLocation.exists() && countJournalLocation.isDirectory());
      String protocol = "amqp";

      ConnectionFactory sendCF = CFUtil.createConnectionFactory(protocol, sendURI);

      String bodyBuffer;
      {
         StringBuffer buffer = new StringBuffer();
         for (int i = 0; i < 1024; i++) {
            buffer.append("*");
         }
         bodyBuffer = buffer.toString();
      }

      int NUMBER_OF_MESSAGES = 200;

      try (Connection sendConnecton = sendCF.createConnection()) {
         Session sendSession = sendConnecton.createSession(true, Session.SESSION_TRANSACTED);
         Queue jmsQueue = sendSession.createQueue("someQueue");
         MessageProducer producer = sendSession.createProducer(jmsQueue);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = sendSession.createTextMessage(bodyBuffer);
            message.setIntProperty("i", i);
            producer.send(message);
         }
         sendSession.commit();
      }

      Wait.assertEquals(0, snf1::getMessageCount);
      Wait.assertEquals(0, snf2::getMessageCount);

      org.apache.activemq.artemis.core.server.Queue serverQueue2 = server2.locateQueue("someQueue");
      Assert.assertNotNull(serverQueue2);
      org.apache.activemq.artemis.core.server.Queue serverQueue1 = server1.locateQueue("someQueue");
      Assert.assertNotNull(serverQueue1);

      Wait.assertEquals(NUMBER_OF_MESSAGES, serverQueue2::getMessageCount);
      Wait.assertEquals(NUMBER_OF_MESSAGES, serverQueue1::getMessageCount);


      ConnectionFactory consumeCF = CFUtil.createConnectionFactory(protocol, consumeURI);
      try (Connection connection = consumeCF.createConnection()) {
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue jmsQueue = session.createQueue("someQueue");
         MessageConsumer consumer = session.createConsumer(jmsQueue);
         for (int i = 0; i < 10; i++) {
            Message recMessage = consumer.receive(5000);
            Assert.assertNotNull(recMessage);
         }
         session.commit();
      }

      Wait.assertEquals(NUMBER_OF_MESSAGES - 10, serverQueue2::getMessageCount);

      LinkedList<MessageReference> refs = new LinkedList<>();
      serverQueue2.forEach((t) -> refs.add(t));

      AMQPMirrorControllerTarget controllerTarget = locateMirrorTarget(server2);

      CountDownLatch latch = new CountDownLatch(refs.size());

      IOCallback callback = new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      };

      for (MessageReference r : refs) {
         Long messageID = (Long)r.getMessage().getBrokerProperty(SimpleString.toSimpleString("x-opt-amq-mr-id"));
         Object nodeID = r.getMessage().getBrokerProperty(SimpleString.toSimpleString("x-opt-amq-bkr-id"));

         // this will force the retry on the queue after a depage happened
         controllerTarget.performAckOnPage(nodeID.toString(), messageID, serverQueue2, callback);
      }

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

      Wait.assertEquals(NUMBER_OF_MESSAGES - 10 - refs.size(), serverQueue2::getMessageCount);
   }

   protected static AMQPMirrorControllerTarget locateMirrorTarget(ActiveMQServer server) {
      ActiveMQServerImpl theServer = (ActiveMQServerImpl) server;

      for (RemotingConnection connection : theServer.getRemotingService().getConnections()) {
         if (connection instanceof ActiveMQProtonRemotingConnection) {
            ActiveMQProtonRemotingConnection protonRC = (ActiveMQProtonRemotingConnection) connection;
            for (AMQPSessionContext sessionContext : protonRC.getAmqpConnection().getSessions().values()) {
               for (ProtonAbstractReceiver receiver : sessionContext.getReceivers().values()) {
                  if (receiver instanceof AMQPMirrorControllerTarget) {
                     return (AMQPMirrorControllerTarget) receiver;
                  }
               }
            }
         }
      }

      return null;
   }

   private int acksCount(File countJournalLocation) throws Exception {
      HashMap<Integer, AtomicInteger> countJournal = countJournal(countJournalLocation, 10485760, 2, 2);
      AtomicInteger acksCount = countJournal.get((int)JournalRecordIds.ACKNOWLEDGE_CURSOR);
      return acksCount != null ? acksCount.get() : 0;
   }

}
