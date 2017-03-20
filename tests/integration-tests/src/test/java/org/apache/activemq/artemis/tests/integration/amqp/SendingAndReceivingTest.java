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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SendingAndReceivingTest extends AmqpTestSupport {

   private ActiveMQServer server;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, true);
      Set<TransportConfiguration> acceptors = server.getConfiguration().getAcceptorConfigurations();
      for (TransportConfiguration tc : acceptors) {
         if (tc.getName().equals("netty")) {
            tc.getExtraParams().put("anycastPrefix", "anycast://");
            tc.getExtraParams().put("multicastPrefix", "multicast://");
         }
      }
      server.getConfiguration().setMessageExpiryScanPeriod(1);
      server.start();
      server.createQueue(SimpleString.toSimpleString("exampleQueue"), RoutingType.ANYCAST, SimpleString.toSimpleString("exampleQueue"), null, true, false, -1, false, true);
      server.createQueue(SimpleString.toSimpleString("DLQ"), RoutingType.ANYCAST, SimpleString.toSimpleString("DLQ"), null, true, false, -1, false, true);

      AddressSettings as = new AddressSettings();
      as.setExpiryAddress(SimpleString.toSimpleString("DLQ"));
      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch("#", as);
   }

   @After
   @Override
   public void tearDown() throws Exception {
      try {
         server.stop();
      } finally {
         super.tearDown();
      }
   }

   //https://issues.apache.org/jira/browse/ARTEMIS-214
   @Test
   public void testSendingBigMessage() throws Exception {
      Connection connection = null;
      ConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:61616");

      try {
         connection = connectionFactory.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("anycast://exampleQueue");
         MessageProducer sender = session.createProducer(queue);

         String body = createMessage(10240);
         sender.send(session.createTextMessage(body));
         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);
         TextMessage m = (TextMessage) consumer.receive(5000);

         Assert.assertEquals(body, m.getText());
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testSendMessageThenAllowToExpireUsingTimeToLive() throws Exception {
      AddressSettings as = new AddressSettings();
      as.setExpiryAddress(SimpleString.toSimpleString("DLQ"));
      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch("exampleQueue", as);

      Connection connection = null;
      ConnectionFactory connectionFactory = new JmsConnectionFactory("amqp://localhost:61616");

      try {
         connection = connectionFactory.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         String address = "exampleQueue";
         Queue queue = session.createQueue(address);

         MessageProducer sender = session.createProducer(queue);
         sender.setTimeToLive(1);

         Message message = session.createMessage();
         sender.send(message);
         connection.start();

         MessageConsumer consumer = session.createConsumer(session.createQueue("DLQ"));
         Message m = consumer.receive(10000);
         Assert.assertNotNull(m);
         consumer.close();


         consumer = session.createConsumer(queue);
         m = consumer.receiveNoWait();
         Assert.assertNull(m);
         consumer.close();


      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testSendExpiry() throws Throwable {
      internalSendExpiry(false);
   }

   @Test(timeout = 60000)
   public void testSendExpiryRestartServer() throws Throwable {
      internalSendExpiry(true);
   }

   public void internalSendExpiry(boolean restartServer) throws Throwable {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();

      try {

         // Normal Session which won't create an TXN itself
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender("exampleQueue");

         AmqpMessage message = new AmqpMessage();
         message.setDurable(true);
         message.setText("Test-Message");
         message.setDeliveryAnnotation("shouldDisappear", 1);
         message.setAbsoluteExpiryTime(System.currentTimeMillis() + 1000);
         sender.send(message);

         org.apache.activemq.artemis.core.server.Queue dlq = server.locateQueue(SimpleString.toSimpleString("DLQ"));

         Wait.waitFor(() -> dlq.getMessageCount() > 0, 5000, 500);

         connection.close();

         if (restartServer) {
            server.stop();
            server.start();
         }

         connection = client.connect();
         session = connection.createSession();

         // Read all messages from the Queue, do not accept them yet.
         AmqpReceiver receiver = session.createReceiver("DLQ");
         receiver.flow(20);
         message = receiver.receive(5, TimeUnit.SECONDS);
         Assert.assertNotNull(message);
         Assert.assertEquals("exampleQueue", message.getMessageAnnotation(org.apache.activemq.artemis.api.core.Message.HDR_ORIGINAL_ADDRESS.toString()));
         Assert.assertNull(message.getDeliveryAnnotation("shouldDisappear"));
         Assert.assertNull(receiver.receiveNoWait());

      } finally {
         connection.close();
      }
   }



   private static String createMessage(int messageSize) {
      final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
      Random rnd = new Random();
      StringBuilder sb = new StringBuilder(messageSize);
      for (int j = 0; j < messageSize; j++) {
         sb.append(AB.charAt(rnd.nextInt(AB.length())));
      }
      String body = sb.toString();
      return body;
   }

}
