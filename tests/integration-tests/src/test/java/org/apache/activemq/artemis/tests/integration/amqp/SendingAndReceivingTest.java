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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SendingAndReceivingTest extends ActiveMQTestBase {

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
      server.start();
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
         sender.setTimeToLive(10);

         Message message = session.createMessage();
         sender.send(message);
         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);
         Message m = consumer.receive(5000);
         Assert.assertNull(m);
         consumer.close();

         consumer = session.createConsumer(session.createQueue("DLQ"));
         m = consumer.receive(5000);
         Assert.assertNotNull(m);
         consumer.close();
      } finally {
         if (connection != null) {
            connection.close();
         }
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
