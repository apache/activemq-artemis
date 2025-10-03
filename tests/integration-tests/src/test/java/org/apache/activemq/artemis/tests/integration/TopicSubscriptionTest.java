/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.activemq.artemis.tests.integration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;
import java.lang.invoke.MethodHandles;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQMessageConsumer;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.SingleServerTestBase;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A simple test-case used for documentation purposes.
 */
public class TopicSubscriptionTest extends SingleServerTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected ActiveMQServer createServer() throws Exception {
      return createServer(true, createDefaultNettyConfig());
   }


   /**
    * This is simulating a context that will never finish, the timeout should take care of it.
    */
   @Test
   public void testBlockedContext() throws Exception {

      int numberOfMessages = 100;

      String queueName = getName();

      org.apache.activemq.artemis.core.server.Queue serverQueue = server.createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));
      ConnectionFactory connectionFactory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = connectionFactory.createConnection()) {
         connection.setClientID("clientID");
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(queueName);

         ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(queue);

         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < numberOfMessages; i++) {
            TextMessage message = session.createTextMessage("hello " + i);
            message.setIntProperty("i", i);
            producer.send(message);
         }
         session.commit();

         Wait.assertEquals(numberOfMessages, serverQueue::getMessageCount, 5000);

         Wait.waitFor(() -> serverQueue.getDeliveringCount() > 1);
         assertNotNull(consumer.receive(5000));

         server.getRemotingService().getConnections().stream().filter(r -> String.valueOf(r.getClientID()).equals("clientID")).forEach(r -> {
            server.getSessions().stream().filter(s -> s.getRemotingConnection() == r).forEach(s -> {
               // this will make the context to never finish
               s.getSessionContext().storeLineUp();
            });
            r.fail(new ActiveMQException("fail"));
            r.getTransportConnection().disconnect();
         });
      }

      Wait.assertEquals(numberOfMessages, serverQueue::getMessageCount);
      Wait.assertEquals(0, serverQueue::getDeliveringCount);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
         for (int i = 0; i < numberOfMessages; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertNotNull(message);
            assertEquals(i, message.getIntProperty("i"));
         }
      }
   }
}