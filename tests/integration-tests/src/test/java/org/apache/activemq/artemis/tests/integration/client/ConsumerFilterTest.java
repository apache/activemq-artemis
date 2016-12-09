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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class ConsumerFilterTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private ActiveMQServer server;
   private ClientSession session;
   private ClientProducer producer;
   private ClientConsumer consumer;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);

      server.start();
      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      session = sf.createSession();

      session.start();
      session.createQueue("foo", RoutingType.ANYCAST, "foo");

      producer = session.createProducer("foo");
      consumer = session.createConsumer("foo", "animal='giraffe'");
   }

   @Test
   public void testLargeToken() throws Exception {
      StringBuffer token = new StringBuffer();

      token.append("'");
      for (int i = 0; i < 5000; i++) {
         token.append("a");
      }
      token.append("'");

      // The server would fail to create this consumer if HORNETQ-545 wasn't solved
      consumer = session.createConsumer("foo", "animal=" + token.toString());
   }

   @Test
   public void testNonMatchingMessagesFollowedByMatchingMessages() throws Exception {

      ClientMessage message = session.createMessage(false);

      message.putStringProperty("animal", "hippo");

      producer.send(message);

      assertNull(consumer.receiveImmediate());

      message = session.createMessage(false);

      message.putStringProperty("animal", "giraffe");

      log.info("sending second msg");

      producer.send(message);

      ClientMessage received = consumer.receiveImmediate();

      assertNotNull(received);

      assertEquals("giraffe", received.getStringProperty("animal"));

      assertNull(consumer.receiveImmediate());

      session.close();
   }

   @Test
   public void testNonMatchingMessagesFollowedByMatchingMessagesMany() throws Exception {

      for (int i = 0; i < QueueImpl.MAX_DELIVERIES_IN_LOOP * 2; i++) {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty("animal", "hippo");

         producer.send(message);
      }

      assertNull(consumer.receiveImmediate());

      for (int i = 0; i < QueueImpl.MAX_DELIVERIES_IN_LOOP * 2; i++) {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty("animal", "giraffe");

         producer.send(message);
      }

      for (int i = 0; i < QueueImpl.MAX_DELIVERIES_IN_LOOP * 2; i++) {
         ClientMessage received = consumer.receiveImmediate();

         assertNotNull(received);

         assertEquals("giraffe", received.getStringProperty("animal"));
      }

      assertNull(consumer.receiveImmediate());

      session.close();
   }

   @Test
   public void testTwoConsumers() throws Exception {
      ClientConsumer consumer2 = session.createConsumer("foo", "animal='elephant'");

      //Create and consume message that matches the first consumer's filter

      ClientMessage message = session.createMessage(false);

      message.putStringProperty("animal", "giraffe");

      producer.send(message);

      ClientMessage received = consumer.receive(10000);

      assertNotNull(received);

      assertEquals("giraffe", received.getStringProperty("animal"));

      assertNull(consumer.receiveImmediate());
      assertNull(consumer2.receiveImmediate());

      //Create and consume another message that matches the first consumer's filter
      message = session.createMessage(false);

      message.putStringProperty("animal", "giraffe");

      producer.send(message);

      received = consumer.receive(10000);

      assertNotNull(received);

      assertEquals("giraffe", received.getStringProperty("animal"));

      assertNull(consumer.receiveImmediate());
      assertNull(consumer2.receiveImmediate());

      //Create and consume a message that matches the second consumer's filter

      message = session.createMessage(false);

      message.putStringProperty("animal", "elephant");

      producer.send(message);

      received = consumer2.receive(10000);

      assertNotNull(received);

      assertEquals("elephant", received.getStringProperty("animal"));

      assertNull(consumer.receiveImmediate());
      assertNull(consumer2.receiveImmediate());

      //Create and consume another message that matches the second consumer's filter

      message = session.createMessage(false);

      message.putStringProperty("animal", "elephant");

      producer.send(message);

      received = consumer2.receive(1000);

      assertNotNull(received);

      assertEquals("elephant", received.getStringProperty("animal"));

      assertNull(consumer.receiveImmediate());
      assertNull(consumer2.receiveImmediate());

      session.close();
   }

   @Test
   public void testLinkedListOrder() throws Exception {
      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession();

      session.start();

      ClientProducer producer = session.createProducer("foo");

      ClientConsumer redConsumer = session.createConsumer("foo", "color='red'");

      ClientConsumer anyConsumer = session.createConsumer("foo");

      sendMessage(session, producer, "any", "msg1");

      sendMessage(session, producer, "any", "msg2");

      sendMessage(session, producer, "any", "msg3");

      sendMessage(session, producer, "red", "msgRed4");

      sendMessage(session, producer, "red", "msgRed5");

      readConsumer("anyConsumer", anyConsumer);

      readConsumer("anyConsumer", anyConsumer);

      log.info("### closing consumer ###");

      anyConsumer.close();

      readConsumer("redConsumer", redConsumer);

      readConsumer("redConsumer", redConsumer);

      log.info("### recreating consumer ###");

      anyConsumer = session.createConsumer("foo");

      session.start();

      readConsumer("anyConsumer", anyConsumer);

      session.close();

      sf.close();

      locator.close();
   }

   /**
    * @param consumer
    * @throws Exception
    */
   private void readConsumer(String consumerName, ClientConsumer consumer) throws Exception {
      ClientMessage message = consumer.receive(5000);
      assertNotNull(message);
      System.out.println("consumer = " + consumerName + " message, color=" + message.getStringProperty("color") + ", msg = " + message.getStringProperty("value"));
      message.acknowledge();
   }

   /**
    * @param session
    * @param producer
    * @throws Exception
    */
   private void sendMessage(ClientSession session, ClientProducer producer, String color, String msg) throws Exception {
      ClientMessage anyMessage = session.createMessage(true);
      anyMessage.putStringProperty("color", color);
      anyMessage.putStringProperty("value", msg);
      producer.send(anyMessage);
      session.commit();
   }
}
