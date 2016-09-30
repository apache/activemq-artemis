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

import org.apache.activemq.artemis.api.core.ActiveMQInvalidTransientQueueUseException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.tests.util.SingleServerTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Test;

public class TransientQueueTest extends SingleServerTestBase {

   @Test
   public void testSimpleTransientQueue() throws Exception {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createSharedQueue(address, queue, false);
      assertEquals(1, server.getConnectionCount());

      // we create a second session. the temp queue must be present
      // even after we closed the session which created it
      ClientSession session2 = sf.createSession(false, true, true);

      session.close();

      session2.start();

      session2.createConsumer(queue);

      session2.close();
   }

   @Test
   public void testMultipleConsumers() throws Exception {
      SimpleString queue = SimpleString.toSimpleString("queue");
      SimpleString address = SimpleString.toSimpleString("address");

      session.createSharedQueue(address, queue, false);
      assertEquals(1, server.getConnectionCount());

      assertNotNull(server.locateQueue(queue));

      ServerLocator locator2 = createLocator();
      ClientSessionFactory sf2 = locator2.createSessionFactory();
      ClientSession session2 = sf2.createSession(false, false);

      // At this point this has no effect, other than making sure the queue exists...
      // the JMS implementation will certainly create the queue again when dealing with
      // non durable shared subscriptions
      session2.createSharedQueue(address, queue, false);

      ClientConsumer consumer1 = session.createConsumer(queue);
      ClientConsumer consumer2 = session2.createConsumer(queue);

      session.start();
      session2.start();

      ClientProducer producer = session.createProducer(address);

      for (int i = 0; i < 1000; i++) {
         ClientMessage msg = session.createMessage(false);
         producer.send(msg);
      }

      ClientMessage msg;
      for (int i = 0; i < 500; i++) {
         msg = consumer1.receive(1000);
         assertNotNull(msg);

         msg.acknowledge();
         msg = consumer2.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      assertNull(consumer1.receiveImmediate());
      assertNull(consumer2.receiveImmediate());

      for (int i = 0; i < 1000; i++) {
         msg = session.createMessage(false);
         producer.send(msg);
      }

      msg = consumer1.receiveImmediate();
      assertNotNull(msg);
      msg.acknowledge();

      msg = consumer2.receiveImmediate();
      assertNotNull(msg);
      msg.acknowledge();

      consumer1.close();

      consumer2.close();

      session.createSharedQueue(address, queue, false);

      consumer1 = session.createConsumer(queue);

      // validate if the queue was deleted after the consumer was closed
      assertNotNull(server.locateQueue(queue));

      assertNull(consumer1.receiveImmediate());

      consumer1.close();

      assertNull(server.locateQueue(queue));

   }

   @Test
   public void testQueueDifferentConfigs() throws Exception {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      server.locateQueue(queue);
      SimpleString address2 = RandomUtil.randomSimpleString();

      session.createSharedQueue(address, queue, false);
      assertEquals(1, server.getConnectionCount());

      ServerLocator locator2 = createLocator();
      ClientSessionFactory sf2 = locator2.createSessionFactory();
      ClientSession session2 = sf2.createSession(false, false);
      addClientSession(session2);

      boolean exHappened = false;

      try {
         // There's already a queue with that name, we are supposed to throw an exception
         session2.createSharedQueue(address2, queue, false);
      } catch (ActiveMQInvalidTransientQueueUseException e) {
         exHappened = true;
      }

      assertTrue(exHappened);

      exHappened = false;

      try {
         // There's already a queue with that name, we are supposed to throw an exception
         session2.createSharedQueue(address, queue, SimpleString.toSimpleString("a=1"), false);
      } catch (ActiveMQInvalidTransientQueueUseException e) {
         exHappened = true;
      }

      assertTrue(exHappened);

      // forcing a consumer close to make the queue go away
      session.createConsumer(queue).close();

      assertNull(server.locateQueue(queue));

      session.createSharedQueue(address, queue, SimpleString.toSimpleString("q=1"), false);

      exHappened = false;

      try {
         // There's already a queue with that name, we are supposed to throw an exception
         session2.createSharedQueue(address, queue, SimpleString.toSimpleString("q=2"), false);
      } catch (ActiveMQInvalidTransientQueueUseException e) {
         exHappened = true;
      }

      assertTrue(exHappened);

      exHappened = false;
      try {
         // There's already a queue with that name, we are supposed to throw an exception
         session2.createSharedQueue(address, queue, false);
      } catch (ActiveMQInvalidTransientQueueUseException e) {
         exHappened = true;
      }

      assertTrue(exHappened);
   }

   @Override
   protected ServerLocator createLocator() {
      return super.createLocator().setConsumerWindowSize(0).setBlockOnAcknowledge(true).setBlockOnDurableSend(false).setBlockOnNonDurableSend(false);
   }
}
