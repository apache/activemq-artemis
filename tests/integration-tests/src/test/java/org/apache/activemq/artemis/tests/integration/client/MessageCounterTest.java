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

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MessageCounterTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);
      server.start();

      locator = createInVMNonHALocator();
   }

   @Test
   public void testMessageCounter() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);

      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

      session.createQueue(new QueueConfiguration(QUEUE).setDurable(false));

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      session.commit();
      session.start();

      Assert.assertEquals(100, getMessageCount(server.getPostOffice(), QUEUE.toString()));

      ClientConsumer consumer = session.createConsumer(QUEUE, null, false);

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull(message);
         message.acknowledge();

         session.commit();

         Assert.assertEquals("m" + i, message.getBodyBuffer().readString());
      }

      session.close();

      Assert.assertEquals(0, getMessageCount(server.getPostOffice(), QUEUE.toString()));

   }

}
