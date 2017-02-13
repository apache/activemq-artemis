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
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SimpleSendMultipleQueuesTest extends ActiveMQTestBase {

   public static final String address = "testaddress";

   public static final String queueName = "testqueue";

   private ActiveMQServer server;

   private ClientSession session;

   private ClientProducer producer;

   private ClientConsumer consumer1;

   private ClientConsumer consumer2;

   private ClientConsumer consumer3;

   private ServerLocator locator;

   @Test
   public void testSimpleSend() throws Exception {
      for (int i = 0; i < 1000; i++) {
         ClientMessage message = session.createMessage(false);

         final String body = RandomUtil.randomString();

         message.getBodyBuffer().writeString(body);

         //  log.info("sending message");
         producer.send(message);
         // log.info("sent message");

         ClientMessage received1 = consumer1.receive(1000);
         Assert.assertNotNull(received1);
         Assert.assertEquals(body, received1.getBodyBuffer().readString());

         ClientMessage received2 = consumer2.receive(1000);
         Assert.assertNotNull(received2);
         Assert.assertEquals(body, received2.getBodyBuffer().readString());

         ClientMessage received3 = consumer3.receive(1000);
         Assert.assertNotNull(received3);
         Assert.assertEquals(body, received3.getBodyBuffer().readString());
      }
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false, true);

      server.start();

      locator = createNettyNonHALocator();

      ClientSessionFactory cf = createSessionFactory(locator);

      session = cf.createSession();

      session.createQueue(SimpleSendMultipleQueuesTest.address, RoutingType.MULTICAST, "queue1");
      session.createQueue(SimpleSendMultipleQueuesTest.address, RoutingType.MULTICAST, "queue2");
      session.createQueue(SimpleSendMultipleQueuesTest.address, RoutingType.MULTICAST, "queue3");

      producer = session.createProducer(SimpleSendMultipleQueuesTest.address);

      consumer1 = session.createConsumer("queue1");

      consumer2 = session.createConsumer("queue2");

      consumer3 = session.createConsumer("queue3");

      session.start();
   }
}
