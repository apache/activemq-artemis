/**
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
package org.apache.activemq.tests.integration.client;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.ServiceTestBase;

/**
 * @author Tim Fox
 *
 *
 */
public class SimpleSendMultipleQueuesTest extends ServiceTestBase
{
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
   public void testSimpleSend() throws Exception
   {
      for (int i = 0; i < 1000; i++)
      {
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
   public void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false, true);

      server.start();

      locator = createNettyNonHALocator();

      ClientSessionFactory cf = createSessionFactory(locator);

      session = cf.createSession();

      session.createQueue(SimpleSendMultipleQueuesTest.address, "queue1");
      session.createQueue(SimpleSendMultipleQueuesTest.address, "queue2");
      session.createQueue(SimpleSendMultipleQueuesTest.address, "queue3");

      producer = session.createProducer(SimpleSendMultipleQueuesTest.address);

      consumer1 = session.createConsumer("queue1");

      consumer2 = session.createConsumer("queue2");

      consumer3 = session.createConsumer("queue3");

      session.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (session != null)
      {
         consumer1.close();

         consumer2.close();

         consumer3.close();

         session.deleteQueue("queue1");
         session.deleteQueue("queue2");
         session.deleteQueue("queue3");

         session.close();
      }

      super.tearDown();
   }

}
