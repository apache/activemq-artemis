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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RequestorTest extends ActiveMQTestBase {

   private ActiveMQServer server;
   private ClientSessionFactory sf;
   private ServerLocator locator;

   @Test
   public void testRequest() throws Exception {
      final SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();
      SimpleString requestAddress = new SimpleString("AdTest");
      SimpleString requestQueue = RandomUtil.randomSimpleString();

      final ClientSession session = sf.createSession(false, true, true);

      session.start();

      session.createTemporaryQueue(requestAddress, requestQueue);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, session));

      ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createMessage(false);
      request.putLongProperty(key, value);

      ClientMessage reply = requestor.request(request, 500);
      Assert.assertNotNull("reply was not received", reply);
      Assert.assertEquals(value, reply.getObjectProperty(key));

      Thread.sleep(5000);
      session.close();
   }

   @Test
   public void testManyRequestsOverBlocked() throws Exception {
      final SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();

      AddressSettings settings = new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK).setMaxSizeBytes(1024);
      server.getAddressSettingsRepository().addMatch("#", settings);

      SimpleString requestAddress = new SimpleString("RequestAddress");

      SimpleString requestQueue = new SimpleString("RequestAddress Queue");

      final ClientSession sessionRequest = sf.createSession(false, true, true);

      sessionRequest.createQueue(requestAddress, requestQueue);

      sessionRequest.start();

      ClientConsumer requestConsumer = sessionRequest.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, sessionRequest));

      for (int i = 0; i < 2000; i++) {
         if (i % 100 == 0) {
            System.out.println(i);
         }
         final ClientSession session = sf.createSession(false, true, true);

         session.start();

         ClientRequestor requestor = new ClientRequestor(session, requestAddress);
         ClientMessage request = session.createMessage(false);
         request.putLongProperty(key, value);

         ClientMessage reply = requestor.request(request, 5000);
         Assert.assertNotNull("reply was not received", reply);
         reply.acknowledge();
         Assert.assertEquals(value, reply.getObjectProperty(key));
         requestor.close();
         session.close();
      }

      sessionRequest.close();

   }

   @Test
   public void testTwoRequests() throws Exception {
      final SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();
      SimpleString requestAddress = RandomUtil.randomSimpleString();
      SimpleString requestQueue = RandomUtil.randomSimpleString();

      ClientSessionFactory sf = createSessionFactory(locator);
      final ClientSession session = sf.createSession(false, true, true);

      session.start();

      session.createTemporaryQueue(requestAddress, requestQueue);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, session));

      ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createMessage(false);
      request.putLongProperty(key, value);

      ClientMessage reply = requestor.request(request, 500);
      Assert.assertNotNull("reply was not received", reply);
      Assert.assertEquals(value, reply.getObjectProperty(key));

      request = session.createMessage(false);
      request.putLongProperty(key, value + 1);

      reply = requestor.request(request, 500);
      Assert.assertNotNull("reply was not received", reply);
      Assert.assertEquals(value + 1, reply.getObjectProperty(key));

      session.close();
   }

   @Test
   public void testRequestWithRequestConsumerWhichDoesNotReply() throws Exception {
      SimpleString requestAddress = RandomUtil.randomSimpleString();
      SimpleString requestQueue = RandomUtil.randomSimpleString();

      ClientSessionFactory sf = createSessionFactory(locator);
      final ClientSession session = sf.createSession(false, true, true);

      session.start();

      session.createTemporaryQueue(requestAddress, requestQueue);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new MessageHandler() {
         // return a message with the negative request's value
         @Override
         public void onMessage(final ClientMessage request) {
            // do nothing -> no reply
         }
      });

      ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createMessage(false);

      ClientMessage reply = requestor.request(request, 500);
      Assert.assertNull(reply);

      session.close();
   }

   @Test
   public void testClientRequestorConstructorWithClosedSession() throws Exception {
      final SimpleString requestAddress = RandomUtil.randomSimpleString();

      ClientSessionFactory sf = createSessionFactory(locator);
      final ClientSession session = sf.createSession(false, true, true);

      session.close();

      ActiveMQAction activeMQAction = new ActiveMQAction() {
         @Override
         public void run() throws Exception {
            new ClientRequestor(session, requestAddress);
         }
      };

      ActiveMQTestBase.expectActiveMQException("ClientRequestor's session must not be closed", ActiveMQExceptionType.OBJECT_CLOSED, activeMQAction);
   }

   @Test
   public void testClose() throws Exception {
      final SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();
      SimpleString requestAddress = RandomUtil.randomSimpleString();
      SimpleString requestQueue = RandomUtil.randomSimpleString();

      ClientSessionFactory sf = createSessionFactory(locator);
      final ClientSession session = sf.createSession(false, true, true);

      session.start();

      session.createTemporaryQueue(requestAddress, requestQueue);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, session));

      final ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createMessage(false);
      request.putLongProperty(key, value);

      ClientMessage reply = requestor.request(request, 500);
      Assert.assertNotNull("reply was not received", reply);
      Assert.assertEquals(value, reply.getObjectProperty(key));

      request = session.createMessage(false);
      request.putLongProperty(key, value + 1);

      requestor.close();

      ActiveMQAction activeMQAction = new ActiveMQAction() {
         @Override
         public void run() throws Exception {
            requestor.request(session.createMessage(false), 500);
         }
      };

      ActiveMQTestBase.expectActiveMQException("can not send a request on a closed ClientRequestor", ActiveMQExceptionType.OBJECT_CLOSED, activeMQAction);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(false, createDefaultInVMConfig());
      server.start();
      locator = createInVMNonHALocator().setAckBatchSize(0);
      sf = createSessionFactory(locator);
   }

   private final class SimpleMessageHandler implements MessageHandler {

      private final SimpleString key;

      private final ClientSession session;

      private SimpleMessageHandler(final SimpleString key, final ClientSession session) {
         this.key = key;
         this.session = session;
      }

      @Override
      public void onMessage(final ClientMessage request) {
         try {
            ClientMessage reply = session.createMessage(false);
            SimpleString replyTo = (SimpleString) request.getObjectProperty(ClientMessageImpl.REPLYTO_HEADER_NAME);
            long value = (Long) request.getObjectProperty(key);
            reply.putLongProperty(key, value);
            ClientProducer replyProducer = session.createProducer(replyTo);
            replyProducer.send(reply);
            request.acknowledge();
         } catch (ActiveMQException e) {
            e.printStackTrace();
         }
      }
   }
}
