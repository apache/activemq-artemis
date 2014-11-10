/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.client;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientRequestor;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.TransportConfigurationUtils;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A ClientRequestorTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class RequestorTest extends ServiceTestBase
{

   private HornetQServer service;
   private ClientSessionFactory sf;
   private ServerLocator locator;

   @Test
   public void testRequest() throws Exception
   {
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
   public void testManyRequestsOverBlocked() throws Exception
   {
      final SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();

      AddressSettings settings = new AddressSettings();
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      settings.setMaxSizeBytes(1024);
      service.getAddressSettingsRepository().addMatch("#", settings);

      SimpleString requestAddress = new SimpleString("RequestAddress");

      SimpleString requestQueue = new SimpleString("RequestAddress Queue");

      final ClientSession sessionRequest = sf.createSession(false, true, true);

      sessionRequest.createQueue(requestAddress, requestQueue);

      sessionRequest.start();

      ClientConsumer requestConsumer = sessionRequest.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, sessionRequest));


      for (int i = 0; i < 2000; i++)
      {
         if (i % 100 == 0)
         {
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
   public void testTwoRequests() throws Exception
   {
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
   public void testRequestWithRequestConsumerWhichDoesNotReply() throws Exception
   {
      SimpleString requestAddress = RandomUtil.randomSimpleString();
      SimpleString requestQueue = RandomUtil.randomSimpleString();

      ClientSessionFactory sf = createSessionFactory(locator);
      final ClientSession session = sf.createSession(false, true, true);

      session.start();

      session.createTemporaryQueue(requestAddress, requestQueue);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new MessageHandler()
      {
         // return a message with the negative request's value
         public void onMessage(final ClientMessage request)
         {
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
   public void testClientRequestorConstructorWithClosedSession() throws Exception
   {
      final SimpleString requestAddress = RandomUtil.randomSimpleString();

      ClientSessionFactory sf = createSessionFactory(locator);
      final ClientSession session = sf.createSession(false, true, true);

      session.close();

      HornetQAction hornetQAction = new HornetQAction()
      {
         public void run() throws Exception
         {
            new ClientRequestor(session, requestAddress);
         }
      };

      UnitTestCase.expectHornetQException("ClientRequestor's session must not be closed",
                                          HornetQExceptionType.OBJECT_CLOSED,
                                          hornetQAction);
   }

   @Test
   public void testClose() throws Exception
   {
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

      HornetQAction hornetQAction = new HornetQAction()
      {
         public void run() throws Exception
         {
            requestor.request(session.createMessage(false), 500);
         }
      };

      UnitTestCase.expectHornetQException("can not send a request on a closed ClientRequestor",
                                          HornetQExceptionType.OBJECT_CLOSED, hornetQAction);
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createDefaultConfig()
         .addAcceptorConfiguration(TransportConfigurationUtils.getInVMAcceptor(true));
      service = createServer(false, conf);
      service.start();

      locator = createInVMNonHALocator();
      locator.setAckBatchSize(0);
      sf = createSessionFactory(locator);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      locator = null;

      sf = null;

      service = null;

      super.tearDown();
   }

   private final class SimpleMessageHandler implements MessageHandler
   {
      private final SimpleString key;

      private final ClientSession session;

      private SimpleMessageHandler(final SimpleString key, final ClientSession session)
      {
         this.key = key;
         this.session = session;
      }

      public void onMessage(final ClientMessage request)
      {
         try
         {
            ClientMessage reply = session.createMessage(false);
            SimpleString replyTo = (SimpleString) request.getObjectProperty(ClientMessageImpl.REPLYTO_HEADER_NAME);
            long value = (Long) request.getObjectProperty(key);
            reply.putLongProperty(key, value);
            ClientProducer replyProducer = session.createProducer(replyTo);
            replyProducer.send(reply);
            request.acknowledge();
         }
         catch (HornetQException e)
         {
            e.printStackTrace();
         }
      }
   }
}
