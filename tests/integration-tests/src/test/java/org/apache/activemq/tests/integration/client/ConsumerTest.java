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
package org.apache.activemq.tests.integration.client;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.MessageHandler;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.utils.ConcurrentHashSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */

@RunWith(value = Parameterized.class)
public class ConsumerTest extends ServiceTestBase
{
   @Parameterized.Parameters(name = "isNetty={0}")
   public static Collection getParameters()
   {
      return Arrays.asList(new Object[][]{
         {true},
         {false}
      });
   }

   public ConsumerTest(boolean netty)
   {
      this.netty = netty;
   }

   private final boolean netty;
   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private ServerLocator locator;

   protected boolean isNetty()
   {
      return netty;
   }

   @Before
   @Override
   public void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false, isNetty());

      server.start();

      locator = createFactory(isNetty());
   }

   @Test
   public void testStressConnection() throws Exception
   {

      for (int i = 0; i < 10; i++)
      {
         ServerLocator locatorSendx = createFactory(isNetty());
         locatorSendx.setReconnectAttempts(-1);
         ClientSessionFactory factoryx = locatorSendx.createSessionFactory();
         factoryx.close();
         locatorSendx.close();
      }

   }


   @Test
   public void testConsumerAckImmediateAutoCommitTrue() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));

      session.close();
   }

   @Test
   public void testConsumerAckImmediateAutoCommitFalse() throws Exception
   {

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, false, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));

      session.close();
   }

   @Test
   public void testConsumerAckImmediateAckIgnored() throws Exception
   {

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         if (i < 50)
         {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));

      session.close();
   }

   @Test
   public void testConsumerAckImmediateCloseSession() throws Exception
   {

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         if (i < 50)
         {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));

      session.close();

      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));
   }

   @Test
   public void testAcksWithSmallSendWindow() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }
      session.close();
      sf.close();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      server.getRemotingService().addIncomingInterceptor(new Interceptor()
      {
         public boolean intercept(final Packet packet, final RemotingConnection connection) throws ActiveMQException
         {
            if (packet.getType() == PacketImpl.SESS_ACKNOWLEDGE)
            {
               latch.countDown();
            }
            return true;
         }
      });
      ServerLocator locator = createInVMNonHALocator();
      locator.setConfirmationWindowSize(100);
      locator.setAckBatchSize(-1);
      ClientSessionFactory sfReceive = createSessionFactory(locator);
      ClientSession sessionRec = sfReceive.createSession(false, true, true);
      ClientConsumer consumer = sessionRec.createConsumer(QUEUE);
      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage message)
         {
            try
            {
               message.acknowledge();
            }
            catch (ActiveMQException e)
            {
               e.printStackTrace();
            }
         }
      });
      sessionRec.start();
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      sessionRec.close();
      locator.close();
   }

   // https://jira.jboss.org/browse/HORNETQ-410
   @Test
   public void testConsumeWithNoConsumerFlowControl() throws Exception
   {

      ServerLocator locator = createInVMNonHALocator();

      locator.setConsumerWindowSize(-1);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      session.start();

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(10000);
         assertNotNull(message);
         message.acknowledge();
      }

      session.close();
      sf.close();
      locator.close();

   }

   @Test
   public void testClearListener() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientConsumer consumer = session.createConsumer(QUEUE);

      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage msg)
         {
         }
      });

      consumer.setMessageHandler(null);
      consumer.receiveImmediate();

      session.close();
   }

   @Test
   public void testNoReceiveWithListener() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientConsumer consumer = session.createConsumer(QUEUE);

      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage msg)
         {
         }
      });

      try
      {
         consumer.receiveImmediate();
         Assert.fail("Should throw exception");
      }
      catch (ActiveMQIllegalStateException ise)
      {
         //ok
      }
      catch (ActiveMQException me)
      {
         Assert.fail("Wrong exception code");
      }

      session.close();
   }


   @Test
   public void testReceiveAndResend() throws Exception
   {

      final Set<Object> sessions = new ConcurrentHashSet<Object>();
      final AtomicInteger errors = new AtomicInteger(0);

      final SimpleString QUEUE_RESPONSE = SimpleString.toSimpleString("QUEUE_RESPONSE");

      final int numberOfSessions = 50;
      final int numberOfMessages = 10;

      final CountDownLatch latchReceive = new CountDownLatch(numberOfSessions * numberOfMessages);

      ClientSessionFactory sf = locator.createSessionFactory();
      for (int i = 0; i < numberOfSessions; i++)
      {

         ClientSession session = sf.createSession(false, true, true);

         sessions.add(session);

         session.createQueue(QUEUE, QUEUE.concat("" + i), null, false);

         if (i == 0)
         {
            session.createQueue(QUEUE_RESPONSE, QUEUE_RESPONSE);
         }


         ClientConsumer consumer = session.createConsumer(QUEUE.concat("" + i));
         sessions.add(consumer);

         {

            consumer.setMessageHandler(new MessageHandler()
            {
               public void onMessage(final ClientMessage msg)
               {
                  try
                  {
                     ServerLocator locatorSendx = createFactory(isNetty());
                     locatorSendx.setReconnectAttempts(-1);
                     ClientSessionFactory factoryx = locatorSendx.createSessionFactory();
                     ClientSession sessionSend = factoryx.createSession(true, true);

                     sessions.add(sessionSend);
                     sessions.add(locatorSendx);
                     sessions.add(factoryx);


                     final ClientProducer prod = sessionSend.createProducer(QUEUE_RESPONSE);
                     sessionSend.start();

                     sessions.add(prod);

                     msg.acknowledge();
                     prod.send(sessionSend.createMessage(true));
                     prod.close();
                     sessionSend.commit();
                     sessionSend.close();
                     factoryx.close();
                     if (Thread.currentThread().isInterrupted())
                     {
                        System.err.println("Netty has interrupted a thread!!!");
                        errors.incrementAndGet();
                     }

                  }
                  catch (Throwable e)
                  {
                     e.printStackTrace();
                     errors.incrementAndGet();
                  }
                  finally
                  {
                     latchReceive.countDown();
                  }
               }
            });
         }

         session.start();
      }


      Thread tCons = new Thread()
      {
         public void run()
         {
            try
            {
               final ServerLocator locatorSend = createFactory(isNetty());
               final ClientSessionFactory factory = locatorSend.createSessionFactory();
               final ClientSession sessionSend = factory.createSession(true, true);
               ClientConsumer cons = sessionSend.createConsumer(QUEUE_RESPONSE);
               sessionSend.start();

               for (int i = 0; i < numberOfMessages * numberOfSessions; i++)
               {
                  ClientMessage msg = cons.receive(5000);
                  if (msg == null)
                  {
                     break;
                  }
                  msg.acknowledge();
               }

               if (cons.receiveImmediate() != null)
               {
                  System.out.println("ERROR: Received an extra message");
                  errors.incrementAndGet();
               }
               sessionSend.close();
               factory.close();
               locatorSend.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
               errors.incrementAndGet();

            }

         }
      };

      tCons.start();

      ClientSession mainSessionSend = sf.createSession(true, true);
      ClientProducer mainProd = mainSessionSend.createProducer(QUEUE);

      for (int i = 0; i < numberOfMessages; i++)
      {
         mainProd.send(mainSessionSend.createMessage(true));
      }


      latchReceive.await(2, TimeUnit.MINUTES);


      tCons.join();

      sf.close();

      assertEquals("Had errors along the execution", 0, errors.get());
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-111
   // Test that, on rollback credits are released for messages cleared in the buffer
   @Test
   public void testConsumerCreditsOnRollback() throws Exception
   {
      locator.setConsumerWindowSize(10000);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createTransactedSession();

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      final byte[] bytes = new byte[1000];

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         message.putIntProperty("count", i);

         producer.send(message);
      }

      session.commit();

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();

      for (int i = 0; i < 110; i++)
      {
         ClientMessage message = consumer.receive();

         int count = message.getIntProperty("count");

         boolean redelivered = message.getDeliveryCount() > 1;

         if (count % 2 == 0 && !redelivered)
         {
            session.rollback();
         }
         else
         {
            session.commit();
         }
      }

      session.close();
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-111
   // Test that, on rollback credits are released for messages cleared in the buffer
   @Test
   public void testConsumerCreditsOnRollbackLargeMessages() throws Exception
   {

      locator.setConsumerWindowSize(10000);
      locator.setMinLargeMessageSize(1000);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createTransactedSession();

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      final byte[] bytes = new byte[10000];

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         message.putIntProperty("count", i);

         producer.send(message);
      }

      session.commit();

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();

      for (int i = 0; i < 110; i++)
      {
         ClientMessage message = consumer.receive();

         int count = message.getIntProperty("count");

         boolean redelivered = message.getDeliveryCount() > 1;

         if (count % 2 == 0 && !redelivered)
         {
            session.rollback();
         }
         else
         {
            session.commit();
         }
      }

      session.close();
   }

}
