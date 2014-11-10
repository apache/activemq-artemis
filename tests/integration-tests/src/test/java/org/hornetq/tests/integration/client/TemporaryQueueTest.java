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

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQDisconnectedException;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQIOErrorException;
import org.hornetq.api.core.HornetQInternalErrorException;
import org.hornetq.api.core.HornetQNonExistentQueueException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientProducerImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.SingleServerTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * A TemporaryQueueTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author Clebert Suconic
 */
public class TemporaryQueueTest extends SingleServerTestBase
{
   // Constants -----------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private static final long CONNECTION_TTL = 2000;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testConsumeFromTemporaryQueue() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage msg = session.createMessage(false);

      producer.send(msg);

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      message.acknowledge();

      consumer.close();
      session.deleteQueue(queue);

      session.close();
   }


   @Test
   public void testMemoryLeakOnAddressSettingForTemporaryQueue() throws Exception
   {
      for (int i = 0; i < 1000; i++)
      {
         SimpleString queue = RandomUtil.randomSimpleString();
         SimpleString address = RandomUtil.randomSimpleString();
         session.createTemporaryQueue(address, queue);

         session.close();
         session = sf.createSession();
      }


      session.close();

      sf.close();

      System.out.println("size = " + server.getAddressSettingsRepository().getCacheSize());

      assertTrue(server.getAddressSettingsRepository().getCacheSize() < 10);
   }

   @Test
   public void testPaginStoreIsRemovedWhenQueueIsDeleted() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage msg = session.createMessage(false);

      producer.send(msg);

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      message.acknowledge();

      SimpleString[] storeNames = server.getPagingManager().getStoreNames();
      assertTrue(Arrays.asList(storeNames).contains(address));

      consumer.close();
      session.deleteQueue(queue);

      storeNames = server.getPagingManager().getStoreNames();
      assertFalse(Arrays.asList(storeNames).contains(address));

      session.close();
   }

   @Test
   public void testConsumeFromTemporaryQueueCreatedByOtherSession() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));

      ClientSession session2 = sf.createSession(false, true, true);
      session2.start();

      ClientConsumer consumer = session2.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);

      session2.close();
      session.close();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {

      super.tearDown();
   }

   @Test
   public void testDeleteTemporaryQueueAfterConnectionIsClosed() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);
      RemotingConnectionImpl conn = (RemotingConnectionImpl) server.getRemotingService()
         .getConnections()
         .iterator()
         .next();

      final CountDownLatch latch = new CountDownLatch(1);
      conn.addCloseListener(new CloseListener()
      {
         public void connectionClosed()
         {
            latch.countDown();
         }
      });
      session.close();
      sf.close();
      // wait for the closing listeners to be fired
      Assert.assertTrue("connection close listeners not fired", latch.await(2 * TemporaryQueueTest.CONNECTION_TTL,
                                                                            TimeUnit.MILLISECONDS));

      sf = addSessionFactory(createSessionFactory(locator));
      session = sf.createSession(false, true, true);
      session.start();

      try
      {
         session.createConsumer(queue);
         Assert.fail("temp queue must not exist after the remoting connection is closed");
      }
      catch (HornetQNonExistentQueueException neqe)
      {
         //ol
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      session.close();
   }


   @Test
   public void testQueueWithWildcard() throws Exception
   {
      session.createQueue("a.b", "queue1");
      session.createTemporaryQueue("a.#", "queue2");
      session.createTemporaryQueue("a.#", "queue3");

      ClientProducer producer = session.createProducer("a.b");
      producer.send(session.createMessage(false));

      ClientConsumer cons = session.createConsumer("queue2");

      session.start();

      ClientMessage msg = cons.receive(5000);

      assertNotNull(msg);

      msg.acknowledge();

      cons.close();

      cons = session.createConsumer("queue3");

      session.start();

      msg = cons.receive(5000);

      assertNotNull(msg);

      msg.acknowledge();

      cons.close();

      session.deleteQueue("queue2");
      session.deleteQueue("queue3");

      session.close();
   }


   @Test
   public void testQueueWithWildcard2() throws Exception
   {
      session.createQueue("a.b", "queue1");
      session.createTemporaryQueue("a.#", "queue2");
      session.createTemporaryQueue("a.#", "queue3");

      ClientProducer producer = session.createProducer("a.b");
      producer.send(session.createMessage(false));

      ClientConsumer cons = session.createConsumer("queue2");

      session.start();

      ClientMessage msg = cons.receive(5000);

      assertNotNull(msg);

      msg.acknowledge();

      cons.close();

      cons = session.createConsumer("queue3");

      session.start();

      msg = cons.receive(5000);

      assertNotNull(msg);

      msg.acknowledge();

      cons.close();

      session.deleteQueue("queue2");
      session.deleteQueue("queue3");

      session.close();
   }

   @Test
   public void testQueueWithWildcard3() throws Exception
   {
      session.createQueue("a.b", "queue1");
      session.createTemporaryQueue("a.#", "queue2");
      session.createTemporaryQueue("a.#", "queue2.1");

      session.deleteQueue("queue2");
   }

   @Test
   public void testDeleteTemporaryQueueAfterConnectionIsClosed_2() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);
      Assert.assertEquals(1, server.getConnectionCount());

      // we create a second session. the temp queue must be present
      // even after we closed the session which created it
      ClientSession session2 = sf.createSession(false, true, true);

      session.close();

      // let some time for the server to clean the connections
      // Thread.sleep(1000);

      session2.start();

      session2.createConsumer(queue);

      session2.close();
   }

   @Test
   public void testRecreateConsumerOverServerFailure() throws Exception
   {
      ServerLocator serverWithReattach = createLocator();
      serverWithReattach.setReconnectAttempts(-1);
      serverWithReattach.setRetryInterval(1000);
      serverWithReattach.setConfirmationWindowSize(-1);
      ClientSessionFactory reattachSF = createSessionFactory(serverWithReattach);

      ClientSession session = reattachSF.createSession(false, false);
      session.createTemporaryQueue("tmpAd", "tmpQ");
      ClientConsumer consumer = session.createConsumer("tmpQ");

      ClientProducer prod = session.createProducer("tmpAd");

      session.start();

      RemotingConnectionImpl conn = (RemotingConnectionImpl) ((ClientSessionInternal) session).getConnection();

      conn.fail(new HornetQIOErrorException());

      prod.send(session.createMessage(false));
      session.commit();

      assertNotNull(consumer.receive(1000));

      session.close();

      reattachSF.close();

      serverWithReattach.close();


   }

   @Test
   public void testTemporaryQueuesWithFilter() throws Exception
   {

      int countTmpQueue = 0;

      final AtomicInteger errors = new AtomicInteger(0);

      class MyHandler implements MessageHandler
      {
         final String color;

         final CountDownLatch latch;

         final ClientSession sess;

         public MyHandler(ClientSession sess, String color, int expectedMessages)
         {
            this.sess = sess;
            latch = new CountDownLatch(expectedMessages);
            this.color = color;
         }

         public boolean waitCompletion() throws Exception
         {
            return latch.await(10, TimeUnit.SECONDS);
         }

         public void onMessage(ClientMessage message)
         {
            try
            {
               message.acknowledge();
               sess.commit();
               latch.countDown();

               if (!message.getStringProperty("color").equals(color))
               {
                  log.warn("Unexpected color " + message.getStringProperty("color") + " when we were expecting " + color);
                  errors.incrementAndGet();
               }
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
               errors.incrementAndGet();
            }
         }

      }

      String address = "AD_test";
      int iterations = 100;
      int msgs = 100;

      for (int i = 0; i < iterations; i++)
      {
         ClientSessionFactory clientsConnecton = addSessionFactory(createSessionFactory(locator));
         ClientSession localSession = clientsConnecton.createSession();

         ClientProducer prod = localSession.createProducer(address);

         localSession.start();


         log.info("Iteration " + i);
         String queueRed = address + "_red_" + (countTmpQueue++);
         String queueBlue = address + "_blue_" + (countTmpQueue++);

         ClientSession sessConsumerRed = clientsConnecton.createSession();
         sessConsumerRed.createTemporaryQueue(address, queueRed, "color='red'");
         MyHandler redHandler = new MyHandler(sessConsumerRed, "red", msgs);
         ClientConsumer redClientConsumer = sessConsumerRed.createConsumer(queueRed);
         redClientConsumer.setMessageHandler(redHandler);
         sessConsumerRed.start();

         ClientSession sessConsumerBlue = clientsConnecton.createSession();
         sessConsumerBlue.createTemporaryQueue(address, queueBlue, "color='blue'");
         MyHandler blueHandler = new MyHandler(sessConsumerBlue, "blue", msgs);
         ClientConsumer blueClientConsumer = sessConsumerBlue.createConsumer(queueBlue);
         blueClientConsumer.setMessageHandler(blueHandler);
         sessConsumerBlue.start();

         try
         {
            ClientMessage msgBlue = session.createMessage(false);
            msgBlue.putStringProperty("color", "blue");

            ClientMessage msgRed = session.createMessage(false);
            msgRed.putStringProperty("color", "red");

            for (int nmsg = 0; nmsg < msgs; nmsg++)
            {
               prod.send(msgBlue);

               prod.send(msgRed);

               session.commit();
            }

            blueHandler.waitCompletion();
            redHandler.waitCompletion();

            assertEquals(0, errors.get());

         }
         finally
         {
            localSession.close();
            clientsConnecton.close();
         }
      }

   }

   @Test
   public void testDeleteTemporaryQueueWhenClientCrash() throws Exception
   {
      session.close();
      sf.close();

      final SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      // server must received at least one ping from the client to pass
      // so that the server connection TTL is configured with the client value
      final CountDownLatch pingOnServerLatch = new CountDownLatch(1);
      server.getRemotingService().addIncomingInterceptor(new Interceptor()
      {

         public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
         {
            if (packet.getType() == PacketImpl.PING)
            {
               pingOnServerLatch.countDown();
            }
            return true;
         }
      });

      ServerLocator locator = createInVMNonHALocator();
      locator.setConnectionTTL(TemporaryQueueTest.CONNECTION_TTL);
      sf = addSessionFactory(createSessionFactory(locator));
      session = sf.createSession(false, true, true);

      session.createTemporaryQueue(address, queue);
      Assert.assertTrue("server has not received any ping from the client",
                        pingOnServerLatch.await(2 * RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL,
                                                TimeUnit.MILLISECONDS));
      Assert.assertEquals(1, server.getConnectionCount());

      RemotingConnection remotingConnection = server.getRemotingService().getConnections().iterator().next();
      final CountDownLatch serverCloseLatch = new CountDownLatch(1);
      remotingConnection.addCloseListener(new CloseListener()
      {
         public void connectionClosed()
         {
            serverCloseLatch.countDown();
         }
      });

      ((ClientSessionInternal) session).getConnection().fail(new HornetQInternalErrorException("simulate a client failure"));

      // let some time for the server to clean the connections
      Assert.assertTrue("server has not closed the connection",
                        serverCloseLatch.await(2 * RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL +
                                                  2 *
                                                     TemporaryQueueTest.CONNECTION_TTL, TimeUnit.MILLISECONDS));

      // The next getCount will be asynchronously done at the end of failure. We will wait some time until it has reached there.
      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && server.getConnectionCount() > 0;)
      {
         Thread.sleep(1);
      }

      Assert.assertEquals(0, server.getConnectionCount());

      session.close();

      sf.close();
      ServerLocator locator2 = createInVMNonHALocator();

      sf = addSessionFactory(createSessionFactory(locator2));
      session = sf.createSession(false, true, true);
      session.start();

      HornetQAction hornetQAction = new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createConsumer(queue);
         }
      };

      UnitTestCase.expectHornetQException("temp queue must not exist after the server detected the client crash",
                                          HornetQExceptionType.QUEUE_DOES_NOT_EXIST, hornetQAction);

      session.close();

      locator2.close();
   }

   @Test
   public void testBlockingWithTemporaryQueue() throws Exception
   {

      AddressSettings setting = new AddressSettings();
      setting.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      setting.setMaxSizeBytes(1024 * 1024);

      server.getAddressSettingsRepository().addMatch("TestAD", setting);

      ClientSessionFactory consumerCF = createSessionFactory(locator);
      ClientSession consumerSession = consumerCF.createSession(true, true);
      consumerSession.addMetaData("consumer", "consumer");
      consumerSession.createTemporaryQueue("TestAD", "Q1");
      consumerSession.createConsumer("Q1");
      consumerSession.start();

      final ClientProducerImpl prod = (ClientProducerImpl) session.createProducer("TestAD");

      final AtomicInteger errors = new AtomicInteger(0);

      final AtomicInteger msgs = new AtomicInteger(0);

      final int TOTAL_MSG = 1000;

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               for (int i = 0; i < TOTAL_MSG; i++)
               {
                  ClientMessage msg = session.createMessage(false);
                  msg.getBodyBuffer().writeBytes(new byte[1024]);
                  prod.send(msg);
                  msgs.incrementAndGet();
               }
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               errors.incrementAndGet();
            }

            System.out.println("done");
         }
      };

      t.start();

      while (msgs.get() == 0)
      {
         Thread.sleep(100);
      }

      while (t.isAlive() && errors.get() == 0 && !prod.getProducerCredits().isBlocked())
      {
         Thread.sleep(100);
      }

      assertEquals(0, errors.get());

      ClientSessionFactory newConsumerCF = createSessionFactory(locator);
      ClientSession newConsumerSession = newConsumerCF.createSession(true, true);
      newConsumerSession.createTemporaryQueue("TestAD", "Q2");
      ClientConsumer newConsumer = newConsumerSession.createConsumer("Q2");
      newConsumerSession.start();

      int toReceive = TOTAL_MSG - msgs.get();

      for (ServerSession sessionIterator : server.getSessions())
      {
         if (sessionIterator.getMetaData("consumer") != null)
         {
            System.out.println("Failing session");
            ServerSessionImpl impl = (ServerSessionImpl) sessionIterator;
            impl.getRemotingConnection().fail(new HornetQDisconnectedException("failure e"));
         }
      }

      int secondReceive = 0;

      ClientMessage msg = null;
      while (secondReceive < toReceive && (msg = newConsumer.receive(5000)) != null)
      {
         msg.acknowledge();
         secondReceive++;
      }

      assertNull(newConsumer.receiveImmediate());

      assertEquals(toReceive, secondReceive);

      t.join();


   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected ServerLocator createLocator()
   {
      ServerLocator retlocator = super.createLocator();
      retlocator.setConnectionTTL(TemporaryQueueTest.CONNECTION_TTL);
      retlocator.setClientFailureCheckPeriod(TemporaryQueueTest.CONNECTION_TTL / 3);
      return retlocator;
   }

}
