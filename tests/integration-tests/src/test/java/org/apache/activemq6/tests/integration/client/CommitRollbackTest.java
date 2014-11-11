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
package org.apache.activemq6.tests.integration.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientConsumer;
import org.apache.activemq6.api.core.client.ClientMessage;
import org.apache.activemq6.api.core.client.ClientProducer;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.MessageHandler;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.Queue;
import org.apache.activemq6.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class CommitRollbackTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString addressB = new SimpleString("addressB");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   @Test
   public void testReceiveWithCommit() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession(false, false, false);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = cc.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(numMessages, q.getDeliveringCount());
      session.commit();
      Assert.assertEquals(0, q.getDeliveringCount());
      session.close();
      sendSession.close();
   }

   @Test
   public void testReceiveWithRollback() throws Exception
   {
      HornetQServer server = createServer(false);

      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession(false, false, false);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = cc.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(numMessages, q.getDeliveringCount());
      session.rollback();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = cc.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }
      Assert.assertEquals(numMessages, q.getDeliveringCount());
      session.close();
      sendSession.close();
   }

   @Test
   public void testReceiveWithRollbackMultipleConsumersDifferentQueues() throws Exception
   {
      HornetQServer server = createServer(false);

      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession(false, false, false);
      sendSession.createQueue(addressA, queueA, false);
      sendSession.createQueue(addressB, queueB, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientProducer cp2 = sendSession.createProducer(addressB);
      ClientConsumer cc = session.createConsumer(queueA);
      ClientConsumer cc2 = session.createConsumer(queueB);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
         cp2.send(sendSession.createMessage(false));
      }
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = cc.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
         cm = cc2.receive(5000);
         Assert.assertNotNull(cm);
         cm.acknowledge();
      }
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Queue q2 = (Queue) server.getPostOffice().getBinding(queueB).getBindable();
      Assert.assertEquals(numMessages, q.getDeliveringCount());
      cc.close();
      cc2.close();
      session.rollback();
      Assert.assertEquals(0, q2.getDeliveringCount());
      Assert.assertEquals(numMessages, getMessageCount(q));
      Assert.assertEquals(0, q2.getDeliveringCount());
      Assert.assertEquals(numMessages, getMessageCount(q));
      sendSession.close();
      session.close();
   }

   @Test
   public void testAsyncConsumerCommit() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnAcknowledge(true);
      locator.setAckBatchSize(0);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      final ClientSession session = cf.createSession(false, true, false);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }
      final CountDownLatch latch = new CountDownLatch(numMessages);
      session.start();
      cc.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage message)
         {
            try
            {
               message.acknowledge();
            }
            catch (HornetQException e)
            {
               try
               {
                  session.close();
               }
               catch (HornetQException e1)
               {
                  e1.printStackTrace();
               }
            }
            latch.countDown();
         }
      });
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(numMessages, q.getDeliveringCount());
      Assert.assertEquals(numMessages, getMessageCount(q));
      session.commit();
      Assert.assertEquals(0, q.getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(q));
      sendSession.close();
      session.close();

   }

   @Test
   public void testAsyncConsumerRollback() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnAcknowledge(true);
      locator.setAckBatchSize(0);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      final ClientSession session = cf.createSession(false, true, false);
      sendSession.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientConsumer cc = session.createConsumer(queueA);
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      session.start();
      cc.setMessageHandler(new ackHandler(session, latch));
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(numMessages, q.getDeliveringCount());
      Assert.assertEquals(numMessages, getMessageCount(q));
      session.stop();
      session.rollback();
      Assert.assertEquals(0, q.getDeliveringCount());
      Assert.assertEquals(numMessages, getMessageCount(q));
      latch = new CountDownLatch(numMessages);
      cc.setMessageHandler(new ackHandler(session, latch));
      session.start();
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      sendSession.close();
      session.close();
      cf.close();

   }

   private static class ackHandler implements MessageHandler
   {
      private final ClientSession session;

      private final CountDownLatch latch;

      public ackHandler(final ClientSession session, final CountDownLatch latch)
      {
         this.session = session;
         this.latch = latch;
      }

      public void onMessage(final ClientMessage message)
      {
         try
         {
            message.acknowledge();
         }
         catch (HornetQException e)
         {
            try
            {
               session.close();
            }
            catch (HornetQException e1)
            {
               e1.printStackTrace();
            }
         }
         latch.countDown();
      }
   }
}
