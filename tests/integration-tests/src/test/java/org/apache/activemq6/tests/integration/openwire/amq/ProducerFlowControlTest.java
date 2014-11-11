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
package org.apache.activemq6.tests.integration.openwire.amq;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq6.core.settings.impl.AddressSettings;
import org.apache.activemq6.tests.integration.openwire.BasicOpenWireTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * adapted from: org.apache.activemq.ProducerFlowControlTest
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class ProducerFlowControlTest extends BasicOpenWireTest
{
   ActiveMQQueue queueA = new ActiveMQQueue("QUEUE.A");
   ActiveMQQueue queueB = new ActiveMQQueue("QUEUE.B");
   protected ActiveMQConnection flowControlConnection;
   // used to test sendFailIfNoSpace on SystemUsage
   protected final AtomicBoolean gotResourceException = new AtomicBoolean(false);
   private Thread asyncThread = null;

   @Test
   public void test2ndPubisherWithProducerWindowSendConnectionThatIsBlocked() throws Exception
   {
      factory.setProducerWindowSize(1024 * 64);
      flowControlConnection = (ActiveMQConnection) factory.createConnection();
      flowControlConnection.start();

      Session session = flowControlConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queueB);

      // Test sending to Queue A
      // 1 few sends should not block until the producer window is used up.
      fillQueue(queueA);

      // Test sending to Queue B it should not block since the connection
      // should not be blocked.
      CountDownLatch pubishDoneToQeueuB = asyncSendTo(queueB, "Message 1");
      assertTrue(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));

      TextMessage msg = (TextMessage) consumer.receive();
      assertEquals("Message 1", msg.getText());
      msg.acknowledge();

      pubishDoneToQeueuB = asyncSendTo(queueB, "Message 2");
      assertTrue(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));

      msg = (TextMessage) consumer.receive();
      assertEquals("Message 2", msg.getText());
      msg.acknowledge();

      consumer.close();
   }

   @Test
   public void testPubisherRecoverAfterBlock() throws Exception
   {
      flowControlConnection = (ActiveMQConnection) factory.createConnection();
      flowControlConnection.start();

      final Session session = flowControlConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      final MessageProducer producer = session.createProducer(queueA);

      final AtomicBoolean done = new AtomicBoolean(true);
      final AtomicBoolean keepGoing = new AtomicBoolean(true);

      Thread thread = new Thread("Filler")
      {
         int i;

         @Override
         public void run()
         {
            while (keepGoing.get())
            {
               done.set(false);
               try
               {
                  producer.send(session.createTextMessage("Test message " + ++i));
               }
               catch (JMSException e)
               {
                  break;
               }
            }
         }
      };
      thread.start();
      waitForBlockedOrResourceLimit(done);

      // after receiveing messges, producer should continue sending messages
      // (done == false)
      MessageConsumer consumer = session.createConsumer(queueA);
      TextMessage msg;
      for (int idx = 0; idx < 5; ++idx)
      {
         msg = (TextMessage) consumer.receive(1000);
         System.out.println("received: " + idx + ", msg: "
               + msg.getJMSMessageID());
         msg.acknowledge();
      }
      Thread.sleep(1000);
      keepGoing.set(false);

      consumer.close();
      assertFalse("producer has resumed", done.get());
   }

   @Test
   public void testAsyncPubisherRecoverAfterBlock() throws Exception
   {
      factory.setProducerWindowSize(1024 * 5);
      factory.setUseAsyncSend(true);
      flowControlConnection = (ActiveMQConnection) factory.createConnection();
      flowControlConnection.start();

      final Session session = flowControlConnection.createSession(false,
            Session.CLIENT_ACKNOWLEDGE);
      final MessageProducer producer = session.createProducer(queueA);

      final AtomicBoolean done = new AtomicBoolean(true);
      final AtomicBoolean keepGoing = new AtomicBoolean(true);

      Thread thread = new Thread("Filler")
      {
         int i;

         @Override
         public void run()
         {
            while (keepGoing.get())
            {
               done.set(false);
               try
               {
                  producer.send(session.createTextMessage("Test message " + ++i));
               }
               catch (JMSException e)
               {
               }
            }
         }
      };
      thread.start();
      waitForBlockedOrResourceLimit(done);

      // after receiveing messges, producer should continue sending messages
      // (done == false)
      MessageConsumer consumer = session.createConsumer(queueA);
      TextMessage msg;
      for (int idx = 0; idx < 5; ++idx)
      {
         msg = (TextMessage) consumer.receive(1000);
         assertNotNull("Got a message", msg);
         System.out.println("received: " + idx + ", msg: " + msg.getJMSMessageID());
         msg.acknowledge();
      }
      Thread.sleep(1000);
      keepGoing.set(false);

      consumer.close();
      assertFalse("producer has resumed", done.get());
   }

   @Test
   public void test2ndPubisherWithSyncSendConnectionThatIsBlocked() throws Exception
   {
      factory.setAlwaysSyncSend(true);
      flowControlConnection = (ActiveMQConnection) factory.createConnection();
      flowControlConnection.start();

      Session session = flowControlConnection.createSession(false,
            Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queueB);

      // Test sending to Queue A
      // 1st send should not block. But the rest will.
      fillQueue(queueA);

      // Test sending to Queue B it should not block.
      CountDownLatch pubishDoneToQeueuB = asyncSendTo(queueB, "Message 1");
      assertTrue(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));

      TextMessage msg = (TextMessage) consumer.receive();
      assertEquals("Message 1", msg.getText());
      msg.acknowledge();

      pubishDoneToQeueuB = asyncSendTo(queueB, "Message 2");
      assertTrue(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));

      msg = (TextMessage) consumer.receive();
      assertEquals("Message 2", msg.getText());
      msg.acknowledge();
      consumer.close();
   }

   @Test
   public void testSimpleSendReceive() throws Exception
   {
      factory.setAlwaysSyncSend(true);
      flowControlConnection = (ActiveMQConnection) factory.createConnection();
      flowControlConnection.start();

      Session session = flowControlConnection.createSession(false,
            Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queueA);

      // Test sending to Queue B it should not block.
      CountDownLatch pubishDoneToQeueuA = asyncSendTo(queueA, "Message 1");
      assertTrue(pubishDoneToQeueuA.await(2, TimeUnit.SECONDS));

      TextMessage msg = (TextMessage) consumer.receive();
      assertEquals("Message 1", msg.getText());
      msg.acknowledge();

      pubishDoneToQeueuA = asyncSendTo(queueA, "Message 2");
      assertTrue(pubishDoneToQeueuA.await(2, TimeUnit.SECONDS));

      msg = (TextMessage) consumer.receive();
      assertEquals("Message 2", msg.getText());
      msg.acknowledge();
      consumer.close();
   }

   @Test
   public void test2ndPubisherWithStandardConnectionThatIsBlocked() throws Exception
   {
      flowControlConnection = (ActiveMQConnection) factory.createConnection();
      flowControlConnection.start();

      // Test sending to Queue A
      // 1st send should not block.
      fillQueue(queueA);

      // Test sending to Queue B it should block.
      // Since even though the it's queue limits have not been reached, the
      // connection
      // is blocked.
      CountDownLatch pubishDoneToQeueuB = asyncSendTo(queueB, "Message 1");
      assertFalse(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));
   }

   private void fillQueue(final ActiveMQQueue queue) throws JMSException,
         InterruptedException
   {
      final AtomicBoolean done = new AtomicBoolean(true);
      final AtomicBoolean keepGoing = new AtomicBoolean(true);

      // Starts an async thread that every time it publishes it sets the done
      // flag to false.
      // Once the send starts to block it will not reset the done flag
      // anymore.
      asyncThread = new Thread("Fill thread.")
      {
         public void run()
         {
            Session session = null;
            try
            {
               session = flowControlConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               MessageProducer producer = session.createProducer(queue);
               producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
               while (keepGoing.get())
               {
                  done.set(false);
                  producer.send(session.createTextMessage("Hello World"));
               }
            }
            catch (JMSException e)
            {
            }
            finally
            {
               safeClose(session);
            }
         }
      };
      asyncThread.start();

      waitForBlockedOrResourceLimit(done);
      keepGoing.set(false);
   }

   protected void waitForBlockedOrResourceLimit(final AtomicBoolean done) throws InterruptedException
   {
      while (true)
      {
         Thread.sleep(2000);
         System.out.println("check done: " + done.get() + " ex: " + gotResourceException.get());
         // the producer is blocked once the done flag stays true or there is a
         // resource exception
         if (done.get() || gotResourceException.get())
         {
            break;
         }
         done.set(true);
      }
   }

   private CountDownLatch asyncSendTo(final ActiveMQQueue queue,
         final String message) throws JMSException
   {
      final CountDownLatch done = new CountDownLatch(1);
      new Thread("Send thread.")
      {
         public void run()
         {
            Session session = null;
            try
            {
               session = flowControlConnection.createSession(false,
                     Session.AUTO_ACKNOWLEDGE);
               MessageProducer producer = session.createProducer(queue);
               producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
               producer.send(session.createTextMessage(message));
               done.countDown();
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
            finally
            {
               safeClose(session);
            }
         }
      }.start();
      return done;
   }

   @Override
   protected void extraServerConfig(Configuration serverConfig)
   {
      String match = "jms.queue.#";
      Map<String, AddressSettings> asMap = serverConfig.getAddressesSettings();
      AddressSettings settings = asMap.get(match);
      settings.setMaxSizeBytes(1);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      this.makeSureCoreQueueExist("QUEUE.A");
      this.makeSureCoreQueueExist("QUEUE.B");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      try
      {
         if (flowControlConnection != null)
         {
            TcpTransport t = (TcpTransport) flowControlConnection.getTransport()
               .narrow(TcpTransport.class);
            t.getTransportListener()
               .onException(new IOException("Disposed."));
            flowControlConnection.getTransport()
               .stop();
            flowControlConnection.close();
         }
         if (asyncThread != null)
         {
            asyncThread.join();
            asyncThread = null;
         }
      }
      finally
      {
         super.tearDown();
      }
   }

}
