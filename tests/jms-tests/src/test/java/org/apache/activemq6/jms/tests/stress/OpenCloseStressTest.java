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
package org.apache.activemq6.jms.tests.stress;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.apache.activemq6.jms.tests.HornetQServerTestCase;
import org.apache.activemq6.jms.tests.util.ProxyAssertSupport;
import org.apache.activemq6.utils.UUIDGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A OpenCloseStressTest.
 * <p>
 * This stress test starts several publisher connections and several subscriber connections, then
 * sends and consumes messages while concurrently closing the sessions.
 * <p>
 * This test will help catch race conditions that occurred with rapid open/closing of sessions when
 * messages are being sent/received
 * <p>
 * E.g. http://jira.jboss.com/jira/browse/JBMESSAGING-982
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class OpenCloseStressTest extends HornetQServerTestCase
{
   @BeforeClass
   public static void stressTestsEnabled()
   {
      org.junit.Assume.assumeTrue(JMSStressTestBase.STRESS_TESTS_ENABLED);
   }

   InitialContext ic;

   ConnectionFactory cf;

   Topic topic;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      // ServerManagement.start("all");

      ic = getInitialContext();
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      destroyTopic("TestTopic");
      createTopic("TestTopic");

      topic = (Topic)ic.lookup("topic/TestTopic");

      log.debug("setup done");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      destroyQueue("TestQueue");
      log.debug("tear down done");
   }

   @Test
   public void testOpenClose() throws Exception
   {
      Connection conn1 = null;
      Connection conn2 = null;
      Connection conn3 = null;

      Connection conn4 = null;
      Connection conn5 = null;
      Connection conn6 = null;
      Connection conn7 = null;
      Connection conn8 = null;

      try
      {
         Publisher[] publishers = new Publisher[3];

         final int MSGS_PER_PUBLISHER = 10000;

         conn1 = cf.createConnection();
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod1 = sess1.createProducer(topic);
         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
         publishers[0] = new Publisher(sess1, prod1, MSGS_PER_PUBLISHER, 2);

         conn2 = cf.createConnection();
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod2 = sess2.createProducer(topic);
         prod2.setDeliveryMode(DeliveryMode.PERSISTENT);
         publishers[1] = new Publisher(sess2, prod2, MSGS_PER_PUBLISHER, 5);

         conn3 = cf.createConnection();
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod3 = sess3.createProducer(topic);
         prod3.setDeliveryMode(DeliveryMode.PERSISTENT);
         publishers[2] = new Publisher(sess3, prod3, MSGS_PER_PUBLISHER, 1);

         Subscriber[] subscribers = new Subscriber[5];

         conn4 = cf.createConnection();
         subscribers[0] = new Subscriber(conn4, 3 * MSGS_PER_PUBLISHER, 500, 1000 * 60 * 15, topic, false);

         conn5 = cf.createConnection();
         subscribers[1] = new Subscriber(conn5, 3 * MSGS_PER_PUBLISHER, 2000, 1000 * 60 * 15, topic, false);

         conn6 = cf.createConnection();
         subscribers[2] = new Subscriber(conn6, 3 * MSGS_PER_PUBLISHER, 700, 1000 * 60 * 15, topic, false);

         conn7 = cf.createConnection();
         subscribers[3] = new Subscriber(conn7, 3 * MSGS_PER_PUBLISHER, 1500, 1000 * 60 * 15, topic, true);

         conn8 = cf.createConnection();
         subscribers[4] = new Subscriber(conn8, 3 * MSGS_PER_PUBLISHER, 1200, 1000 * 60 * 15, topic, true);

         Thread[] threads = new Thread[8];

         // subscribers
         threads[0] = new Thread(subscribers[0]);

         threads[1] = new Thread(subscribers[1]);

         threads[2] = new Thread(subscribers[2]);

         threads[3] = new Thread(subscribers[3]);

         threads[4] = new Thread(subscribers[4]);

         // publishers

         threads[5] = new Thread(publishers[0]);

         threads[6] = new Thread(publishers[1]);

         threads[7] = new Thread(publishers[2]);

         for (int i = 0; i < subscribers.length; i++)
         {
            threads[i].start();
         }

         // Pause before creating producers otherwise subscribers to make sure they're all created

         Thread.sleep(5000);

         for (int i = subscribers.length; i < threads.length; i++)
         {
            threads[i].start();
         }

         for (Thread thread : threads)
         {
            thread.join();
         }

         for (Subscriber subscriber : subscribers)
         {
            if (subscriber.isDurable())
            {
               ProxyAssertSupport.assertEquals(3 * MSGS_PER_PUBLISHER, subscriber.getMessagesReceived());
            }
            else
            {
               // Note that for a non durable subscriber the number of messages received in total
               // will be somewhat less than the total number received since when recycling the session
               // there is a period of time after closing the previous session and starting the next one
               // when messages are being sent and won't be received (since there is no consumer)
            }

            ProxyAssertSupport.assertFalse(subscriber.isFailed());
         }

         for (Publisher publisher : publishers)
         {
            ProxyAssertSupport.assertFalse(publisher.isFailed());
         }
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
         if (conn3 != null)
         {
            conn3.close();
         }
         if (conn4 != null)
         {
            conn4.close();
         }
         if (conn5 != null)
         {
            conn5.close();
         }
         if (conn6 != null)
         {
            conn6.close();
         }
         if (conn7 != null)
         {
            conn7.close();
         }
         if (conn8 != null)
         {
            conn8.close();
         }
      }

   }

   class Publisher implements Runnable
   {
      private final Session sess;

      private final int numMessages;

      private final int delay;

      private final MessageProducer prod;

      private boolean failed;

      boolean isFailed()
      {
         return failed;
      }

      Publisher(final Session sess, final MessageProducer prod, final int numMessages, final int delay)
      {
         this.sess = sess;

         this.prod = prod;

         this.numMessages = numMessages;

         this.delay = delay;
      }

      public void run()
      {
         try
         {
            for (int i = 0; i < numMessages; i++)
            {
               TextMessage tm = sess.createTextMessage("message" + i);

               prod.send(tm);

               try
               {
                  Thread.sleep(delay);
               }
               catch (Exception ignore)
               {
               }
            }
         }
         catch (JMSException e)
         {
            log.error("Failed to send message", e);
            failed = true;
         }
      }

   }

   class Subscriber implements Runnable
   {
      private Session sess;

      private MessageConsumer cons;

      private int msgsReceived;

      private final int numMessages;

      private final int delay;

      private final Connection conn;

      private boolean failed;

      private final long timeout;

      private final Destination dest;

      private final boolean durable;

      private String subname;

      boolean isFailed()
      {
         return failed;
      }

      boolean isDurable()
      {
         return durable;
      }

      synchronized void msgReceived()
      {
         msgsReceived++;
      }

      synchronized int getMessagesReceived()
      {
         return msgsReceived;
      }

      class Listener implements MessageListener
      {

         public void onMessage(final Message msg)
         {
            msgReceived();
         }

      }

      Subscriber(final Connection conn,
                 final int numMessages,
                 final int delay,
                 final long timeout,
                 final Destination dest,
                 final boolean durable) throws Exception
      {
         this.conn = conn;

         this.numMessages = numMessages;

         this.delay = delay;

         this.timeout = timeout;

         this.dest = dest;

         this.durable = durable;

         if (durable)
         {
            conn.setClientID(UUIDGenerator.getInstance().generateStringUUID());

            subname = UUIDGenerator.getInstance().generateStringUUID();
         }
      }

      public void run()
      {
         try
         {
            long start = System.currentTimeMillis();

            while (System.currentTimeMillis() - start < timeout && msgsReceived < numMessages)
            {
               // recycle the session

               recycleSession();

               Thread.sleep(delay);
            }

            // Delete the durable sub

            if (durable)
            {
               recycleSession();

               cons.close();

               sess.unsubscribe(subname);
            }
         }
         catch (Exception e)
         {
            log.error("Failed in subscriber", e);
            failed = true;
         }

      }

      void recycleSession() throws Exception
      {
         conn.stop();

         if (sess != null)
         {
            sess.close();
         }

         sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         if (durable)
         {
            cons = sess.createDurableSubscriber((Topic)dest, subname);
         }
         else
         {
            cons = sess.createConsumer(dest);
         }

         cons.setMessageListener(new Listener());

         conn.start();
      }

   }

}
