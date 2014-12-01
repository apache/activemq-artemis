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
package org.apache.activemq.jms.tests.stress;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.jms.tests.ActiveMQServerTestCase;
import org.apache.activemq.jms.tests.JmsTestLogger;
import org.apache.activemq.jms.tests.util.ProxyAssertSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test was added to test regression on http://jira.jboss.com/jira/browse/JBMESSAGING-660
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ConcurrentCloseStressTest extends ActiveMQServerTestCase
{
   @BeforeClass
   public static void stressTestsEnabled()
   {
      org.junit.Assume.assumeTrue(JMSStressTestBase.STRESS_TESTS_ENABLED);
   }

   InitialContext ic;

   ConnectionFactory cf;

   Queue queue;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      // ServerManagement.start("all");

      ic = getInitialContext();
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      destroyQueue("TestQueue");
      createQueue("TestQueue");

      queue = (Queue)ic.lookup("queue/TestQueue");

      log.debug("setup done");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      destroyQueue("TestQueue");
      super.tearDown();
   }

   @Test
   public void testProducersAndConsumers() throws Exception
   {
      Connection connectionProducer = cf.createConnection();
      Connection connectionReader = cf.createConnection();

      connectionReader.start();
      connectionProducer.start(); // try with and without this...

      ProducerThread[] producerThread = new ProducerThread[20];
      ReaderThread[] readerThread = new ReaderThread[20];
      TestThread[] threads = new TestThread[40];

      for (int i = 0; i < 20; i++)
      {
         producerThread[i] = new ProducerThread(i, connectionProducer, queue);
         readerThread[i] = new ReaderThread(i, connectionReader, queue);
         threads[i] = producerThread[i];
         threads[i + 20] = readerThread[i];
      }

      for (int i = 0; i < 40; i++)
      {
         threads[i].start();
      }

      for (int i = 0; i < 40; i++)
      {
         threads[i].join();
      }

      boolean hasFailure = false;

      for (int i = 0; i < 40; i++)
      {
         if (!threads[i].exceptions.isEmpty())
         {
            hasFailure = true;
            for (Exception element : threads[i].exceptions)
            {
               Exception ex = element;
               log.error("Exception occurred in one of the threads - " + ex, ex);
            }
         }
      }

      int messagesProduced = 0;
      int messagesRead = 0;
      for (ProducerThread element : producerThread)
      {
         messagesProduced += element.messagesProduced;
      }

      for (int i = 0; i < producerThread.length; i++)
      {
         messagesRead += readerThread[i].messagesRead;
      }

      if (hasFailure)
      {
         ProxyAssertSupport.fail("An exception has occurred in one of the threads");
      }
   }

   static class TestThread extends Thread
   {
      List<Exception> exceptions = new ArrayList<Exception>();

      protected int index;

      public int messageCount = 0;
   }

   static class ReaderThread extends TestThread
   {
      private static final JmsTestLogger log = JmsTestLogger.LOGGER;

      Connection conn;

      Queue queue;

      int messagesRead = 0;

      public ReaderThread(final int index, final Connection conn, final Queue queue) throws Exception
      {
         this.index = index;
         this.conn = conn;
         this.queue = queue;
      }

      @Override
      public void run()
      {
         int commitCounter = 0;
         try
         {
            Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);

            int lastCount = messageCount;
            while (true)
            {
               TextMessage message = (TextMessage)consumer.receive(5000);
               if (message == null)
               {
                  break;
               }
               ReaderThread.log.debug("read message " + message.getText());

               // alternating commits and rollbacks
               if (commitCounter++ % 2 == 0)
               {
                  messagesRead += messageCount - lastCount;
                  lastCount = messageCount;
                  ReaderThread.log.debug("commit");
                  session.commit();
               }
               else
               {
                  lastCount = messageCount;
                  ReaderThread.log.debug("rollback");
                  session.rollback();
               }

               messageCount++;

               if (messageCount % 7 == 0)
               {
                  session.close();

                  session = conn.createSession(true, Session.SESSION_TRANSACTED);
                  consumer = session.createConsumer(queue);
               }
            }

            messagesRead += messageCount - lastCount;

            session.commit();
            consumer.close();
            session.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            exceptions.add(e);
         }
      }

   }

   static class ProducerThread extends TestThread
   {
      private static final JmsTestLogger log = JmsTestLogger.LOGGER;

      Connection conn;

      Queue queue;

      int messagesProduced = 0;

      public ProducerThread(final int index, final Connection conn, final Queue queue) throws Exception
      {
         this.index = index;
         this.conn = conn;
         this.queue = queue;
      }

      @Override
      public void run()
      {
         for (int i = 0; i < 10; i++)
         {
            try
            {
               int lastMessage = messageCount;
               Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
               MessageProducer producer = sess.createProducer(queue);

               for (int j = 0; j < 20; j++)
               {
                  producer.send(sess.createTextMessage("Message " + i + ", " + j));

                  if (j % 2 == 0)
                  {
                     ProducerThread.log.debug("commit");
                     messagesProduced += messageCount - lastMessage;
                     lastMessage = messageCount;

                     sess.commit();
                  }
                  else
                  {
                     ProducerThread.log.debug("rollback");
                     lastMessage = messageCount;
                     sess.rollback();
                  }
                  messageCount++;

               }

               messagesProduced += messageCount - lastMessage;
               sess.commit();
               sess.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
               exceptions.add(e);
            }
         }
      }

   }

}
