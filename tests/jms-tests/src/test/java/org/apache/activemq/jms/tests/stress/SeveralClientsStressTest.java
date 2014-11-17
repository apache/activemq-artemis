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
package org.apache.activemq.jms.tests.stress;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;

import org.apache.activemq.jms.tests.HornetQServerTestCase;
import org.apache.activemq.jms.tests.JmsTestLogger;
import org.apache.activemq.jms.tests.util.ProxyAssertSupport;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * In order for this test to run, you will need to edit /etc/security/limits.conf and change your max sockets to something bigger than 1024
 *
 * It's required to re-login after this change.
 *
 * For Windows you need also to increase this limit (max opened files) somehow.
 *
 *
Example of /etc/security/limits.confg:
#<domain>      <type>  <item>         <value>
clebert        hard    nofile          10240


 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class SeveralClientsStressTest extends HornetQServerTestCase
{
   @BeforeClass
   public static void stressTestsEnabled()
   {
      org.junit.Assume.assumeTrue(JMSStressTestBase.STRESS_TESTS_ENABLED);
   }

   protected boolean info = false;

   protected boolean startServer = true;

   // Static ---------------------------------------------------------------------------------------

   protected static long PRODUCER_ALIVE_FOR = 60000; // one minute

   protected static long CONSUMER_ALIVE_FOR = 60000; // one minutes

   protected static long TEST_ALIVE_FOR = 5 * 60 * 1000; // 5 minutes

   protected static int NUMBER_OF_PRODUCERS = 100; // this should be set to 300 later

   protected static int NUMBER_OF_CONSUMERS = 100; // this should be set to 300 later

   // a producer should have a long wait between each message sent?
   protected static boolean LONG_WAIT_ON_PRODUCERS = false;

   protected static AtomicInteger producedMessages = new AtomicInteger(0);

   protected static AtomicInteger readMessages = new AtomicInteger(0);

   protected Context createContext() throws Exception
   {
      return getInitialContext();
   }

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   @Test
   public void testQueue() throws Exception
   {
      Context ctx = createContext();

      HashSet<SeveralClientsStressTest.Worker> threads = new HashSet<SeveralClientsStressTest.Worker>();

      // A chhanel of communication between workers and the test method
      LinkedBlockingQueue<InternalMessage> testChannel = new LinkedBlockingQueue<InternalMessage>();

      for (int i = 0; i < SeveralClientsStressTest.NUMBER_OF_PRODUCERS; i++)
      {
         threads.add(new SeveralClientsStressTest.Producer(i, testChannel));
      }

      for (int i = 0; i < SeveralClientsStressTest.NUMBER_OF_CONSUMERS; i++)
      {
         threads.add(new SeveralClientsStressTest.Consumer(i, testChannel));
      }

      for (Worker worker : threads)
      {
         worker.start();
      }

      long timeToFinish = System.currentTimeMillis() + SeveralClientsStressTest.TEST_ALIVE_FOR;

      int numberOfProducers = SeveralClientsStressTest.NUMBER_OF_PRODUCERS;
      int numberOfConsumers = SeveralClientsStressTest.NUMBER_OF_CONSUMERS;

      while (threads.size() > 0)
      {
         SeveralClientsStressTest.InternalMessage msg = testChannel.poll(2000,
                                                                                                                   TimeUnit.MILLISECONDS);

         log.info("Produced:" + SeveralClientsStressTest.producedMessages.get() +
                  " and Consumed:" +
                  SeveralClientsStressTest.readMessages.get() +
                  " messages");

         if (msg != null)
         {
            if (info)
            {
               log.info("Received message " + msg);
            }
            if (msg instanceof SeveralClientsStressTest.WorkerFailed)
            {
               ProxyAssertSupport.fail("Worker " + msg.getWorker() + " has failed");
            }
            else if (msg instanceof SeveralClientsStressTest.WorkedFinishedMessages)
            {
               SeveralClientsStressTest.WorkedFinishedMessages finished = (SeveralClientsStressTest.WorkedFinishedMessages)msg;
               if (threads.remove(finished.getWorker()))
               {
                  if (System.currentTimeMillis() < timeToFinish)
                  {
                     if (finished.getWorker() instanceof SeveralClientsStressTest.Producer)
                     {
                        if (info)
                        {
                           log.info("Scheduling new Producer " + numberOfProducers);
                        }
                        SeveralClientsStressTest.Producer producer = new SeveralClientsStressTest.Producer(numberOfProducers++,
                                                                                                           testChannel);
                        threads.add(producer);
                        producer.start();
                     }
                     else if (finished.getWorker() instanceof SeveralClientsStressTest.Consumer)
                     {
                        if (info)
                        {
                           log.info("Scheduling new ClientConsumer " + numberOfConsumers);
                        }
                        SeveralClientsStressTest.Consumer consumer = new SeveralClientsStressTest.Consumer(numberOfConsumers++,
                                                                                                           testChannel);
                        threads.add(consumer);
                        consumer.start();
                     }
                  }
               }
               else
               {
                  log.warn(finished.getWorker() + " was not available on threads HashSet");
               }
            }
         }
      }

      log.info("Produced:" + SeveralClientsStressTest.producedMessages.get() +
               " and Consumed:" +
               SeveralClientsStressTest.readMessages.get() +
               " messages");

      clearMessages();

      log.info("Produced:" + SeveralClientsStressTest.producedMessages.get() +
               " and Consumed:" +
               SeveralClientsStressTest.readMessages.get() +
               " messages");

      ProxyAssertSupport.assertEquals(SeveralClientsStressTest.producedMessages.get(),
                                      SeveralClientsStressTest.readMessages.get());
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void clearMessages() throws Exception
   {
      Context ctx = createContext();
      ConnectionFactory cf = (ConnectionFactory)ctx.lookup("/ClusteredConnectionFactory");
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = (Queue)ctx.lookup("queue/testQueue");
      MessageConsumer consumer = sess.createConsumer(queue);

      conn.start();

      while (consumer.receive(1000) != null)
      {
         SeveralClientsStressTest.readMessages.incrementAndGet();
         log.info("Received JMS message on clearMessages");
      }

      conn.close();
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      if (startServer)
      {
         // ServerManagement.start("all", true);
         createQueue("testQueue");
      }

      clearMessages();
      SeveralClientsStressTest.producedMessages = new AtomicInteger(0);
      SeveralClientsStressTest.readMessages = new AtomicInteger(0);
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private class Worker extends Thread
   {

      protected JmsTestLogger log = JmsTestLogger.LOGGER;

      private boolean failed = false;

      private final int workerId;

      private Exception ex;

      LinkedBlockingQueue<InternalMessage> messageQueue;

      public int getWorkerId()
      {
         return workerId;
      }

      public Exception getException()
      {
         return ex;
      }

      public boolean isFailed()
      {
         return failed;
      }

      protected synchronized void setFailed(final boolean failed, final Exception ex)
      {
         this.failed = failed;
         this.ex = ex;

         log.info("Sending Exception", ex);

         sendInternalMessage(new SeveralClientsStressTest.WorkerFailed(this));

      }

      protected void sendInternalMessage(final SeveralClientsStressTest.InternalMessage msg)
      {
         if (info)
         {
            log.info("Sending message " + msg);
         }
         try
         {
            messageQueue.put(msg);
         }
         catch (Exception e)
         {
            log.error(e, e);
            setFailed(true, e);
         }
      }

      public Worker(final String name, final int workerId,
                    final LinkedBlockingQueue<SeveralClientsStressTest.InternalMessage> messageQueue)
      {
         super(name);
         this.workerId = workerId;
         this.messageQueue = messageQueue;
         setDaemon(true);
      }

      @Override
      public String toString()
      {
         return this.getClass().getName() + ":" + getWorkerId();
      }
   }

   final class Producer extends SeveralClientsStressTest.Worker
   {
      public Producer(final int producerId,
                      final LinkedBlockingQueue<SeveralClientsStressTest.InternalMessage> messageQueue)
      {
         super("Producer:" + producerId, producerId, messageQueue);
      }

      Random random = new Random();

      @Override
      public void run()
      {
         try
         {
            Context ctx = createContext();

            ConnectionFactory cf = (ConnectionFactory)ctx.lookup("/ClusteredConnectionFactory");

            Queue queue = (Queue)ctx.lookup("queue/testQueue");

            if (info)
            {
               log.info("Creating connection and producer");
            }
            Connection conn = cf.createConnection();
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer prod = sess.createProducer(queue);

            if (getWorkerId() % 2 == 0)
            {
               if (info)
               {
                  log.info("Non Persistent Producer was created");
               }
               prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
            else
            {
               if (info)
               {
                  log.info("Persistent Producer was created");
               }
               prod.setDeliveryMode(DeliveryMode.PERSISTENT);
            }

            long timeToFinish = System.currentTimeMillis() + SeveralClientsStressTest.PRODUCER_ALIVE_FOR;

            try
            {
               int messageSent = 0;
               while (System.currentTimeMillis() < timeToFinish)
               {
                  prod.send(sess.createTextMessage("Message sent at " + System.currentTimeMillis()));
                  SeveralClientsStressTest.producedMessages.incrementAndGet();
                  messageSent++;
                  if (messageSent % 50 == 0)
                  {
                     if (info)
                     {
                        log.info("Sent " + messageSent + " Messages");
                     }
                  }

                  if (SeveralClientsStressTest.LONG_WAIT_ON_PRODUCERS)
                  {
                     int waitTime = random.nextInt() % 2 + 1;
                     if (waitTime < 0)
                     {
                        waitTime *= -1;
                     }
                     Thread.sleep(waitTime * 1000); // wait 1 or 2 seconds
                  }
                  else
                  {
                     Thread.sleep(100);
                  }
               }
               sendInternalMessage(new SeveralClientsStressTest.WorkedFinishedMessages(this));
            }
            finally
            {
               conn.close();
            }

         }
         catch (Exception e)
         {
            log.error(e, e);
            setFailed(true, e);
         }
      }
   }

   final class Consumer extends SeveralClientsStressTest.Worker
   {
      public Consumer(final int consumerId,
                      final LinkedBlockingQueue<SeveralClientsStressTest.InternalMessage> messageQueue)
      {
         super("ClientConsumer:" + consumerId, consumerId, messageQueue);
      }

      @Override
      public void run()
      {
         try
         {
            Context ctx = createContext();

            ConnectionFactory cf = (ConnectionFactory)ctx.lookup("/ClusteredConnectionFactory");

            Queue queue = (Queue)ctx.lookup("queue/testQueue");

            if (info)
            {
               log.info("Creating connection and consumer");
            }
            Connection conn = cf.createConnection();
            Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = sess.createConsumer(queue);
            if (info)
            {
               log.info("ClientConsumer was created");
            }

            conn.start();

            int msgs = 0;

            int transactions = 0;

            long timeToFinish = System.currentTimeMillis() + SeveralClientsStressTest.CONSUMER_ALIVE_FOR;

            try
            {
               while (System.currentTimeMillis() < timeToFinish)
               {
                  Message msg = consumer.receive(1000);
                  if (msg != null)
                  {
                     msgs++;
                     if (msgs >= 50)
                     {
                        transactions++;
                        if (transactions % 2 == 0)
                        {
                           if (info)
                           {
                              log.info("Commit transaction");
                           }
                           sess.commit();
                           SeveralClientsStressTest.readMessages.addAndGet(msgs);
                        }
                        else
                        {
                           if (info)
                           {
                              log.info("Rollback transaction");
                           }
                           sess.rollback();
                        }
                        msgs = 0;
                     }
                  }
                  else
                  {
                     break;
                  }
               }

               SeveralClientsStressTest.readMessages.addAndGet(msgs);
               sess.commit();

               sendInternalMessage(new SeveralClientsStressTest.WorkedFinishedMessages(this));
            }
            finally
            {
               conn.close();
            }

         }
         catch (Exception e)
         {
            log.error(e);
            setFailed(true, e);
         }
      }
   }

   // Objects used on the communication between Workers and the test
   static class InternalMessage
   {
      SeveralClientsStressTest.Worker worker;

      public InternalMessage(final SeveralClientsStressTest.Worker worker)
      {
         this.worker = worker;
      }

      public SeveralClientsStressTest.Worker getWorker()
      {
         return worker;
      }

      @Override
      public String toString()
      {
         return this.getClass().getName() + " worker-> " + worker.toString();
      }
   }

   static class WorkedFinishedMessages extends SeveralClientsStressTest.InternalMessage
   {

      public WorkedFinishedMessages(final SeveralClientsStressTest.Worker worker)
      {
         super(worker);
      }

   }

   static class WorkerFailed extends SeveralClientsStressTest.InternalMessage
   {
      public WorkerFailed(final SeveralClientsStressTest.Worker worker)
      {
         super(worker);
      }
   }

}
