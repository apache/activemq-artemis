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

package org.apache.activemq.tests.integration.paging;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class PagingWithFailoverAndCountersTest extends ServiceTestBase
{

   Process liveProcess;
   Process backupProcess;

   PagingWithFailoverServer inProcessBackup;

   private static final int PORT1 = 5000;
   private static final int PORT2 = 5001;

   private void startLive() throws Exception
   {
      assertNull(liveProcess);
      liveProcess = PagingWithFailoverServer.spawnVM(getTestDir(), PORT1, PORT2);
   }

   private void startBackupInProcess() throws Exception
   {
      assertNull(backupProcess);
      assertNull(inProcessBackup);
      inProcessBackup = new PagingWithFailoverServer();
      inProcessBackup.perform(getTestDir(), PORT2, PORT1, true);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      killLive();
      killBackup();
      super.tearDown();
   }

   private void killBackup()
   {
      try
      {
         if (backupProcess != null)
         {
            backupProcess.destroy();
         }
      }
      catch (Throwable ignored)
      {
      }
      backupProcess = null;


      if (inProcessBackup != null)
      {
         try
         {
            inProcessBackup.getServer().stop(false);
         }
         catch (Throwable ignored)
         {
            ignored.printStackTrace();
         }

         inProcessBackup = null;
      }

   }

   private void killLive()
   {
      try
      {
         if (liveProcess != null)
         {
            liveProcess.destroy();
         }
      }
      catch (Throwable ignored)
      {
      }
      liveProcess = null;
   }

   class TestThread extends Thread
   {
      public TestThread()
      {
      }

      public TestThread(String name)
      {
         super(name);
      }

      boolean running = true;
      Object waitNotify = new Object();
      private boolean failed = false;

      public void failed(String message)
      {
         System.err.println(message);
         failed = true;
      }

      public boolean isFailed()
      {
         return failed;
      }


      public void stopTest()
      {
         synchronized (waitNotify)
         {
            running = false;
            waitNotify.notifyAll();
         }

         while (this.isAlive())
         {
            try
            {
               this.join(5000);
            }
            catch (Throwable ignored)
            {
            }
            if (this.isAlive())
            {
               this.interrupt();
            }
         }
         assertFalse(failed);
      }

      public boolean isRunning(long timeWait)
      {
         synchronized (waitNotify)
         {
            try
            {
               if (timeWait > 0)
               {
                  waitNotify.wait(timeWait);
               }
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
               Thread.currentThread().interrupt();
            }
            return running;
         }
      }
   }

   class ConsumerThread extends TestThread
   {
      ClientSessionFactory factory;

      String queueName;

      final AtomicInteger errors = new AtomicInteger(0);
      final int txSize;

      ConsumerThread(ClientSessionFactory factory, String queueName, long delayEachMessage, int txSize)
      {
         this.factory = factory;
         this.queueName = queueName;
         this.txSize = txSize;
      }

      public void run()
      {
         try
         {

            ClientSession session;

            if (txSize == 0)
            {
               session = factory.createSession(true, true);
            }
            else
            {
               session = factory.createSession(false, false);
            }

            ClientConsumer cons = session.createConsumer(queueName);
            session.start();

            long lastCommit = 0;

            int msgcount = 0;
            long currentMsg = 0;
            while (isRunning(0))
            {
               try
               {

                  ClientMessage msg = cons.receive(100);
                  if (msg != null)
                  {
                     currentMsg = msg.getLongProperty("count");
                     if (currentMsg < lastCommit)
                     {
                        failed("Message recieved in duplicate out of order, LastCommit = " + lastCommit + ", currentMsg = " + currentMsg);
                     }
                     msg.acknowledge();
                     if (txSize > 0 && msgcount > 0 && (msgcount % txSize == 0))
                     {
                        session.commit();
                        if (currentMsg > lastCommit)
                        {
                           lastCommit = currentMsg;
                        }
                        else
                        {
                           System.out.println("Ignoring setting lastCommit (" + lastCommit + ") <= currentMSG (" + currentMsg + ")");
                        }
                     }
                     msgcount++;
                  }

                  if (msgcount % 100 == 0)
                  {
                     System.out.println("received " + msgcount + " on " + queueName);
                  }
               }
               catch (Throwable e)
               {
                  System.out.println("=====> expected Error at " + currentMsg + " with lastCommit=" + lastCommit);
               }
            }


            session.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            errors.incrementAndGet();
         }
      }
   }

   class MonitorThread extends TestThread
   {

      public MonitorThread()
      {
         super("Monitor-thread");
      }

      public void run()
      {

         HornetQServer server = inProcessBackup.getServer();
         try
         {
            waitForServer(server);

            // The best to way to validate if the server is ready and operating is to send and consume at least one message
            // before we could do valid monitoring
            try
            {
               ServerLocator locator = PagingWithFailoverServer.createLocator(PORT2);
               locator.setInitialConnectAttempts(100);
               locator.setRetryInterval(100);
               ClientSessionFactory factory = locator.createSessionFactory();
               ClientSession session = factory.createSession();

               session.createQueue("new-queue", "new-queue");

               System.out.println("created queue");

               session.start();
               ClientProducer prod = session.createProducer("new-queue");
               prod.send(session.createMessage(true));
               ClientConsumer cons = session.createConsumer("new-queue");
               cons.receive(500).acknowledge();
               cons.close();
               session.deleteQueue("new-queue");
               locator.close();

            }
            catch (Throwable e)
            {
               e.printStackTrace();
               fail(e.getMessage());
            }
            System.out.println("Started monitoring");

            Queue queue2 = inProcessBackup.getServer().locateQueue(SimpleString.toSimpleString("cons2"));

            while (isRunning(1))
            {
               long count = getMessageCount(queue2);
               if (count < 0)
               {
                  fail("count < 0 .... being " + count);
               }
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

      }
   }

   @Test
   public void testValidateDeliveryAndCounters() throws Exception
   {
      startLive();

      ServerLocator locator = PagingWithFailoverServer.createLocator(PORT1);
      locator.setInitialConnectAttempts(100);
      locator.setReconnectAttempts(-1);
      locator.setRetryInterval(100);
      ClientSessionFactory factory = locator.createSessionFactory();


      ClientSession session = factory.createSession();

      session.createQueue("myAddress", "DeadConsumer", true);
      session.createQueue("myAddress", "cons2", true);


      startBackupInProcess();

      waitForRemoteBackup(factory, 10);

      ConsumerThread tConsumer = new ConsumerThread(factory, "cons2", 0, 10);
      tConsumer.start();


      MonitorThread monitor = new MonitorThread();

      ClientProducer prod = session.createProducer("myAddress");


      long i = 0;

      long timeRun = System.currentTimeMillis() + 5000;
      long timeKill = System.currentTimeMillis() + 2000;
      while (System.currentTimeMillis() < timeRun)
      {
         i++;
         if (System.currentTimeMillis() > timeKill && liveProcess != null)
         {
            killLive();
            monitor.start();
         }

         try
         {
            ClientMessage msg = session.createMessage(true);
            msg.putLongProperty("count", i);
            prod.send(msg);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

      try
      {
         tConsumer.stopTest();
         monitor.stopTest();
      }
      finally
      {
         killBackup();
         killLive();
      }

      factory.close();

      verifyServer();

   }

   public void verifyServer() throws Exception
   {
      ServerLocator locator;
      ClientSessionFactory factory;
      ClientSession session;

      HornetQServer server = PagingWithFailoverServer.createServer(getTestDir(), PORT1, PORT2, false);

      server.start();

      waitForServer(server);
      Queue queue = server.locateQueue(SimpleString.toSimpleString("cons2"));


      int messageCount = (int) getMessageCount(queue);

      assertTrue(messageCount >= 0);

      locator = PagingWithFailoverServer.createLocator(PORT1);
      locator.setInitialConnectAttempts(100);
      locator.setReconnectAttempts(-1);
      locator.setRetryInterval(100);
      factory = locator.createSessionFactory();


      session = factory.createSession();
      session.start();

      try
      {
         drainConsumer(session.createConsumer("cons2"), "cons2", messageCount);
      }
      finally
      {
         session.close();
         factory.close();
         locator.close();
         server.stop();
      }
   }

   private void drainConsumer(ClientConsumer consumer, String name, int expectedCount) throws Exception
   {
      for (int i = 0; i < expectedCount; i++)
      {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      assertNull(consumer.receiveImmediate());

      consumer.close();
   }
}
