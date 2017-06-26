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
package org.apache.activemq.artemis.tests.integration.paging;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Test;

public class PagingWithFailoverAndCountersTest extends ActiveMQTestBase {

   Process liveProcess;
   Process backupProcess;

   PagingWithFailoverServer inProcessBackup;

   private static final int PORT1 = 5000;
   private static final int PORT2 = 5001;

   private void startLive() throws Exception {
      assertNull(liveProcess);
      liveProcess = PagingWithFailoverServer.spawnVM(getTestDir(), PORT1, PORT2);
   }

   private void startBackupInProcess() throws Exception {
      assertNull(backupProcess);
      assertNull(inProcessBackup);
      inProcessBackup = new PagingWithFailoverServer();
      inProcessBackup.perform(getTestDir(), PORT2, PORT1, true);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      killLive();
      killBackup();
      super.tearDown();
   }

   private void killBackup() {
      try {
         if (backupProcess != null) {
            backupProcess.destroy();
         }
      } catch (Throwable ignored) {
      }
      backupProcess = null;

      if (inProcessBackup != null) {
         try {
            inProcessBackup.getServer().fail(false);
         } catch (Throwable ignored) {
            ignored.printStackTrace();
         }

         inProcessBackup = null;
      }

   }

   private void killLive() {
      try {
         if (liveProcess != null) {
            liveProcess.destroy();
         }
      } catch (Throwable ignored) {
      }
      liveProcess = null;
   }

   class TestThread extends Thread {

      TestThread() {
      }

      TestThread(String name) {
         super(name);
      }

      boolean running = true;
      final Object waitNotify = new Object();
      private boolean failed = false;

      public void failed(String message) {
         System.err.println(message);
         failed = true;
      }

      public boolean isFailed() {
         return failed;
      }

      public void stopTest() {
         synchronized (waitNotify) {
            running = false;
            waitNotify.notifyAll();
         }

         while (this.isAlive()) {
            try {
               this.join(5000);
            } catch (Throwable ignored) {
            }
            if (this.isAlive()) {
               this.interrupt();
            }
         }
         assertFalse(failed);
      }

      public boolean isRunning(long timeWait) {
         synchronized (waitNotify) {
            try {
               if (timeWait > 0) {
                  long timeout = System.currentTimeMillis() + timeWait;
                  while (running && timeout > System.currentTimeMillis()) {
                     waitNotify.wait(timeWait);
                  }
               }
            } catch (InterruptedException e) {
               e.printStackTrace();
               Thread.currentThread().interrupt();
            }
            return running;
         }
      }
   }

   class ConsumerThread extends TestThread {

      ClientSessionFactory factory;

      String queueName;

      final AtomicInteger errors = new AtomicInteger(0);
      final int txSize;

      ConsumerThread(ClientSessionFactory factory, String queueName, long delayEachMessage, int txSize) {
         this.factory = factory;
         this.queueName = queueName;
         this.txSize = txSize;
      }

      @Override
      public void run() {
         try {

            ClientSession session;

            if (txSize == 0) {
               session = factory.createSession(true, true);
            } else {
               session = factory.createSession(false, false);
            }

            ClientConsumer cons = session.createConsumer(queueName);
            session.start();

            long lastCommit = 0;

            int msgcount = 0;
            long currentMsg = 0;
            while (isRunning(0)) {
               try {

                  ClientMessage msg = cons.receive(100);
                  if (msg != null) {
                     currentMsg = msg.getLongProperty("count");
                     if (currentMsg < lastCommit) {
                        failed("Message received in duplicate out of order, LastCommit = " + lastCommit + ", currentMsg = " + currentMsg);
                     }
                     msg.acknowledge();
                     if (txSize > 0 && msgcount > 0 && (msgcount % txSize == 0)) {
                        session.commit();
                        if (currentMsg > lastCommit) {
                           lastCommit = currentMsg;
                        } else {
                           System.out.println("Ignoring setting lastCommit (" + lastCommit + ") <= currentMSG (" + currentMsg + ")");
                        }
                     }
                     msgcount++;
                  }

                  if (msgcount % 100 == 0) {
                     System.out.println("received " + msgcount + " on " + queueName);
                  }
               } catch (Throwable e) {
                  System.out.println("=====> expected Error at " + currentMsg + " with lastCommit=" + lastCommit);
               }
            }

            session.close();
         } catch (Exception e) {
            e.printStackTrace();
            errors.incrementAndGet();
         }
      }
   }

   class MonitorThread extends TestThread {

      MonitorThread() {
         super("Monitor-thread");
      }

      @Override
      public void run() {

         ActiveMQServer server = inProcessBackup.getServer();
         try {
            waitForServerToStart(server);

            // The best to way to validate if the server is ready and operating is to send and consume at least one message
            // before we could do valid monitoring
            try {
               ServerLocator locator = SpawnedServerSupport.createLocator(PORT2).setInitialConnectAttempts(100).setRetryInterval(100);
               ClientSessionFactory factory = locator.createSessionFactory();
               ClientSession session = factory.createSession();

               session.createQueue("new-queue", RoutingType.ANYCAST, "new-queue");

               System.out.println("created queue");

               session.start();
               ClientProducer prod = session.createProducer("new-queue");
               prod.send(session.createMessage(true));
               ClientConsumer cons = session.createConsumer("new-queue");
               cons.receive(500).acknowledge();
               cons.close();
               session.deleteQueue("new-queue");
               locator.close();

            } catch (Throwable e) {
               e.printStackTrace();
               fail(e.getMessage());
            }
            System.out.println("Started monitoring");

            Queue queue2 = inProcessBackup.getServer().locateQueue(SimpleString.toSimpleString("cons2"));

            while (isRunning(1)) {
               long count = getMessageCount(queue2);
               if (count < 0) {
                  fail("count < 0 .... being " + count);
               }
            }
         } catch (Exception e) {
            e.printStackTrace();
         }

      }
   }

   @Test
   public void testValidateDeliveryAndCounters() throws Exception {
      startLive();

      ServerLocator locator = SpawnedServerSupport.createLocator(PORT1).setInitialConnectAttempts(300).setReconnectAttempts(300).setRetryInterval(100);

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
      while (System.currentTimeMillis() < timeRun) {
         i++;
         if (System.currentTimeMillis() > timeKill && liveProcess != null) {
            killLive();
            monitor.start();
         }

         try {
            ClientMessage msg = session.createMessage(true);
            msg.putLongProperty("count", i);
            prod.send(msg);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      try {
         tConsumer.stopTest();
         monitor.stopTest();
      } finally {
         killBackup();
         killLive();
      }

      factory.close();

      verifyServer();
   }

   public void verifyServer() throws Exception {
      ServerLocator locator;
      ClientSessionFactory factory;
      ClientSession session;

      ActiveMQServer server = PagingWithFailoverServer.createServer(getTestDir(), PORT1, PORT2, false);

      server.start();

      waitForServerToStart(server);
      Queue queue = server.locateQueue(SimpleString.toSimpleString("cons2"));

      int messageCount = getMessageCount(queue);

      assertTrue(messageCount >= 0);

      locator = SpawnedServerSupport.createLocator(PORT1).setInitialConnectAttempts(100).setReconnectAttempts(300).setRetryInterval(100);

      factory = locator.createSessionFactory();

      session = factory.createSession();
      session.start();

      try {
         drainConsumer(session.createConsumer("cons2"), "cons2", messageCount);
      } finally {
         session.close();
         factory.close();
         locator.close();
         server.stop();
      }
   }

   private void drainConsumer(ClientConsumer consumer, String name, int expectedCount) throws Exception {
      for (int i = 0; i < expectedCount; i++) {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      assertNull(consumer.receiveImmediate());

      consumer.close();
   }
}
