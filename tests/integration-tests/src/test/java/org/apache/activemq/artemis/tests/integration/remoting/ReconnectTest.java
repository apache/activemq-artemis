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
package org.apache.activemq.artemis.tests.integration.remoting;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;

public class ReconnectTest extends ActiveMQTestBase {

   @Test
   public void testReconnectNetty() throws Exception {
      internalTestReconnect(true);
   }

   @Test
   public void testReconnectInVM() throws Exception {
      internalTestReconnect(false);
   }

   public void internalTestReconnect(final boolean isNetty) throws Exception {
      final int pingPeriod = 1000;

      ActiveMQServer server = createServer(false, isNetty);

      server.start();

      ClientSessionInternal session = null;

      try {
         ServerLocator locator = createFactory(isNetty).setClientFailureCheckPeriod(pingPeriod).setRetryInterval(500).setRetryIntervalMultiplier(1d).setReconnectAttempts(-1).setConfirmationWindowSize(1024 * 1024);
         ClientSessionFactory factory = createSessionFactory(locator);

         session = (ClientSessionInternal) factory.createSession();

         final AtomicInteger count = new AtomicInteger(0);

         final CountDownLatch latch = new CountDownLatch(1);

         session.addFailureListener(new SessionFailureListener() {
            @Override
            public void connectionFailed(final ActiveMQException me, boolean failedOver) {
               count.incrementAndGet();
               latch.countDown();
            }

            @Override
            public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
               connectionFailed(me, failedOver);
            }

            public void beforeReconnect(final ActiveMQException exception) {
            }

         });

         server.stop();

         Thread.sleep((pingPeriod * 2));

         server.start();

         Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

         // Some time to let possible loops to occur
         Thread.sleep(500);

         Assert.assertEquals(1, count.get());

         locator.close();
      }
      finally {
         try {
            session.close();
         }
         catch (Throwable e) {
         }

         server.stop();
      }

   }

   @Test
   public void testInterruptReconnectNetty() throws Exception {
      internalTestInterruptReconnect(true, false);
   }

   @Test
   public void testInterruptReconnectInVM() throws Exception {
      internalTestInterruptReconnect(false, false);
   }

   @Test
   public void testInterruptReconnectNettyInterruptMainThread() throws Exception {
      internalTestInterruptReconnect(true, true);
   }

   @Test
   public void testInterruptReconnectInVMInterruptMainThread() throws Exception {
      internalTestInterruptReconnect(false, true);
   }

   public void internalTestInterruptReconnect(final boolean isNetty,
                                              final boolean interruptMainThread) throws Exception {
      final int pingPeriod = 1000;

      ActiveMQServer server = createServer(false, isNetty);

      server.start();

      try {
         ServerLocator locator = createFactory(isNetty).setClientFailureCheckPeriod(pingPeriod).setRetryInterval(500).setRetryIntervalMultiplier(1d).setReconnectAttempts(-1).setConfirmationWindowSize(1024 * 1024);
         ClientSessionFactoryInternal factory = (ClientSessionFactoryInternal) locator.createSessionFactory();

         // One for beforeReconnecto from the Factory, and one for the commit about to be done
         final CountDownLatch latchCommit = new CountDownLatch(2);

         final ArrayList<Thread> threadToBeInterrupted = new ArrayList<Thread>();

         factory.addFailureListener(new SessionFailureListener() {

            @Override
            public void connectionFailed(ActiveMQException exception, boolean failedOver) {
            }

            @Override
            public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
               connectionFailed(me, failedOver);
            }

            @Override
            public void beforeReconnect(ActiveMQException exception) {
               latchCommit.countDown();
               threadToBeInterrupted.add(Thread.currentThread());
               System.out.println("Thread " + Thread.currentThread() + " reconnecting now");
            }
         });

         final ClientSessionInternal session = (ClientSessionInternal) factory.createSession();

         final AtomicInteger count = new AtomicInteger(0);

         final CountDownLatch latch = new CountDownLatch(1);

         session.addFailureListener(new SessionFailureListener() {

            public void connectionFailed(final ActiveMQException me, boolean failedOver) {
               count.incrementAndGet();
               latch.countDown();
            }

            @Override
            public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
               connectionFailed(me, failedOver);
            }

            public void beforeReconnect(final ActiveMQException exception) {
            }

         });

         server.stop();

         Thread tcommitt = new Thread() {
            public void run() {
               latchCommit.countDown();
               try {
                  session.commit();
               }
               catch (ActiveMQException e) {
                  e.printStackTrace();
               }
            }
         };

         tcommitt.start();
         assertTrue(latchCommit.await(10, TimeUnit.SECONDS));

         // There should be only one thread
         assertEquals(1, threadToBeInterrupted.size());

         if (interruptMainThread) {
            tcommitt.interrupt();
         }
         else {
            for (Thread tint : threadToBeInterrupted) {
               tint.interrupt();
            }
         }
         tcommitt.join(5000);

         assertFalse(tcommitt.isAlive());

         locator.close();
      }
      finally {
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
