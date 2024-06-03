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
package org.apache.activemq.artemis.tests.integration.cluster.reattach;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.protocol.core.impl.RemotingConnectionImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnector;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public abstract class MultiThreadReattachSupportTestBase extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private Timer timer;

   protected abstract void start() throws Exception;

   protected abstract void stop() throws Exception;

   protected abstract ServerLocator createLocator() throws Exception;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
      timer = new Timer();
   }

   @Override
   @AfterEach
   public void tearDown() throws Exception {
      timer.cancel();
      timer = null;
      super.tearDown();
   }

   protected boolean shouldFail() {
      return true;
   }

   protected void runMultipleThreadsFailoverTest(final RunnableT runnable,
                                                 final int numThreads,
                                                 final int numIts,
                                                 final boolean failOnCreateConnection,
                                                 final long failDelay) throws Exception {
      for (int its = 0; its < numIts; its++) {
         logger.debug("Beginning iteration {}", its);

         start();

         final ServerLocator locator = createLocator();

         final ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) createSessionFactory(locator);

         final ClientSession session = addClientSession(sf.createSession(false, true, true));

         Failer failer = startFailer(failDelay, session, failOnCreateConnection);

         class Runner extends Thread {

            private volatile Throwable throwable;

            private final RunnableT test;

            private final int threadNum;

            Runner(final RunnableT test, final int threadNum) {
               this.test = test;

               this.threadNum = threadNum;
            }

            @Override
            public void run() {
               try {
                  test.run(sf, threadNum);
               } catch (Throwable t) {
                  throwable = t;

                  logger.error("Failed to run test", t);

                  // Case a failure happened here, it should print the Thread dump
                  // Sending it to System.out, as it would show on the Tests report
                  System.out.println(ActiveMQTestBase.threadDump(" - fired by MultiThreadRandomReattachTestBase::runTestMultipleThreads (" + t.getLocalizedMessage() +
                                                                    ")"));
               }
            }
         }

         do {
            List<Runner> threads = new ArrayList<>();

            for (int i = 0; i < numThreads; i++) {
               Runner runner = new Runner(runnable, i);

               threads.add(runner);

               runner.start();
            }

            for (Runner thread : threads) {
               thread.join();

               if (thread.throwable != null) {
                  throw new Exception("Exception on thread " + thread, thread.throwable);
               }
            }

            runnable.checkFail();

         }
         while (!failer.isExecuted());

         InVMConnector.resetFailures();

         session.close();

         locator.close();

         assertEquals(0, sf.numSessions());

         assertEquals(0, sf.numConnections());

         sf.close();

         stop();
      }
   }


   private Failer startFailer(final long time, final ClientSession session, final boolean failOnCreateConnection) {
      Failer failer = new Failer(session, failOnCreateConnection);

      // This is useful for debugging.. just change shouldFail to return false, and Failer will not be executed
      if (shouldFail()) {
         timer.schedule(failer, (long) (time * Math.random()), 100);
      }

      return failer;
   }


   protected abstract class RunnableT extends Thread {

      private volatile String failReason;

      private volatile Throwable throwable;

      public void setFailed(final String reason, final Throwable throwable) {
         failReason = reason;
         this.throwable = throwable;
      }

      public void checkFail() {
         if (throwable != null) {
            logger.error("Test failed: {}", failReason, throwable);
         }
         if (failReason != null) {
            fail(failReason);
         }
      }

      public abstract void run(ClientSessionFactory sf, int threadNum) throws Exception;
   }

   private class Failer extends TimerTask {

      private final ClientSession session;

      private boolean executed;

      private final boolean failOnCreateConnection;

      private Failer(final ClientSession session, final boolean failOnCreateConnection) {
         this.session = session;

         this.failOnCreateConnection = failOnCreateConnection;
      }

      @Override
      public synchronized void run() {
         logger.debug("** Failing connection");

         RemotingConnectionImpl conn = (RemotingConnectionImpl) ((ClientSessionInternal) session).getConnection();

         if (failOnCreateConnection) {
            InVMConnector.numberOfFailures = 1;
            InVMConnector.failOnCreateConnection = true;
         } else {
            conn.fail(new ActiveMQNotConnectedException("blah"));
         }

         logger.debug("** Fail complete");

         cancel();

         executed = true;
      }

      public synchronized boolean isExecuted() {
         return executed;
      }
   }

}
