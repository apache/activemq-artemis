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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionOutcomeUnknownException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionRolledBackException;
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.CountDownSessionFailureListener;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A MultiThreadFailoverTest
 * <br>
 * Test Failover where failure is prompted by another thread
 */
public class AsynchronousFailoverTest extends FailoverTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private volatile CountDownSessionFailureListener listener;

   private volatile ClientSessionFactoryInternal sf;

   private final Object lockFail = new Object();

   @Test
   public void testNonTransactional() throws Throwable {
      runTest(new TestRunner() {
         @Override
         public void run() {
            try {
               doTestNonTransactional(this);
            } catch (Throwable e) {
               AsynchronousFailoverTest.log.error("Test failed", e);
               addException(e);
            }
         }
      });
   }

   @Test
   public void testTransactional() throws Throwable {
      runTest(new TestRunner() {
         volatile boolean running = false;

         @Override
         public void run() {
            try {
               assertFalse(running);
               running = true;
               try {
                  doTestTransactional(this);
               } finally {
                  running = false;
               }
            } catch (Throwable e) {
               AsynchronousFailoverTest.log.error("Test failed", e);
               addException(e);
            }
         }
      });
   }

   abstract class TestRunner implements Runnable {

      volatile boolean failed;

      ArrayList<Throwable> errors = new ArrayList<>();

      boolean isFailed() {
         return failed;
      }

      void setFailed() {
         failed = true;
      }

      void reset() {
         failed = false;
      }

      synchronized void addException(Throwable e) {
         errors.add(e);
      }

      void checkForExceptions() throws Throwable {
         if (errors.size() > 0) {
            log.warn("Exceptions on test:");
            for (Throwable e : errors) {
               log.warn(e.getMessage(), e);
            }
            // throwing the first error that happened on the Runnable
            throw errors.get(0);
         }

      }

   }

   private void runTest(final TestRunner runnable) throws Throwable {
      final int numIts = 1;

      try {
         for (int i = 0; i < numIts; i++) {
            AsynchronousFailoverTest.log.info("Iteration " + i);
            ServerLocator locator = getServerLocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setReconnectAttempts(15).setConfirmationWindowSize(10 * 1024 * 1024);
            sf = createSessionFactoryAndWaitForTopology(locator, 2);
            try {

               ClientSession createSession = sf.createSession(true, true);

               createSession.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, (SimpleString) null, true);

               RemotingConnection conn = ((ClientSessionInternal) createSession).getConnection();

               Thread t = new Thread(runnable);

               t.setName("MainTEST");

               t.start();

               long randomDelay = (long) (2000 * Math.random());

               AsynchronousFailoverTest.log.info("Sleeping " + randomDelay);

               Thread.sleep(randomDelay);

               AsynchronousFailoverTest.log.info("Failing asynchronously");

               // Simulate failure on connection
               synchronized (lockFail) {
                  if (log.isDebugEnabled()) {
                     log.debug("#test crashing test");
                  }
                  crash(createSession);
               }

               /*if (listener != null)
               {
                  boolean ok = listener.latch.await(10000, TimeUnit.MILLISECONDS);

                  Assert.assertTrue(ok);
               }*/

               runnable.setFailed();

               AsynchronousFailoverTest.log.info("Fail complete");

               t.join(TimeUnit.SECONDS.toMillis(60));
               if (t.isAlive()) {
                  System.out.println(threadDump("Thread still running from the test"));
                  t.interrupt();
                  fail("Test didn't complete successful, thread still running");
               }

               runnable.checkForExceptions();

               createSession.close();

               Assert.assertEquals(0, sf.numSessions());

               locator.close();
            } finally {
               locator.close();

               Assert.assertEquals(0, sf.numConnections());
            }

            if (i != numIts - 1) {
               tearDown();
               runnable.checkForExceptions();
               runnable.reset();
               setUp();
            }
         }
      } finally {
      }
   }

   protected void addPayload(ClientMessage msg) {
   }

   private void doTestNonTransactional(final TestRunner runner) throws Exception {
      while (!runner.isFailed()) {
         AsynchronousFailoverTest.log.info("looping");

         ClientSession session = sf.createSession(true, true, 0);

         listener = new CountDownSessionFailureListener(session);

         session.addFailureListener(listener);

         ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

         final int numMessages = 1000;

         for (int i = 0; i < numMessages; i++) {
            boolean retry = false;
            do {
               try {
                  ClientMessage message = session.createMessage(true);

                  message.getBodyBuffer().writeString("message" + i);

                  message.putIntProperty("counter", i);

                  addPayload(message);

                  producer.send(message);

                  retry = false;
               } catch (ActiveMQUnBlockedException ube) {
                  AsynchronousFailoverTest.log.info("exception when sending message with counter " + i);

                  ube.printStackTrace();

                  retry = true;

               } catch (ActiveMQException e) {
                  fail("Invalid Exception type:" + e.getType());
               }
            }
            while (retry);
         }

         // create the consumer with retry if failover occurs during createConsumer call
         ClientConsumer consumer = null;
         boolean retry = false;
         do {
            try {
               consumer = session.createConsumer(FailoverTestBase.ADDRESS);

               retry = false;
            } catch (ActiveMQUnBlockedException ube) {
               AsynchronousFailoverTest.log.info("exception when creating consumer");

               retry = true;

            } catch (ActiveMQException e) {
               fail("Invalid Exception type:" + e.getType());
            }
         }
         while (retry);

         session.start();

         List<Integer> counts = new ArrayList<>(1000);
         int lastCount = -1;
         boolean counterGap = false;
         while (true) {
            ClientMessage message = consumer.receive(500);

            if (message == null) {
               break;
            }

            // messages must remain ordered but there could be a "jump" if messages
            // are missing or duplicated
            int count = message.getIntProperty("counter");
            counts.add(count);
            if (count != lastCount + 1) {
               if (counterGap) {
                  Assert.fail("got another counter gap at " + count + ": " + counts);
               } else {
                  if (lastCount != -1) {
                     AsynchronousFailoverTest.log.info("got first counter gap at " + count);
                     counterGap = true;
                  }
               }
            }

            lastCount = count;

            message.acknowledge();
         }

         session.close();

         this.listener = null;
      }
   }

   private void doTestTransactional(final TestRunner runner) throws Throwable {
      // For duplication detection
      int executionId = 0;

      while (!runner.isFailed()) {
         ClientSession session = null;

         executionId++;

         log.info("#test doTestTransactional starting now. Execution " + executionId);

         try {

            boolean retry = false;

            final int numMessages = 1000;

            session = sf.createSession(false, false);

            listener = new CountDownSessionFailureListener(session);
            session.addFailureListener(listener);

            do {
               try {
                  ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

                  for (int i = 0; i < numMessages; i++) {
                     ClientMessage message = session.createMessage(true);

                     message.getBodyBuffer().writeString("message" + i);

                     message.putIntProperty("counter", i);

                     message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString("id:" + i +
                                                                                                       ",exec:" +
                                                                                                       executionId));

                     addPayload(message);

                     if (log.isDebugEnabled()) {
                        log.debug("Sending message " + message);
                     }

                     producer.send(message);
                  }

                  log.debug("Sending commit");
                  session.commit();

                  retry = false;
               } catch (ActiveMQDuplicateIdException die) {
                  logAndSystemOut("#test duplicate id rejected on sending");
                  break;
               } catch (ActiveMQTransactionRolledBackException trbe) {
                  log.info("#test transaction rollback retrying on sending");
                  // OK
                  retry = true;
               } catch (ActiveMQUnBlockedException ube) {
                  log.info("#test transaction rollback retrying on sending");
                  // OK
                  retry = true;
               } catch (ActiveMQTransactionOutcomeUnknownException toue) {
                  log.info("#test transaction rollback retrying on sending");
                  // OK
                  retry = true;
               } catch (ActiveMQException e) {
                  log.info("#test Exception " + e, e);
                  throw e;
               }
            }
            while (retry);

            logAndSystemOut("#test Finished sending, starting consumption now");

            boolean blocked = false;

            retry = false;

            ClientConsumer consumer = null;
            do {
               ArrayList<Integer> msgs = new ArrayList<>();
               try {
                  if (consumer == null) {
                     consumer = session.createConsumer(FailoverTestBase.ADDRESS);
                     session.start();
                  }

                  for (int i = 0; i < numMessages; i++) {
                     if (log.isDebugEnabled()) {
                        log.debug("Consumer receiving message " + i);
                     }
                     ClientMessage message = consumer.receive(10000);
                     if (message == null) {
                        break;
                     }

                     if (log.isDebugEnabled()) {
                        log.debug("Received message " + message);
                     }

                     int count = message.getIntProperty("counter");

                     if (count != i) {
                        log.warn("count was received out of order, " + count + "!=" + i);
                     }

                     msgs.add(count);

                     message.acknowledge();
                  }

                  log.info("#test commit");
                  try {
                     session.commit();
                  } catch (ActiveMQTransactionRolledBackException trbe) {
                     //we know the tx has been rolled back so we just consume again
                     retry = true;
                     continue;
                  } catch (ActiveMQException e) {
                     // This could eventually happen
                     // We will get rid of this when we implement 2 phase commit on failover
                     log.warn("exception during commit, it will be ignored for now" + e.getMessage(), e);
                  }

                  try {
                     if (blocked) {
                        assertTrue("msgs.size is expected to be 0 or " + numMessages + " but it was " + msgs.size(), msgs.size() == 0 || msgs.size() == numMessages);
                     } else {
                        assertTrue("msgs.size is expected to be " + numMessages + " but it was " + msgs.size(), msgs.size() == numMessages);
                     }
                  } catch (Throwable e) {
                     log.info(threadDump("Thread dump, messagesReceived = " + msgs.size()));
                     logAndSystemOut(e.getMessage() + " messages received");
                     for (Integer msg : msgs) {
                        logAndSystemOut(msg.toString());
                     }
                     throw e;
                  }

                  int i = 0;
                  for (Integer msg : msgs) {
                     assertEquals(i++, (int) msg);
                  }

                  retry = false;
                  blocked = false;
               } catch (ActiveMQTransactionRolledBackException trbe) {
                  logAndSystemOut("Transaction rolled back with " + msgs.size(), trbe);
                  // TODO: https://jira.jboss.org/jira/browse/HORNETQ-369
                  // ATM RolledBack exception is being called with the transaction is committed.
                  // the test will fail if you remove this next line
                  blocked = true;
                  retry = true;
               } catch (ActiveMQTransactionOutcomeUnknownException tou) {
                  logAndSystemOut("Transaction rolled back with " + msgs.size(), tou);
                  // TODO: https://jira.jboss.org/jira/browse/HORNETQ-369
                  // ATM RolledBack exception is being called with the transaction is committed.
                  // the test will fail if you remove this next line
                  blocked = true;
                  retry = true;
               } catch (ActiveMQUnBlockedException ube) {
                  logAndSystemOut("Unblocked with " + msgs.size(), ube);
                  // TODO: https://jira.jboss.org/jira/browse/HORNETQ-369
                  // This part of the test is never being called.
                  blocked = true;
                  retry = true;
               } catch (ActiveMQException e) {
                  logAndSystemOut(e.getMessage(), e);
                  throw e;
               }
            }
            while (retry);
         } finally {
            if (session != null) {
               session.close();
            }
         }

         listener = null;
      }
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

}
