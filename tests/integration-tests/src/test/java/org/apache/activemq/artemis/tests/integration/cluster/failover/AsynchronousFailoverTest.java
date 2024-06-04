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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException;
import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionOutcomeUnknownException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionRolledBackException;
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
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
import org.apache.activemq.artemis.tests.util.CountDownSessionFailureListener;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * A MultiThreadFailoverTest
 * <br>
 * Test Failover where failure is prompted by another thread
 */
public class AsynchronousFailoverTest extends FailoverTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
               logger.error("Test failed", e);
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
               logger.error("Test failed", e);
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
            logger.warn("Exceptions on test:");
            for (Throwable e : errors) {
               logger.warn(e.getMessage(), e);
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
            logger.debug("Iteration {}", i);
            //set block timeout to 10 sec to reduce test time.
            ServerLocator locator = getServerLocator().setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setReconnectAttempts(30).setRetryInterval(100).setConfirmationWindowSize(10 * 1024 * 1024).setCallTimeout(10000).setCallFailoverTimeout(10000);

            sf = createSessionFactoryAndWaitForTopology(locator, 2);
            try {

               ClientSession createSession = sf.createSession(true, true);

               createSession.createQueue(QueueConfiguration.of(FailoverTestBase.ADDRESS).setAddress(FailoverTestBase.ADDRESS));

               RemotingConnection conn = ((ClientSessionInternal) createSession).getConnection();

               Thread t = new Thread(runnable);

               t.setName("MainTEST");

               t.start();

               long randomDelay = (long) (2000 * Math.random());

               logger.debug("Sleeping {}", randomDelay);

               Thread.sleep(randomDelay);

               logger.debug("Failing asynchronously");

               // Simulate failure on connection
               synchronized (lockFail) {
                  logger.debug("#test crashing test");
                  crash(createSession);
               }

               /*if (listener != null)
               {
                  boolean ok = listener.latch.await(10000, TimeUnit.MILLISECONDS);

                  Assert.assertTrue(ok);
               }*/

               runnable.setFailed();

               logger.debug("Fail complete");

               t.join(TimeUnit.SECONDS.toMillis(120));
               if (t.isAlive()) {
                  System.out.println(threadDump("Thread still running from the test"));
                  t.interrupt();
                  fail("Test didn't complete successful, thread still running");
               }

               runnable.checkForExceptions();

               createSession.close();

               assertEquals(0, sf.numSessions());

               locator.close();
            } finally {
               locator.close();

               assertEquals(0, sf.numConnections());
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
         logger.debug("looping");

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
                  logger.debug("exception when sending message with counter {}", i);

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
               logger.debug("exception when creating consumer");

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
                  fail("got another counter gap at " + count + ": " + counts);
               } else {
                  if (lastCount != -1) {
                     logger.debug("got first counter gap at {}", count);
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

         logger.debug("#test doTestTransactional starting now. Execution {}", executionId);

         try {

            boolean retry = false;

            final int numMessages = 1000;

            int retryCreateSession = 4;
            //session creation may fail in the middle of failover
            while (session == null) {
               try {
                  //if autoCommitSends is false, send will be non-blocking
                  session = sf.createSession(true, false);
               } catch (ActiveMQException e) {
                  if (retryCreateSession == 0) {
                     throw e;
                  }
                  retryCreateSession--;
                  Thread.sleep(2000);
               }
            }

            listener = new CountDownSessionFailureListener(session);
            session.addFailureListener(listener);

            do {
               if (runner.isFailed()) {
                  //test ends, return
                  return;
               }
               try {
                  ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

                  for (int i = 0; i < numMessages; i++) {
                     ClientMessage message = session.createMessage(true);

                     message.getBodyBuffer().writeString("message" + i);

                     message.putIntProperty("counter", i);

                     message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, SimpleString.of("id:" + i +
                                                                                                       ",exec:" +
                                                                                                       executionId));

                     addPayload(message);

                     logger.debug("Sending message {}", message);

                     producer.send(message);
                  }

                  logger.debug("Sending commit");
                  session.commit();

                  retry = false;
               } catch (ActiveMQDuplicateIdException die) {
                  logAndSystemOut("#test duplicate id rejected on sending");
                  break;
               } catch (ActiveMQTransactionRolledBackException trbe) {
                  logger.debug("#test transaction rollback retrying on sending");
                  // OK
                  retry = true;
               } catch (ActiveMQUnBlockedException ube) {
                  logger.debug("#test transaction rollback retrying on sending");
                  // OK
                  retry = true;
               } catch (ActiveMQTransactionOutcomeUnknownException toue) {
                  logger.debug("#test transaction rollback retrying on sending");
                  // OK
                  retry = true;
               } catch (ActiveMQObjectClosedException closedException) {
                  logger.debug("#test producer closed, retrying on sending...");
                  Thread.sleep(2000);
                  // OK
                  retry = true;
               } catch (ActiveMQConnectionTimedOutException timedoutEx) {
                  //commit timedout because of server crash. retry
                  //will be ok after failover
                  Thread.sleep(2000);
                  retry = true;
               } catch (ActiveMQException e) {
                  logger.debug("#test Exception {}", e.getMessage(), e);
                  throw e;
               }
            }
            while (retry);

            logAndSystemOut("#test Finished sending, starting consumption now");

            boolean blocked = false;

            retry = false;

            ClientConsumer consumer = null;
            do {
               if (runner.isFailed()) {
                  //test ends, return
                  return;
               }
               ArrayList<Integer> msgs = new ArrayList<>();
               try {
                  int retryCreate = 4;
                  while (consumer == null) {
                     try {
                        consumer = session.createConsumer(FailoverTestBase.ADDRESS);
                     } catch (ActiveMQObjectClosedException closedEx) {
                        //the session may just crashed and failover not done yet
                        if (retryCreate == 0) {
                           throw closedEx;
                        }
                        Thread.sleep(2000);
                        retryCreate--;
                     }
                  }
                  session.start();

                  for (int i = 0; i < numMessages; i++) {
                     logger.debug("Consumer receiving message {}", i);

                     ClientMessage message = consumer.receive(60000);
                     if (message == null) {
                        break;
                     }

                     logger.debug("Received message {}", message);

                     int count = message.getIntProperty("counter");

                     if (count != i) {
                        logger.warn("count was received out of order, {}!={}", count, i);
                     }

                     msgs.add(count);

                     message.acknowledge();
                  }

                  logger.debug("#test commit");
                  try {
                     session.commit();
                  } catch (ActiveMQTransactionRolledBackException trbe) {
                        //we know the tx has been rolled back so we just consume again
                     retry = true;
                     continue;
                  } catch (ActiveMQException e) {
                     // This could eventually happen
                     // We will get rid of this when we implement 2 phase commit on failover
                     logger.warn("exception during commit, continue {}", e.getMessage(), e);
                     continue;
                  }

                  try {
                     if (blocked) {
                        assertTrue(msgs.size() == 0 || msgs.size() == numMessages, "msgs.size is expected to be 0 or " + numMessages + " but it was " + msgs.size());
                     } else {
                        assertTrue(msgs.size() == numMessages, "msgs.size is expected to be " + numMessages + " but it was " + msgs.size());
                     }
                  } catch (Throwable e) {
                     if (logger.isDebugEnabled()) {
                        String dumpMessage = "Thread dump, messagesReceived = " + msgs.size();
                        logger.debug(threadDump(dumpMessage));
                     }
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
