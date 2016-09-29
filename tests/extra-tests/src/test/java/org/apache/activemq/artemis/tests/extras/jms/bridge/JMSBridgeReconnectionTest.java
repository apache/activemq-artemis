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
package org.apache.activemq.artemis.tests.extras.jms.bridge;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.jms.bridge.ConnectionFactoryFactory;
import org.apache.activemq.artemis.jms.bridge.QualityOfServiceMode;
import org.apache.activemq.artemis.jms.bridge.impl.JMSBridgeImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.ra.DummyTransactionManager;
import org.junit.Assert;
import org.junit.Test;

public class JMSBridgeReconnectionTest extends BridgeTestBase {

   /**
    *
    */
   private static final int TIME_WAIT = 5000;

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Crash and reconnect

   // Once and only once

   @Test
   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_P() throws Exception {
      performCrashAndReconnectDestBasic(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, false);
   }

   @Test
   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_P_LargeMessage() throws Exception {
      performCrashAndReconnectDestBasic(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, true, true);
   }

   @Test
   public void testCrashAndReconnectDestBasic_OnceAndOnlyOnce_NP() throws Exception {
      performCrashAndReconnectDestBasic(QualityOfServiceMode.ONCE_AND_ONLY_ONCE, false, false);
   }

   // dups ok

   @Test
   public void testCrashAndReconnectDestBasic_DuplicatesOk_P() throws Exception {
      performCrashAndReconnectDestBasic(QualityOfServiceMode.DUPLICATES_OK, true, false);
   }

   @Test
   public void testCrashAndReconnectDestBasic_DuplicatesOk_NP() throws Exception {
      performCrashAndReconnectDestBasic(QualityOfServiceMode.DUPLICATES_OK, false, false);
   }

   // At most once

   @Test
   public void testCrashAndReconnectDestBasic_AtMostOnce_P() throws Exception {
      performCrashAndReconnectDestBasic(QualityOfServiceMode.AT_MOST_ONCE, true, false);
   }

   @Test
   public void testCrashAndReconnectDestBasic_AtMostOnce_NP() throws Exception {
      performCrashAndReconnectDestBasic(QualityOfServiceMode.AT_MOST_ONCE, false, false);
   }

   // Crash tests specific to XA transactions

   @Test
   public void testCrashAndReconnectDestCrashBeforePrepare_P() throws Exception {
      performCrashAndReconnectDestCrashBeforePrepare(true);
   }

   @Test
   public void testCrashAndReconnectDestCrashBeforePrepare_NP() throws Exception {
      performCrashAndReconnectDestCrashBeforePrepare(false);
   }

   // Crash before bridge is started

   @Test
   public void testRetryConnectionOnStartup() throws Exception {
      jmsServer1.stop();

      JMSBridgeImpl bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 1000, -1, QualityOfServiceMode.DUPLICATES_OK, 10, -1, null, null, false).setBridgeName("test-bridge");
      bridge.setTransactionManager(newTransactionManager());
      addActiveMQComponent(bridge);
      bridge.start();
      Assert.assertFalse(bridge.isStarted());
      Assert.assertTrue(bridge.isFailed());

      // Restart the server
      jmsServer1.start();

      createQueue("targetQueue", 1);
      setUpAdministeredObjects();

      Thread.sleep(3000);

      Assert.assertTrue(bridge.isStarted());
      Assert.assertFalse(bridge.isFailed());
   }

   /**
    * https://jira.jboss.org/jira/browse/HORNETQ-287
    */
   @Test
   public void testStopBridgeWithFailureWhenStarted() throws Exception {
      jmsServer1.stop();

      JMSBridgeImpl bridge = new JMSBridgeImpl(cff0, cff1, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 500, -1, QualityOfServiceMode.DUPLICATES_OK, 10, -1, null, null, false).setBridgeName("test-bridge");
      bridge.setTransactionManager(newTransactionManager());

      bridge.start();
      Assert.assertFalse(bridge.isStarted());
      Assert.assertTrue(bridge.isFailed());

      bridge.stop();
      Assert.assertFalse(bridge.isStarted());

      // we restart and setup the server for the test's tearDown checks
      jmsServer1.start();
      createQueue("targetQueue", 1);
      setUpAdministeredObjects();
   }

   /*
    * Send some messages
    * Crash the destination server
    * Bring the destination server back up
    * Send some more messages
    * Verify all messages are received
    */
   private void performCrashAndReconnectDestBasic(final QualityOfServiceMode qosMode,
                                                  final boolean persistent,
                                                  final boolean largeMessage) throws Exception {
      JMSBridgeImpl bridge = null;

      ConnectionFactoryFactory factInUse0 = cff0;
      ConnectionFactoryFactory factInUse1 = cff1;
      if (qosMode.equals(QualityOfServiceMode.ONCE_AND_ONLY_ONCE)) {
         factInUse0 = cff0xa;
         factInUse1 = cff1xa;
      }

      bridge = new JMSBridgeImpl(factInUse0, factInUse1, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 1000, -1, qosMode, 10, -1, null, null, false).setBridgeName("test-bridge");
      addActiveMQComponent(bridge);
      bridge.setTransactionManager(newTransactionManager());
      bridge.start();

      final int NUM_MESSAGES = 10;

      // Send some messages

      sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent, largeMessage);

      // Verify none are received

      checkEmpty(targetQueue, 1);

      // Now crash the dest server

      JMSBridgeReconnectionTest.log.info("About to crash server");

      jmsServer1.stop();

      // Wait a while before starting up to simulate the dest being down for a while
      JMSBridgeReconnectionTest.log.info("Waiting 5 secs before bringing server back up");
      Thread.sleep(TIME_WAIT);
      JMSBridgeReconnectionTest.log.info("Done wait");

      // Restart the server
      JMSBridgeReconnectionTest.log.info("Restarting server");
      jmsServer1.start();

      // jmsServer1.createQueue(false, "targetQueue", null, true, "queue/targetQueue");

      createQueue("targetQueue", 1);

      setUpAdministeredObjects();

      // Send some more messages

      JMSBridgeReconnectionTest.log.info("Sending more messages");

      sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent, largeMessage);

      JMSBridgeReconnectionTest.log.info("Sent messages");

      jmsServer1.stop();

      bridge.stop();

      System.out.println("JMSBridgeReconnectionTest.performCrashAndReconnectDestBasic");
   }

   @Test
   public void performCrashDestinationStopBridge() throws Exception {
      ConnectionFactoryFactory factInUse0 = cff0;
      ConnectionFactoryFactory factInUse1 = cff1;
      final JMSBridgeImpl bridge = new JMSBridgeImpl(factInUse0, factInUse1, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 1000, -1, QualityOfServiceMode.DUPLICATES_OK, 10, -1, null, null, false).setBridgeName("test-bridge");

      addActiveMQComponent(bridge);
      bridge.setTransactionManager(newTransactionManager());
      bridge.start();

      Thread clientThread = new Thread(new Runnable() {
         @Override
         public void run() {
            while (bridge.isStarted()) {
               try {
                  sendMessages(cf0, sourceQueue, 0, 1, false, false);
               } catch (Exception e) {
                  e.printStackTrace();
               }
            }
         }
      });

      clientThread.start();

      checkAllMessageReceivedInOrder(cf1, targetQueue, 0, 1, false);

      JMSBridgeReconnectionTest.log.info("About to crash server");

      jmsServer1.stop();

      // Wait a while before starting up to simulate the dest being down for a while
      JMSBridgeReconnectionTest.log.info("Waiting 5 secs before bringing server back up");
      Thread.sleep(TIME_WAIT);
      JMSBridgeReconnectionTest.log.info("Done wait");

      bridge.stop();

      clientThread.join(5000);

      assertTrue(!clientThread.isAlive());
   }

   @Test
   public void performCrashAndReconnect() throws Exception {
      performCrashAndReconnect(true);
   }

   @Test
   public void performCrashAndNoReconnect() throws Exception {
      performCrashAndReconnect(false);
   }

   private void performCrashAndReconnect(boolean restart) throws Exception {
      cff1xa = new ConnectionFactoryFactory() {
         @Override
         public Object createConnectionFactory() throws Exception {
            ActiveMQXAConnectionFactory cf = (ActiveMQXAConnectionFactory) ActiveMQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.XA_CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY, params1));

            // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
            cf.setReconnectAttempts(-1);
            cf.setBlockOnNonDurableSend(true);
            cf.setBlockOnDurableSend(true);
            cf.setCacheLargeMessagesClient(true);

            return cf;
         }

      };

      DummyTransactionManager tm = new DummyTransactionManager();
      DummyTransaction tx = new DummyTransaction();
      tm.tx = tx;

      JMSBridgeImpl bridge = new JMSBridgeImpl(cff0xa, cff1xa, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 1000, -1, QualityOfServiceMode.ONCE_AND_ONLY_ONCE, 10, 5000, null, null, false).setBridgeName("test-bridge");
      addActiveMQComponent(bridge);
      bridge.setTransactionManager(tm);

      bridge.start();

      // Now crash the dest server

      JMSBridgeReconnectionTest.log.info("About to crash server");

      jmsServer1.stop();

      if (restart) {
         jmsServer1.start();
      }
      // Wait a while before starting up to simulate the dest being down for a while
      JMSBridgeReconnectionTest.log.info("Waiting 5 secs before bringing server back up");
      Thread.sleep(TIME_WAIT);
      JMSBridgeReconnectionTest.log.info("Done wait");

      bridge.stop();

      if (restart) {
         assertTrue(tx.rolledback);
         assertTrue(tx.targetConnected);
      } else {
         assertTrue(tx.rolledback);
         assertFalse(tx.targetConnected);
      }
   }

   private class DummyTransaction implements Transaction {

      boolean rolledback = false;
      ClientSession targetSession;
      boolean targetConnected = false;

      @Override
      public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {

      }

      @Override
      public void rollback() throws IllegalStateException, SystemException {
         rolledback = true;
         targetConnected = !targetSession.isClosed();
      }

      @Override
      public void setRollbackOnly() throws IllegalStateException, SystemException {

      }

      @Override
      public int getStatus() throws SystemException {
         return 0;
      }

      @Override
      public boolean enlistResource(XAResource xaResource) throws RollbackException, IllegalStateException, SystemException {
         targetSession = (ClientSession) xaResource;
         return false;
      }

      @Override
      public boolean delistResource(XAResource xaResource, int i) throws IllegalStateException, SystemException {
         return false;
      }

      @Override
      public void registerSynchronization(Synchronization synchronization) throws RollbackException, IllegalStateException, SystemException {

      }
   }

   /*
    * Send some messages
    * Crash the destination server
    * Set the max batch time such that it will attempt to send the batch while the dest server is down
    * Bring up the destination server
    * Send some more messages
    * Verify all messages are received
    */
   private void performCrashAndReconnectDestCrashBeforePrepare(final boolean persistent) throws Exception {
      JMSBridgeImpl bridge = new JMSBridgeImpl(cff0xa, cff1xa, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 1000, -1, QualityOfServiceMode.ONCE_AND_ONLY_ONCE, 10, 5000, null, null, false).setBridgeName("test-bridge");
      addActiveMQComponent(bridge);
      bridge.setTransactionManager(newTransactionManager());

      bridge.start();

      final int NUM_MESSAGES = 10;
      // Send some messages

      sendMessages(cf0, sourceQueue, 0, NUM_MESSAGES / 2, persistent, false);

      // verify none are received

      checkEmpty(targetQueue, 1);

      // Now crash the dest server

      JMSBridgeReconnectionTest.log.info("About to crash server");

      jmsServer1.stop();

      // Wait a while before starting up to simulate the dest being down for a while
      JMSBridgeReconnectionTest.log.info("Waiting 5 secs before bringing server back up");
      Thread.sleep(TIME_WAIT);
      JMSBridgeReconnectionTest.log.info("Done wait");

      // Restart the server
      jmsServer1.start();

      createQueue("targetQueue", 1);

      setUpAdministeredObjects();

      sendMessages(cf0, sourceQueue, NUM_MESSAGES / 2, NUM_MESSAGES / 2, persistent, false);

      checkMessagesReceived(cf1, targetQueue, QualityOfServiceMode.ONCE_AND_ONLY_ONCE, NUM_MESSAGES, false, false);
   }
}
