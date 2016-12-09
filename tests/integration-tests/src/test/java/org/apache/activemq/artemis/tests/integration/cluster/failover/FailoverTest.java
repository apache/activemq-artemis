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

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionOutcomeUnknownException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionRolledBackException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.cluster.ha.BackupPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicaPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.ReplicatedPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreMasterPolicy;
import org.apache.activemq.artemis.core.server.cluster.ha.SharedStoreSlavePolicy;
import org.apache.activemq.artemis.core.server.files.FileMoveManager;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.CountDownSessionFailureListener;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FailoverTest extends FailoverTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected static final int NUM_MESSAGES = 100;

   protected ServerLocator locator;
   protected ClientSessionFactoryInternal sf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      locator = getServerLocator();
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks, ackBatchSize));
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks));
   }

   protected ClientSession createSession(ClientSessionFactory sf1) throws Exception {
      return addClientSession(sf1.createSession());
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   // https://issues.jboss.org/browse/HORNETQ-685
   @Test
   public void testTimeoutOnFailover() throws Exception {
      locator.setCallTimeout(1000).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setAckBatchSize(0).setReconnectAttempts(-1);

      ((InVMNodeManager) nodeManager).failoverPause = 500;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal) createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final CountDownLatch latch = new CountDownLatch(10);
      final CountDownLatch latchFailed = new CountDownLatch(1);

      Runnable r = new Runnable() {
         @Override
         public void run() {
            for (int i = 0; i < 500; i++) {
               ClientMessage message = session.createMessage(true);
               message.putIntProperty("counter", i);
               try {
                  System.out.println("Sent " + i);
                  producer.send(message);
                  if (i < 10) {
                     latch.countDown();
                     if (latch.getCount() == 0) {
                        latchFailed.await(10, TimeUnit.SECONDS);
                     }
                  }
               } catch (Exception e) {
                  // this is our retry
                  try {
                     if (!producer.isClosed())
                        producer.send(message);
                  } catch (ActiveMQException e1) {
                     e1.printStackTrace();
                  }
               }
            }
         }
      };
      Thread t = new Thread(r);
      t.start();
      Assert.assertTrue("latch released", latch.await(10, TimeUnit.SECONDS));
      crash(session);
      latchFailed.countDown();
      t.join(30000);
      if (t.isAlive()) {
         t.interrupt();
         Assert.fail("Thread still alive");
      }
      Assert.assertTrue(backupServer.getServer().waitForActivation(5, TimeUnit.SECONDS));
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();
      for (int i = 0; i < 500; i++) {
         ClientMessage m = consumer.receive(1000);
         Assert.assertNotNull("message #=" + i, m);
         // assertEquals(i, m.getIntProperty("counter").intValue());
      }
   }

   // https://issues.jboss.org/browse/HORNETQ-685
   @Test
   public void testTimeoutOnFailoverConsume() throws Exception {
      locator.setCallTimeout(5000).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setAckBatchSize(0).setBlockOnAcknowledge(true).setReconnectAttempts(-1).setAckBatchSize(0);

      ((InVMNodeManager) nodeManager).failoverPause = 5000L;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal) createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < 500; i++) {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("counter", i);
         producer.send(message);
      }

      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch endLatch = new CountDownLatch(1);

      final ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();

      final Map<Integer, ClientMessage> received = new HashMap<>();

      consumer.setMessageHandler(new MessageHandler() {

         @Override
         public void onMessage(ClientMessage message) {
            Integer counter = message.getIntProperty("counter");
            received.put(counter, message);
            try {
               log.debug("acking message = id = " + message.getMessageID() + ", counter = " +
                            message.getIntProperty("counter"));
               message.acknowledge();
            } catch (ActiveMQException e) {
               e.printStackTrace();
               return;
            }
            log.debug("Acked counter = " + counter);
            if (counter.equals(10)) {
               latch.countDown();
            }
            if (received.size() == 500) {
               endLatch.countDown();
            }
         }

      });
      latch.await(10, TimeUnit.SECONDS);
      log.info("crashing session");
      crash(session);
      endLatch.await(60, TimeUnit.SECONDS);
      Assert.assertTrue("received only " + received.size(), received.size() == 500);

      session.close();
   }

   @Test
   public void testTimeoutOnFailoverConsumeBlocked() throws Exception {
      locator.setCallTimeout(5000).setBlockOnNonDurableSend(true).setConsumerWindowSize(0).setBlockOnDurableSend(true).setAckBatchSize(0).setBlockOnAcknowledge(true).setReconnectAttempts(-1).setAckBatchSize(0);

      ((InVMNodeManager) nodeManager).failoverPause = 5000L;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal) createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < 500; i++) {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("counter", i);
         message.putBooleanProperty("end", i == 499);
         producer.send(message);
      }

      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch endLatch = new CountDownLatch(1);

      final ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();

      final Map<Integer, ClientMessage> received = new HashMap<>();

      Thread t = new Thread() {
         @Override
         public void run() {
            ClientMessage message = null;
            try {
               while ((message = getMessage()) != null) {
                  Integer counter = message.getIntProperty("counter");
                  received.put(counter, message);
                  try {
                     log.info("acking message = id = " + message.getMessageID() +
                                 ", counter = " +
                                 message.getIntProperty("counter"));
                     message.acknowledge();
                  } catch (ActiveMQException e) {
                     e.printStackTrace();
                     continue;
                  }
                  log.info("Acked counter = " + counter);
                  if (counter.equals(10)) {
                     latch.countDown();
                  }
                  if (received.size() == 500) {
                     endLatch.countDown();
                  }

                  if (message.getBooleanProperty("end")) {
                     break;
                  }
               }
            } catch (Exception e) {
               Assert.fail("failing due to exception " + e);
            }

         }

         private ClientMessage getMessage() {
            while (true) {
               try {
                  ClientMessage msg = consumer.receive(20000);
                  if (msg == null) {
                     log.info("Returning null message on consuming");
                  }
                  return msg;
               } catch (ActiveMQObjectClosedException oce) {
                  throw new RuntimeException(oce);
               } catch (ActiveMQException ignored) {
                  // retry
                  ignored.printStackTrace();
               }
            }
         }
      };
      t.start();
      latch.await(10, TimeUnit.SECONDS);
      log.info("crashing session");
      crash(session);
      endLatch.await(60, TimeUnit.SECONDS);
      t.join();
      Assert.assertTrue("received only " + received.size(), received.size() == 500);

      session.close();
   }

   // https://issues.jboss.org/browse/HORNETQ-685
   @Test
   public void testTimeoutOnFailoverTransactionCommit() throws Exception {
      locator.setCallTimeout(5000).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setAckBatchSize(0).setReconnectAttempts(-1);

      ((InVMNodeManager) nodeManager).failoverPause = 5000L;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal) createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < 500; i++) {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("counter", i);

         producer.send(message);

      }
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      crash(false, session);

      try {
         session.commit(xid, false);
      } catch (XAException e) {
         //there is still an edge condition that we must deal with
         session.commit(xid, false);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();
      for (int i = 0; i < 500; i++) {
         ClientMessage m = consumer.receive(1000);
         Assert.assertNotNull(m);
         Assert.assertEquals(i, m.getIntProperty("counter").intValue());
      }
   }

   // https://issues.jboss.org/browse/HORNETQ-685
   @Test
   public void testTimeoutOnFailoverTransactionRollback() throws Exception {
      locator.setCallTimeout(2000).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setAckBatchSize(0).setReconnectAttempts(-1);

      ((InVMNodeManager) nodeManager).failoverPause = 5000L;

      ClientSessionFactoryInternal sf1 = (ClientSessionFactoryInternal) createSessionFactory(locator);

      final ClientSession session = createSession(sf1, true, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < 500; i++) {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("counter", i);

         producer.send(message);
      }

      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      crash(false, session);

      try {
         session.rollback(xid);
      } catch (XAException e) {
         try {
            //there is still an edge condition that we must deal with
            session.rollback(xid);
         } catch (Exception ignored) {
            log.trace(ignored.getMessage(), ignored);
         }
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNull(m);

   }

   /**
    * see http://jira.jboss.org/browse/HORNETQ-522
    *
    * @throws Exception
    */
   @Test
   public void testNonTransactedWithZeroConsumerWindowSize() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setAckBatchSize(0).setReconnectAttempts(-1);

      createClientSessionFactory();

      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message = session.createMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);

      consumer.setMessageHandler(new MessageHandler() {

         @Override
         public void onMessage(ClientMessage message) {
            latch.countDown();
         }

      });

      session.start();

      crash(session);

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

   }

   protected void createClientSessionFactory() throws Exception {
      sf = (ClientSessionFactoryInternal) createSessionFactory(locator);
   }

   @Test
   public void testNonTransacted() throws Exception {
      createSessionFactory();

      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      receiveDurableMessages(consumer);

      session.close();

      sf.close();

      Assert.assertEquals(0, sf.numSessions());

      Assert.assertEquals(0, sf.numConnections());
   }

   /**
    * Basic fail-back test.
    *
    * @throws Exception
    */
   @Test
   public void testFailBack() throws Exception {
      boolean doFailBack = true;
      HAPolicy haPolicy = backupServer.getServer().getHAPolicy();
      if (haPolicy instanceof ReplicaPolicy) {
         ((ReplicaPolicy) haPolicy).setMaxSavedReplicatedJournalsSize(1);
      }

      simpleFailover(haPolicy instanceof ReplicaPolicy, doFailBack);
      tearDown();
      setUp();
   }

   @Test
   public void testFailBackLiveRestartsBackupIsGone() throws Exception {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();
      SimpleString liveId = liveServer.getServer().getNodeID();
      crash(session);

      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer);
      assertNoMoreMessages(consumer);
      consumer.close();
      session.commit();

      Assert.assertEquals("backup must be running with the same nodeID", liveId, backupServer.getServer().getNodeID());
      sf.close();

      backupServer.crash();
      Thread.sleep(100);
      Assert.assertFalse("backup is not running", backupServer.isStarted());

      Assert.assertFalse("must NOT be a backup", liveServer.getServer().getHAPolicy() instanceof BackupPolicy);
      adaptLiveConfigForReplicatedFailBack(liveServer);
      beforeRestart(liveServer);
      liveServer.start();
      Assert.assertTrue("live initialized...", liveServer.getServer().waitForActivation(15, TimeUnit.SECONDS));

      sf = (ClientSessionFactoryInternal) createSessionFactory(locator);

      ClientSession session2 = createSession(sf, false, false);
      session2.start();
      ClientConsumer consumer2 = session2.createConsumer(FailoverTestBase.ADDRESS);
      boolean replication = liveServer.getServer().getHAPolicy() instanceof ReplicatedPolicy;
      if (replication)
         receiveMessages(consumer2, 0, NUM_MESSAGES, true);
      assertNoMoreMessages(consumer2);
      session2.commit();
   }

   @Test
   public void testSimpleFailover() throws Exception {
      HAPolicy haPolicy = backupServer.getServer().getHAPolicy();

      simpleFailover(haPolicy instanceof ReplicaPolicy, false);
   }

   @Test
   public void testWithoutUsingTheBackup() throws Exception {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();

      backupServer.stop(); // Backup stops!
      backupServer.start();

      waitForRemoteBackupSynchronization(backupServer.getServer());

      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer);
      assertNoMoreMessages(consumer);
      consumer.close();
      session.commit();

      session.start();
      producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));
      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();
      backupServer.stop(); // Backup stops!
      beforeRestart(backupServer);
      backupServer.start();
      waitForRemoteBackupSynchronization(backupServer.getServer());
      backupServer.stop(); // Backup stops!

      liveServer.stop();
      beforeRestart(liveServer);
      liveServer.start();
      liveServer.getServer().waitForActivation(10, TimeUnit.SECONDS);

      ClientSession session2 = createSession(sf, false, false);
      session2.start();
      ClientConsumer consumer2 = session2.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessages(consumer2, 0, NUM_MESSAGES, true);
      assertNoMoreMessages(consumer2);
      session2.commit();
   }

   /**
    * @param doFailBack
    * @throws Exception
    */
   private void simpleFailover(boolean isReplicated, boolean doFailBack) throws Exception {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();
      SimpleString liveId = liveServer.getServer().getNodeID();
      crash(session);

      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer);
      assertNoMoreMessages(consumer);
      consumer.close();

      producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));
      sendMessages(session, producer, NUM_MESSAGES);
      producer.close();
      session.commit();

      Assert.assertEquals("backup must be running with the same nodeID", liveId, backupServer.getServer().getNodeID());
      if (doFailBack) {
         Assert.assertFalse("must NOT be a backup", liveServer.getServer().getHAPolicy().isBackup());
         adaptLiveConfigForReplicatedFailBack(liveServer);
         beforeRestart(liveServer);
         liveServer.start();
         Assert.assertTrue("live initialized...", liveServer.getServer().waitForActivation(40, TimeUnit.SECONDS));
         int i = 0;
         while (!backupServer.isStarted() && i++ < 100) {
            Thread.sleep(100);
         }
         liveServer.getServer().waitForActivation(5, TimeUnit.SECONDS);
         Assert.assertTrue(backupServer.isStarted());

         if (isReplicated) {
            FileMoveManager moveManager = new FileMoveManager(backupServer.getServer().getConfiguration().getJournalLocation(), 0);
            Assert.assertEquals(1, moveManager.getNumberOfFolders());
         }
      } else {
         backupServer.stop();
         beforeRestart(backupServer);
         backupServer.start();
         Assert.assertTrue(backupServer.getServer().waitForActivation(10, TimeUnit.SECONDS));
      }

      ClientSession session2 = createSession(sf, false, false);
      session2.start();
      ClientConsumer consumer2 = session2.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessages(consumer2, 0, NUM_MESSAGES, true);
      assertNoMoreMessages(consumer2);
      session2.commit();
   }

   /**
    * @param consumer
    * @throws ActiveMQException
    */
   private void assertNoMoreMessages(ClientConsumer consumer) throws ActiveMQException {
      ClientMessage msg = consumer.receiveImmediate();
      Assert.assertNull("there should be no more messages to receive! " + msg, msg);
   }

   protected void createSessionFactory() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);
   }

   @Test
   public void testConsumeTransacted() throws Exception {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final int numMessages = 10;

      sendMessages(session, producer, numMessages);

      session.commit();

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer.receive(1000);
         Assert.assertNotNull("Just crashed? " + (i == 6) + " " + i, message);

         message.acknowledge();

         // TODO: The test won't pass if you uncomment this line
         // assertEquals(i, (int)message.getIntProperty("counter"));

         if (i == 5) {
            crash(session);
         }
      }

      try {
         session.commit();
         Assert.fail("session must have rolled back on failover");
      } catch (ActiveMQTransactionRolledBackException trbe) {
         //ok
      } catch (ActiveMQException e) {
         Assert.fail("Invalid Exception type:" + e.getType());
      }

      consumer.close();

      consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull("Expecting message #" + i, message);

         message.acknowledge();
      }

      session.commit();

      session.close();
   }

   /**
    * @return
    * @throws Exception
    */
   protected ClientSession createSessionAndQueue() throws Exception {
      ClientSession session = createSession(sf, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);
      return session;
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-285
   @Test
   public void testFailoverOnInitialConnection() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setFailoverOnInitialConnection(true).setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      // Crash live server
      crash();

      ClientSession session = createSession(sf);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessages(session, producer, NUM_MESSAGES);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      receiveMessages(consumer);

      session.close();
   }

   @Test
   public void testTransactedMessagesSentSoRollback() throws Exception {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      Assert.assertTrue(session.isRollbackOnly());

      try {
         session.commit();

         Assert.fail("Should throw exception");
      } catch (ActiveMQTransactionRolledBackException trbe) {
         //ok
      } catch (ActiveMQException e) {
         Assert.fail("Invalid Exception type:" + e.getType());
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull("message should be null! Was: " + message, message);

      session.close();
   }

   /**
    * Test that once the transacted session has throw a TRANSACTION_ROLLED_BACK exception,
    * it can be reused again
    */
   @Test
   public void testTransactedMessagesSentSoRollbackAndContinueWork() throws Exception {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      Assert.assertTrue(session.isRollbackOnly());

      try {
         session.commit();

         Assert.fail("Should throw exception");
      } catch (ActiveMQTransactionRolledBackException trbe) {
         //ok
      } catch (ActiveMQException e) {
         Assert.fail("Invalid Exception type:" + e.getType());
      }

      ClientMessage message = session.createMessage(false);
      int counter = RandomUtil.randomInt();
      message.putIntProperty("counter", counter);

      producer.send(message);

      // session is working again
      session.commit();

      session.start();

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();
      message = consumer.receive(1000);

      Assert.assertNotNull("expecting a message", message);
      Assert.assertEquals(counter, message.getIntProperty("counter").intValue());

      session.close();
   }

   @Test
   public void testTransactedMessagesNotSentSoNoRollback() throws Exception {
      try {
         createSessionFactory();

         ClientSession session = createSessionAndQueue();

         ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

         sendMessagesSomeDurable(session, producer);

         session.commit();

         crash(session);

         // committing again should work since didn't send anything since last commit

         Assert.assertFalse(session.isRollbackOnly());

         session.commit();

         ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

         session.start();

         receiveDurableMessages(consumer);

         Assert.assertNull(consumer.receiveImmediate());

         session.commit();

         session.close();
      } finally {
         try {
            liveServer.getServer().stop();
         } catch (Throwable ignored) {
         }
         try {
            backupServer.getServer().stop();
         } catch (Throwable ignored) {
         }
      }
   }

   @Test
   public void testTransactedMessagesWithConsumerStartedBeforeFailover() throws Exception {
      createSessionFactory();

      ClientSession session = createSessionAndQueue();

      // create a consumer and start the session before failover
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      // messages will be delivered to the consumer when the session is committed
      session.commit();

      Assert.assertFalse(session.isRollbackOnly());

      crash(session);

      session.commit();

      session.close();

      session = createSession(sf, false, false);

      consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      receiveDurableMessages(consumer);

      Assert.assertNull(consumer.receiveImmediate());

      session.commit();
   }

   @Test
   public void testTransactedMessagesConsumedSoRollback() throws Exception {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session1, producer);

      session1.commit();

      ClientSession session2 = createSession(sf, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      receiveMessages(consumer);

      crash(session2);

      Assert.assertTrue(session2.isRollbackOnly());

      try {
         session2.commit();

         Assert.fail("Should throw exception");
      } catch (ActiveMQTransactionRolledBackException trbe) {
         //ok
      } catch (ActiveMQException e) {
         Assert.fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testTransactedMessagesNotConsumedSoNoRollback() throws Exception {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessages(session1, producer, NUM_MESSAGES);
      session1.commit();

      ClientSession session2 = createSession(sf, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      receiveMessages(consumer, 0, NUM_MESSAGES / 2, true);

      session2.commit();

      consumer.close();

      crash(session2);

      Assert.assertFalse(session2.isRollbackOnly());

      consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; i++) {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull("expecting message " + i, message);

         assertMessageBody(i, message);

         Assert.assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session2.commit();

      Assert.assertNull(consumer.receiveImmediate());
   }

   @Test
   public void testXAMessagesSentSoRollbackOnEnd() throws Exception {
      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      session.start(xid, XAResource.TMNOFLAGS);

      sendMessagesSomeDurable(session, producer);

      crash(session);

      try {
         session.end(xid, XAResource.TMSUCCESS);

         Assert.fail("Should throw exception");
      } catch (XAException e) {
         Assert.assertEquals(XAException.XAER_RMFAIL, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);
   }

   @Test
   //start a tx but sending messages after crash
   public void testXAMessagesSentSoRollbackOnEnd2() throws Exception {
      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      session.start(xid, XAResource.TMNOFLAGS);

      crash(session);

      // sendMessagesSomeDurable(session, producer);

      producer.send(createMessage(session, 1, true));

      try {
         session.end(xid, XAResource.TMSUCCESS);

         Assert.fail("Should throw exception");
      } catch (XAException e) {
         //         Assert.assertEquals(XAException.XAER_NOTA, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);
   }

   @Test
   public void testXAMessagesSentSoRollbackOnPrepare() throws Exception {
      createSessionFactory();

      final ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      session.start(xid, XAResource.TMNOFLAGS);

      sendMessagesSomeDurable(session, producer);

      session.end(xid, XAResource.TMSUCCESS);

      crash(session);

      try {
         session.prepare(xid);

         Assert.fail("Should throw exception");
      } catch (XAException e) {
         Assert.assertEquals(XAException.XAER_RMFAIL, e.errorCode);
         // XXXX  session.rollback();
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);

      producer.close();
      consumer.close();
   }

   // This might happen if 1PC optimisation kicks in
   @Test
   public void testXAMessagesSentSoRollbackOnCommit() throws Exception {
      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      session.start(xid, XAResource.TMNOFLAGS);

      sendMessagesSomeDurable(session, producer);

      session.end(xid, XAResource.TMSUCCESS);

      crash(session);

      try {
         session.commit(xid, false);

         Assert.fail("Should throw exception");
      } catch (XAException e) {
         Assert.assertEquals(XAException.XAER_NOTA, e.errorCode);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);
   }

   @Test
   public void testXAMessagesNotSentSoNoRollbackOnCommit() throws Exception {
      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      session.start(xid, XAResource.TMNOFLAGS);

      sendMessagesSomeDurable(session, producer);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.commit(xid, false);

      crash(session);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      Xid xid2 = new XidImpl("tfytftyf".getBytes(), 54654, "iohiuohiuhgiu".getBytes());

      session.start(xid2, XAResource.TMNOFLAGS);

      receiveDurableMessages(consumer);

      session.end(xid2, XAResource.TMSUCCESS);

      session.prepare(xid2);

      session.commit(xid2, false);
   }

   @Test
   public void testXAMessagesConsumedSoRollbackOnEnd() throws Exception {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session1, producer);

      session1.commit();

      ClientSession session2 = createSession(sf, true, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      receiveMessages(consumer);

      crash(session2);

      try {
         session2.end(xid, XAResource.TMSUCCESS);

         Assert.fail("Should throw exception");
      } catch (XAException e) {
         Assert.assertEquals(XAException.XAER_RMFAIL, e.errorCode);
      }
   }

   @Test
   public void testXAMessagesConsumedSoRollbackOnEnd2() throws Exception {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         // some are durable, some are not!
         producer.send(createMessage(session1, i, true));
      }

      session1.commit();

      ClientSession session2 = createSession(sf, true, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      crash(session2);

      receiveMessages(consumer);

      try {
         session2.end(xid, XAResource.TMSUCCESS);

         Assert.fail("Should throw exception");
      } catch (XAException e) {
      }

      // Since the end was not accepted, the messages should be redelivered
      receiveMessages(consumer);
   }

   @Test
   public void testXAMessagesConsumedSoRollbackOnPrepare() throws Exception {
      createSessionFactory();

      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session1, producer);

      session1.commit();

      ClientSession session2 = createSession(sf, true, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      receiveMessages(consumer);

      session2.end(xid, XAResource.TMSUCCESS);

      crash(session2);

      try {
         session2.prepare(xid);

         Assert.fail("Should throw exception");
      } catch (XAException e) {
         Assert.assertEquals(XAException.XAER_RMFAIL, e.errorCode);
      }
   }

   // 1PC optimisation
   @Test
   public void testXAMessagesConsumedSoRollbackOnCommit() throws Exception {
      createSessionFactory();
      ClientSession session1 = createSessionAndQueue();

      ClientProducer producer = session1.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session1, producer);

      session1.commit();

      ClientSession session2 = createSession(sf, true, false, false);

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      Xid xid = new XidImpl("uhuhuhu".getBytes(), 126512, "auhsduashd".getBytes());

      session2.start(xid, XAResource.TMNOFLAGS);

      receiveMessages(consumer);

      session2.end(xid, XAResource.TMSUCCESS);

      // session2.prepare(xid);

      crash(session2);

      try {
         session2.commit(xid, false);

         Assert.fail("Should throw exception");
      } catch (XAException e) {
         // it should be rolled back
         Assert.assertEquals(XAException.XAER_NOTA, e.errorCode);
      }

      session1.close();

      session2.close();
   }

   @Test
   public void testCreateNewFactoryAfterFailover() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setFailoverOnInitialConnection(true);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = sendAndConsume(sf, true);

      crash(true, session);

      session.close();

      long timeout;
      timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis()) {
         try {
            createClientSessionFactory();
            break;
         } catch (Exception e) {
            // retrying
            Thread.sleep(100);
         }
      }

      session = sendAndConsume(sf, true);
   }

   @Test
   public void testFailoverMultipleSessionsWithConsumers() throws Exception {
      createSessionFactory();

      final int numSessions = 5;

      final int numConsumersPerSession = 5;

      Map<ClientSession, List<ClientConsumer>> sessionConsumerMap = new HashMap<>();

      for (int i = 0; i < numSessions; i++) {
         ClientSession session = createSession(sf, true, true);

         List<ClientConsumer> consumers = new ArrayList<>();

         for (int j = 0; j < numConsumersPerSession; j++) {
            SimpleString queueName = new SimpleString("queue" + i + "-" + j);

            session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, queueName, null, true);

            ClientConsumer consumer = session.createConsumer(queueName);

            consumers.add(consumer);
         }

         sessionConsumerMap.put(session, consumers);
      }

      ClientSession sendSession = createSession(sf, true, true);

      ClientProducer producer = sendSession.createProducer(FailoverTestBase.ADDRESS);

      sendMessages(sendSession, producer, NUM_MESSAGES);

      Set<ClientSession> sessionSet = sessionConsumerMap.keySet();
      ClientSession[] sessions = new ClientSession[sessionSet.size()];
      sessionSet.toArray(sessions);
      crash(sessions);

      for (ClientSession session : sessionConsumerMap.keySet()) {
         session.start();
      }

      for (List<ClientConsumer> consumerList : sessionConsumerMap.values()) {
         for (ClientConsumer consumer : consumerList) {
            receiveMessages(consumer);
         }
      }
   }

   /*
    * Browser will get reset to beginning after failover
    */
   @Test
   public void testFailWithBrowser() throws Exception {
      createSessionFactory();
      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS, true);

      session.start();

      receiveMessages(consumer, 0, NUM_MESSAGES, false);

      crash(session);

      receiveDurableMessages(consumer);
   }

   protected void sendMessagesSomeDurable(ClientSession session, ClientProducer producer) throws Exception {
      for (int i = 0; i < NUM_MESSAGES; i++) {
         // some are durable, some are not!
         producer.send(createMessage(session, i, isDurable(i)));
      }
   }

   @Test
   public void testFailThenReceiveMoreMessagesAfterFailover() throws Exception {
      createSessionFactory();

      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      // Receive MSGs but don't ack!
      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull(message);

         assertMessageBody(i, message);

         Assert.assertEquals(i, message.getIntProperty("counter").intValue());
      }

      crash(session);

      // Should get the same ones after failover since we didn't ack

      receiveDurableMessages(consumer);
   }

   protected void receiveDurableMessages(ClientConsumer consumer) throws ActiveMQException {
      // During failover non-persistent messages may disappear but in certain cases they may survive.
      // For that reason the test is validating all the messages but being permissive with non-persistent messages
      // The test will just ack any non-persistent message, however when arriving it must be in order
      ClientMessage repeatMessage = null;
      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message;

         if (repeatMessage != null) {
            message = repeatMessage;
            repeatMessage = null;
         } else {
            message = consumer.receive(1000);
         }

         if (message != null) {
            int msgInternalCounter = message.getIntProperty("counter").intValue();

            if (msgInternalCounter == i + 1) {
               // The test can only jump to the next message if the current iteration is meant for non-durable
               Assert.assertFalse("a message on counter=" + i + " was expected", isDurable(i));
               // message belongs to the next iteration.. let's just ignore it
               repeatMessage = message;
               continue;
            }
         }

         if (isDurable(i)) {
            Assert.assertNotNull(message);
         }

         if (message != null) {
            assertMessageBody(i, message);
            Assert.assertEquals(i, message.getIntProperty("counter").intValue());
            message.acknowledge();
         }
      }
   }

   private boolean isDurable(int i) {
      return i % 2 == 0;
   }

   @Test
   public void testFailThenReceiveMoreMessagesAfterFailover2() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = createSession(sf, true, true, 0);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessagesSomeDurable(session, producer);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();
      receiveMessages(consumer);

      crash(session);

      // Send some more

      for (int i = NUM_MESSAGES; i < NUM_MESSAGES * 2; i++) {
         producer.send(createMessage(session, i, isDurable(i)));
      }
      receiveMessages(consumer, NUM_MESSAGES, NUM_MESSAGES * 2, true);
   }

   protected void receiveMessages(ClientConsumer consumer) throws ActiveMQException {
      receiveMessages(consumer, 0, NUM_MESSAGES, true);
   }

   @Test
   public void testSimpleSendAfterFailoverDurableTemporary() throws Exception {
      doSimpleSendAfterFailover(true, true);
   }

   @Test
   public void testSimpleSendAfterFailoverNonDurableTemporary() throws Exception {
      doSimpleSendAfterFailover(false, true);
   }

   @Test
   public void testSimpleSendAfterFailoverDurableNonTemporary() throws Exception {
      doSimpleSendAfterFailover(true, false);
   }

   @Test
   public void testSimpleSendAfterFailoverNonDurableNonTemporary() throws Exception {
      doSimpleSendAfterFailover(false, false);
   }

   private void doSimpleSendAfterFailover(final boolean durable, final boolean temporary) throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = createSession(sf, true, true, 0);

      if (temporary) {
         session.createTemporaryQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null);
      } else {
         session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, durable);
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      crash(session);

      sendMessagesSomeDurable(session, producer);

      receiveMessages(consumer);
   }

   @Test
   public void testForceBlockingReturn() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(-1);

      createClientSessionFactory();

      // Add an interceptor to delay the send method so we can get time to cause failover before it returns
      liveServer.getServer().getRemotingService().addIncomingInterceptor(new DelayInterceptor());

      final ClientSession session = createSession(sf, true, true, 0);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      class Sender extends Thread {

         @Override
         public void run() {
            ClientMessage message = session.createMessage(true);

            message.getBodyBuffer().writeString("message");

            try {
               producer.send(message);
            } catch (ActiveMQException e1) {
               this.e = e1;
            }
         }

         volatile ActiveMQException e;
      }

      Sender sender = new Sender();

      sender.start();

      crash(session);

      sender.join();

      Assert.assertNotNull(sender.e);

      Assert.assertNotNull(sender.e.getCause());

      Assert.assertEquals(sender.e.getType(), ActiveMQExceptionType.UNBLOCKED);

      Assert.assertEquals(((ActiveMQException) sender.e.getCause()).getType(), ActiveMQExceptionType.DISCONNECTED);

      session.close();
   }

   @Test
   public void testCommitOccurredUnblockedAndResendNoDuplicates() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setReconnectAttempts(-1).setBlockOnAcknowledge(true);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      final ClientSession session = createSession(sf, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      String txID = "my-tx-id";

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message = session.createMessage(true);

         if (i == 0) {
            // Only need to add it on one message per tx
            message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString(txID));
         }

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      class Committer extends Thread {

         DelayInterceptor2 interceptor = new DelayInterceptor2();

         @Override
         public void run() {
            try {
               sf.getServerLocator().addIncomingInterceptor(interceptor);

               session.commit();
            } catch (ActiveMQTransactionRolledBackException trbe) {
               // Ok - now we retry the commit after removing the interceptor

               sf.getServerLocator().removeIncomingInterceptor(interceptor);

               try {
                  session.commit();

                  failed = false;
               } catch (ActiveMQException e2) {
                  throw new RuntimeException(e2);
               }
            } catch (ActiveMQTransactionOutcomeUnknownException toue) {
               // Ok - now we retry the commit after removing the interceptor

               sf.getServerLocator().removeIncomingInterceptor(interceptor);

               try {
                  session.commit();

                  failed = false;
               } catch (ActiveMQException e2) {
                  throw new RuntimeException(e2);
               }
            } catch (ActiveMQException e) {
               //ignore
            }
         }

         volatile boolean failed = true;
      }

      Committer committer = new Committer();

      // Commit will occur, but response will never get back, connection is failed, and commit
      // should be unblocked with transaction rolled back

      committer.start();

      // Wait for the commit to occur and the response to be discarded
      Assert.assertTrue(committer.interceptor.await());

      crash(session);

      committer.join();

      Assert.assertFalse("second attempt succeed?", committer.failed);

      session.close();

      ClientSession session2 = createSession(sf, false, false);

      producer = session2.createProducer(FailoverTestBase.ADDRESS);

      // We now try and resend the messages since we get a transaction rolled back exception
      // but the commit actually succeeded, duplicate detection should kick in and prevent dups

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message = session2.createMessage(true);

         if (i == 0) {
            // Only need to add it on one message per tx
            message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString(txID));
         }

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      try {
         session2.commit();
         Assert.fail("expecting DUPLICATE_ID_REJECTED exception");
      } catch (ActiveMQDuplicateIdException dide) {
         //ok
      } catch (ActiveMQException e) {
         Assert.fail("Invalid Exception type:" + e.getType());
      }

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      receiveMessages(consumer);

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull(message);
   }

   @Test
   public void testCommitDidNotOccurUnblockedAndResend() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      final ClientSession session = createSession(sf, false, false);

      session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);
      sendMessages(session, producer, NUM_MESSAGES);

      class Committer extends Thread {

         @Override
         public void run() {
            Interceptor interceptor = new DelayInterceptor3();

            try {
               liveServer.addInterceptor(interceptor);

               session.commit();
            } catch (ActiveMQTransactionRolledBackException trbe) {
               // Ok - now we retry the commit after removing the interceptor

               liveServer.removeInterceptor(interceptor);

               try {
                  session.commit();

                  failed = false;
               } catch (ActiveMQException e2) {
               }
            } catch (ActiveMQTransactionOutcomeUnknownException toue) {
               // Ok - now we retry the commit after removing the interceptor

               liveServer.removeInterceptor(interceptor);

               try {
                  session.commit();

                  failed = false;
               } catch (ActiveMQException e2) {
               }
            } catch (ActiveMQException e) {
               //ignore
            }
         }

         volatile boolean failed = true;
      }

      Committer committer = new Committer();

      committer.start();

      crash(session);

      committer.join();

      Assert.assertFalse("commiter failed should be false", committer.failed);

      session.close();

      ClientSession session2 = createSession(sf, false, false);

      producer = session2.createProducer(FailoverTestBase.ADDRESS);

      // We now try and resend the messages since we get a transaction rolled back exception
      sendMessages(session2, producer, NUM_MESSAGES);

      session2.commit();

      ClientConsumer consumer = session2.createConsumer(FailoverTestBase.ADDRESS);

      session2.start();

      receiveMessages(consumer);

      ClientMessage message = consumer.receiveImmediate();

      Assert.assertNull("expecting null message", message);
   }

   @Test
   public void testBackupServerNotRemoved() throws Exception {
      // HORNETQ-720 Disabling test for replicating backups.
      if (!(backupServer.getServer().getHAPolicy() instanceof SharedStoreSlavePolicy)) {
         return;
      }
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();

      ClientSession session = sendAndConsume(sf, true);
      CountDownSessionFailureListener listener = new CountDownSessionFailureListener(session);

      session.addFailureListener(listener);

      backupServer.stop();

      liveServer.crash();

      // To reload security or other settings that are read during startup
      beforeRestart(backupServer);

      backupServer.start();

      Assert.assertTrue("session failure listener", listener.getLatch().await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);
   }

   @Test
   public void testLiveAndBackupLiveComesBack() throws Exception {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();
      final CountDownLatch latch = new CountDownLatch(1);

      ClientSession session = sendAndConsume(sf, true);

      session.addFailureListener(new CountDownSessionFailureListener(latch, session));

      backupServer.stop();

      liveServer.crash();

      beforeRestart(liveServer);

      // To reload security or other settings that are read during startup
      beforeRestart(liveServer);

      liveServer.start();

      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);
   }

   @Test
   public void testLiveAndBackupLiveComesBackNewFactory() throws Exception {
      locator.setFailoverOnInitialConnection(true);
      createSessionFactory();

      final CountDownLatch latch = new CountDownLatch(1);

      ClientSession session = sendAndConsume(sf, true);

      session.addFailureListener(new CountDownSessionFailureListener(latch, session));

      backupServer.stop();

      liveServer.crash();

      // To reload security or other settings that are read during startup
      beforeRestart(liveServer);

      liveServer.start();

      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      session.close();

      sf.close();

      createClientSessionFactory();

      session = createSession(sf);

      ClientConsumer cc = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage cm = cc.receive(5000);

      Assert.assertNotNull(cm);

      Assert.assertEquals("message0", cm.getBodyBuffer().readString());
   }

   @Test
   public void testLiveAndBackupBackupComesBackNewFactory() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setFailoverOnInitialConnection(true).setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = sendAndConsume(sf, true);

      CountDownSessionFailureListener listener = new CountDownSessionFailureListener(session);

      session.addFailureListener(listener);

      backupServer.stop();

      liveServer.crash();

      // To reload security or other settings that are read during startup
      beforeRestart(backupServer);

      if (!backupServer.getServer().getHAPolicy().isSharedStore()) {
         // XXX
         // this test would not make sense in the remote replication use case, without the following
         backupServer.getServer().setHAPolicy(new SharedStoreMasterPolicy());
      }

      backupServer.start();

      Assert.assertTrue("session failure listener", listener.getLatch().await(5, TimeUnit.SECONDS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      session.close();

      sf.close();

      createClientSessionFactory();

      session = createSession(sf);

      ClientConsumer cc = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      ClientMessage cm = cc.receive(5000);

      Assert.assertNotNull(cm);

      Assert.assertEquals("message0", cm.getBodyBuffer().readString());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   protected void beforeRestart(TestableServer liveServer1) {
      // no-op
   }

   protected ClientSession sendAndConsume(final ClientSessionFactory sf1, final boolean createQueue) throws Exception {
      ClientSession session = createSession(sf1, false, true, true);

      if (createQueue) {
         session.createQueue(FailoverTestBase.ADDRESS, RoutingType.MULTICAST, FailoverTestBase.ADDRESS, null, false);
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals("aardvarks", message2.getBodyBuffer().readString());

         Assert.assertEquals(i, message2.getObjectProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receiveImmediate();

      Assert.assertNull(message3);

      return session;
   }

   // Inner classes -------------------------------------------------

}
