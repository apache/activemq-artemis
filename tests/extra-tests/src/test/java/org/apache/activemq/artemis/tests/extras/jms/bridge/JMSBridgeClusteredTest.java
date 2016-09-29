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

import javax.transaction.TransactionManager;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.jms.bridge.ConnectionFactoryFactory;
import org.apache.activemq.artemis.jms.bridge.DestinationFactory;
import org.apache.activemq.artemis.jms.bridge.QualityOfServiceMode;
import org.apache.activemq.artemis.jms.bridge.impl.JMSBridgeImpl;
import org.junit.Before;
import org.junit.Test;

/**
 * A JMSBridgeClusteredTest
 * <br>
 * Tests of jms bridge using HA connection factories.
 */
public class JMSBridgeClusteredTest extends ClusteredBridgeTestBase {

   private ServerGroup sourceServer;
   private ServerGroup targetServer;

   private String sourceQueueName = "SourceQueue";
   private String targetQueueName = "TargetQueue";

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      sourceServer = createServerGroup("source-server");
      targetServer = createServerGroup("target-server");

      sourceServer.start();
      targetServer.start();

      sourceServer.createQueue(sourceQueueName);
      targetServer.createQueue(targetQueueName);
   }

   @Test
   public void testBridgeOnFailoverXA() throws Exception {
      performSourceAndTargetCrashAndFailover(QualityOfServiceMode.ONCE_AND_ONLY_ONCE);
   }

   @Test
   public void testBridgeOnFailoverDupsOk() throws Exception {
      performSourceAndTargetCrashAndFailover(QualityOfServiceMode.DUPLICATES_OK);
   }

   @Test
   public void testBridgeOnFailoverAtMostOnce() throws Exception {
      performSourceAndTargetCrashAndFailover(QualityOfServiceMode.AT_MOST_ONCE);
   }

   @Test
   public void testCrashAndFailoverWithMessagesXA() throws Exception {
      performSourceAndTargetCrashAndFailoverWithMessages(QualityOfServiceMode.ONCE_AND_ONLY_ONCE);
   }

   //test messages are correctly bridged when failover happens during a batch send.
   //first send some messages, make sure bridge doesn't send it (below batch size)
   //then crash the live
   //then send more messages
   //then receive those messages, no more, no less.
   //this test are valid for ONCE_AND_ONLY_ONCE and AT_MOST_ONCE.
   //with DUPS_OK the test failed because some messages are delivered again
   //after failover, which is fine as in this mode duplication is allowed.
   public void performSourceAndTargetCrashAndFailoverWithMessages(QualityOfServiceMode mode) throws Exception {
      JMSBridgeImpl bridge = null;
      TransactionManager txMgr = null;

      try {
         ConnectionFactoryFactory sourceCFF = sourceServer.getConnectionFactoryFactory();
         ConnectionFactoryFactory targetCFF = targetServer.getConnectionFactoryFactory();
         DestinationFactory sourceQueueFactory = sourceServer.getDestinationFactory(sourceQueueName);
         DestinationFactory targetQueueFactory = targetServer.getDestinationFactory(targetQueueName);

         //even number
         final int batchSize = 4;
         bridge = new JMSBridgeImpl(sourceCFF, targetCFF, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 1000, -1, mode, batchSize, -1, null, null, false).setBridgeName("test-bridge");

         txMgr = newTransactionManager();
         bridge.setTransactionManager(txMgr);

         //start the bridge
         bridge.start();

         System.out.println("started bridge");

         final int NUM_MESSAGES = batchSize / 2;

         //send some messages to source
         sendMessages(sourceServer, sourceQueueName, NUM_MESSAGES);
         //receive from target, no message should be received.
         receiveMessages(targetServer, targetQueueName, 0);

         //now crash target server
         targetServer.crashLive();

         //send more
         sendMessages(sourceServer, sourceQueueName, NUM_MESSAGES);

         receiveMessages(targetServer, targetQueueName, batchSize);

         //send some again
         sendMessages(sourceServer, sourceQueueName, NUM_MESSAGES);
         //check no messages arrived.
         receiveMessages(targetServer, targetQueueName, 0);
         //now crash source server
         sourceServer.crashLive();

         //verify bridge still work
         sendMessages(sourceServer, sourceQueueName, NUM_MESSAGES);
         receiveMessages(targetServer, targetQueueName, batchSize);
      } finally {
         if (bridge != null) {
            bridge.stop();
         }
      }
   }

   /*
    * Deploy a bridge, source and target queues are in
    * separate live/backup pairs. Source and Target CF are ha.
    * Test the bridge work when the live servers crash.
    */
   private void performSourceAndTargetCrashAndFailover(QualityOfServiceMode mode) throws Exception {

      JMSBridgeImpl bridge = null;
      TransactionManager txMgr = null;

      try {
         ConnectionFactoryFactory sourceCFF = sourceServer.getConnectionFactoryFactory();
         ConnectionFactoryFactory targetCFF = targetServer.getConnectionFactoryFactory();
         DestinationFactory sourceQueueFactory = sourceServer.getDestinationFactory(sourceQueueName);
         DestinationFactory targetQueueFactory = targetServer.getDestinationFactory(targetQueueName);

         bridge = new JMSBridgeImpl(sourceCFF, targetCFF, sourceQueueFactory, targetQueueFactory, null, null, null, null, null, 1000, -1, mode, 10, 1000, null, null, false).setBridgeName("test-bridge");

         txMgr = newTransactionManager();
         bridge.setTransactionManager(txMgr);

         //start the bridge
         bridge.start();

         final int NUM_MESSAGES = 10;

         //send some messages to source
         sendMessages(sourceServer, sourceQueueName, NUM_MESSAGES);
         //receive from target
         receiveMessages(targetServer, targetQueueName, NUM_MESSAGES);

         //now crash target server
         targetServer.crashLive();

         //verify bridge still works
         sendMessages(sourceServer, sourceQueueName, NUM_MESSAGES);
         receiveMessages(targetServer, targetQueueName, NUM_MESSAGES);

         //now crash source server
         sourceServer.crashLive();

         //verify bridge still work
         sendMessages(sourceServer, sourceQueueName, NUM_MESSAGES);
         receiveMessages(targetServer, targetQueueName, NUM_MESSAGES, mode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE);
      } finally {
         if (bridge != null) {
            bridge.stop();
         }
      }
   }

   private void sendMessages(ServerGroup server, String queueName, int num) throws ActiveMQException {
      server.sendMessages(queueName, num);
   }

   private void receiveMessages(ServerGroup server,
                                String queueName,
                                int num,
                                boolean checkDup) throws ActiveMQException {
      try {
         server.receiveMessages(queueName, num, checkDup);
      } catch (ActiveMQException e) {
         e.printStackTrace();
         throw e;
      }
   }

   private void receiveMessages(ServerGroup server, String queueName, int num) throws ActiveMQException {
      try {
         server.receiveMessages(queueName, num, false);
      } catch (ActiveMQException e) {
         e.printStackTrace();
         throw e;
      }
   }

   protected TransactionManager newTransactionManager() {
      return new TransactionManagerImple();
   }

}
