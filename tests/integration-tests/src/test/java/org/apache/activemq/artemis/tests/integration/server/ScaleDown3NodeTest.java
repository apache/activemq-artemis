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
package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ScaleDown3NodeTest extends ClusterTestBase {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      setupLiveServer(0, isFileStorage(), false, isNetty(), true);
      servers[0].getConfiguration().setSecurityEnabled(true);
      setupLiveServer(1, isFileStorage(), false, isNetty(), true);
      servers[1].getConfiguration().setSecurityEnabled(true);
      setupLiveServer(2, isFileStorage(), false, isNetty(), true);
      servers[2].getConfiguration().setSecurityEnabled(true);
      LiveOnlyPolicyConfiguration haPolicyConfiguration0 = (LiveOnlyPolicyConfiguration) servers[0].getConfiguration().getHAPolicyConfiguration();
      ScaleDownConfiguration scaleDownConfiguration0 = new ScaleDownConfiguration();
      haPolicyConfiguration0.setScaleDownConfiguration(scaleDownConfiguration0);
      LiveOnlyPolicyConfiguration haPolicyConfiguration1 = (LiveOnlyPolicyConfiguration) servers[1].getConfiguration().getHAPolicyConfiguration();
      ScaleDownConfiguration scaleDownConfiguration1 = new ScaleDownConfiguration();
      haPolicyConfiguration1.setScaleDownConfiguration(scaleDownConfiguration1);
      scaleDownConfiguration0.setGroupName("bill");
      scaleDownConfiguration1.setGroupName("bill");
      scaleDownConfiguration1.setEnabled(false);

      setupClusterConnection("cluster0", "testAddress", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster0", "testAddress", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);
      setupClusterConnection("cluster0", "testAddress", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);
      String scaleDownConnector = servers[0].getConfiguration().getClusterConfigurations().get(0).getStaticConnectors().get(0);
      Assert.assertEquals(61617, servers[0].getConfiguration().getConnectorConfigurations().get(scaleDownConnector).getParams().get(TransportConstants.PORT_PROP_NAME));
      scaleDownConfiguration0.getConnectors().add(scaleDownConnector);
      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty(), false, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());
      setupSessionFactory(1, isNetty(), false, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());
      setupSessionFactory(2, isNetty(), false, servers[2].getConfiguration().getClusterUser(), servers[2].getConfiguration().getClusterPassword());
      IntegrationTestLogger.LOGGER.info("===============================");
      IntegrationTestLogger.LOGGER.info("Node 0: " + servers[0].getClusterManager().getNodeId());
      IntegrationTestLogger.LOGGER.info("Node 1: " + servers[1].getClusterManager().getNodeId());
      IntegrationTestLogger.LOGGER.info("Node 2: " + servers[2].getClusterManager().getNodeId());
      IntegrationTestLogger.LOGGER.info("===============================");
   }

   protected boolean isNetty() {
      return true;
   }

   @Test
   public void testBasicScaleDownWithDefaultReconnectAttempts() throws Exception {
      testBasicScaleDownInternal(ActiveMQDefaultConfiguration.getDefaultBridgeReconnectAttempts(), false);
   }

   @Test
   public void testBasicScaleDownWithoutBridgeReconnect() throws Exception {
      testBasicScaleDownInternal(0, false);
   }

   @Test
   public void testBasicScaleDownWithDefaultReconnectAttemptsAndLargeMessages() throws Exception {
      testBasicScaleDownInternal(ActiveMQDefaultConfiguration.getDefaultBridgeReconnectAttempts(), true);
   }

   private void testBasicScaleDownInternal(int reconnectAttempts, boolean large) throws Exception {
      AddressSettings addressSettings = new AddressSettings().setRedistributionDelay(0);
      servers[0].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[1].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[2].getAddressSettingsRepository().addMatch("#", addressSettings);

      servers[0].getConfiguration().getClusterConfigurations().get(0).setReconnectAttempts(reconnectAttempts);
      servers[1].getConfiguration().getClusterConfigurations().get(0).setReconnectAttempts(reconnectAttempts);
      servers[2].getConfiguration().getClusterConfigurations().get(0).setReconnectAttempts(reconnectAttempts);

      final int TEST_SIZE = 10;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";

      // create a queue on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());
      createQueue(1, addressName, queueName1, null, false, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());
      createQueue(2, addressName, queueName1, null, false, servers[2].getConfiguration().getClusterUser(), servers[2].getConfiguration().getClusterPassword());

      // pause the SnF queue so that when the server tries to redistribute a message it won't actually go across the cluster bridge
      String snfAddress = servers[0].getInternalNamingPrefix() + "sf.cluster0." + servers[0].getNodeID().toString();
      Queue snfQueue = ((LocalQueueBinding) servers[2].getPostOffice().getBinding(SimpleString.toSimpleString(snfAddress))).getQueue();
      snfQueue.pause();

      ClientSession session = sfs[2].createSession(servers[2].getConfiguration().getClusterUser(), servers[2].getConfiguration().getClusterPassword(), false, true, false, false, 0);

      Message message;

      if (large) {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) servers[2].getStorageManager());

         fileMessage.setMessageID(1005);
         fileMessage.setDurable(true);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            fileMessage.addBytes(new byte[]{ActiveMQTestBase.getSamplebyte(i)});
         }

         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         fileMessage.releaseResources();

         message = fileMessage;
      } else {
         message = session.createMessage(false);
      }

      for (int i = 0; i < TEST_SIZE; i++) {
         ClientProducer producer = session.createProducer(addressName);
         producer.send(message);
      }

      if (large) {
         ((LargeServerMessageImpl) message).deleteFile();
      }

      // add a consumer to node 0 to trigger redistribution here
      addConsumer(0, 0, queueName1, null, true, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());

      // allow some time for redistribution to move the message to the SnF queue
      long timeout = 10000;
      long start = System.currentTimeMillis();
      long messageCount = 0;

      while (System.currentTimeMillis() - start < timeout) {
         // ensure the message is not in the queue on node 2
         messageCount = getMessageCount(snfQueue);
         if (messageCount < TEST_SIZE) {
            Thread.sleep(200);
         } else {
            break;
         }
      }

      // ensure the message is in the SnF queue
      Assert.assertEquals(TEST_SIZE, getMessageCount(snfQueue));

      // trigger scaleDown from node 0 to node 1
      IntegrationTestLogger.LOGGER.info("============ Stopping " + servers[0].getNodeID());
      removeConsumer(0);
      servers[0].stop();

      start = System.currentTimeMillis();

      while (System.currentTimeMillis() - start < timeout) {
         // ensure the message is not in the queue on node 2
         messageCount = getMessageCount(((LocalQueueBinding) servers[2].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue());
         if (messageCount > 0) {
            Thread.sleep(200);
         } else {
            break;
         }
      }

      Assert.assertEquals(0, messageCount);

      // get the messages from queue 1 on node 1
      addConsumer(0, 1, queueName1, null, true, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());

      // allow some time for redistribution to move the message to node 1
      start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout) {
         // ensure the message is not in the queue on node 2
         messageCount = getMessageCount(((LocalQueueBinding) servers[1].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue());
         if (messageCount < TEST_SIZE) {
            Thread.sleep(200);
         } else {
            break;
         }
      }

      // ensure the message is in queue 1 on node 1 as expected
      Assert.assertEquals(TEST_SIZE, messageCount);

      for (int i = 0; i < TEST_SIZE; i++) {
         ClientMessage clientMessage = consumers[0].getConsumer().receive(250);
         Assert.assertNotNull(clientMessage);
         if (large) {
            Assert.assertEquals(2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, clientMessage.getBodySize());

            for (int j = 0; j < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; j++) {
               Assert.assertEquals(ActiveMQTestBase.getSamplebyte(j), clientMessage.getBodyBuffer().readByte());
            }
         }
         IntegrationTestLogger.LOGGER.info("Received: " + clientMessage);
         clientMessage.acknowledge();
      }

      // ensure there are no more messages on queue 1
      ClientMessage clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);
   }

   @Test
   public void testScaleDownWithMultipleQueues() throws Exception {
      AddressSettings addressSettings = new AddressSettings().setRedistributionDelay(0);
      servers[0].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[1].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[2].getAddressSettingsRepository().addMatch("#", addressSettings);

      final int TEST_SIZE = 10;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";
      final String queueName3 = "testQueue3";

      // create a queue on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());
      createQueue(1, addressName, queueName1, null, false, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());
      createQueue(2, addressName, queueName1, null, false, servers[2].getConfiguration().getClusterUser(), servers[2].getConfiguration().getClusterPassword());

      // create a queue on each node mapped to the same address
      createQueue(0, addressName, queueName2, null, false, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());
      createQueue(1, addressName, queueName2, null, false, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());
      createQueue(2, addressName, queueName2, null, false, servers[2].getConfiguration().getClusterUser(), servers[2].getConfiguration().getClusterPassword());

      // create a queue on each node mapped to the same address
      createQueue(0, addressName, queueName3, null, false, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());
      createQueue(1, addressName, queueName3, null, false, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());
      createQueue(2, addressName, queueName3, null, false, servers[2].getConfiguration().getClusterUser(), servers[2].getConfiguration().getClusterPassword());

      // pause the SnF queue so that when the server tries to redistribute a message it won't actually go across the cluster bridge
      String snfAddress = servers[0].getInternalNamingPrefix() + "sf.cluster0." + servers[0].getNodeID().toString();
      Queue snfQueue = ((LocalQueueBinding) servers[2].getPostOffice().getBinding(SimpleString.toSimpleString(snfAddress))).getQueue();
      snfQueue.pause();

      ClientSession session = sfs[2].createSession(servers[2].getConfiguration().getClusterUser(), servers[2].getConfiguration().getClusterPassword(), false, true, false, false, 0);

      Message message;
      message = session.createMessage(false);

      for (int i = 0; i < TEST_SIZE; i++) {
         ClientProducer producer = session.createProducer(addressName);
         producer.send(message);
      }

      // add a consumer to node 0 to trigger redistribution here
      addConsumer(0, 0, queueName1, null, true, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());
      addConsumer(1, 0, queueName3, null, true, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());

      // allow some time for redistribution to move the message to the SnF queue
      long timeout = 10000;
      long start = System.currentTimeMillis();
      long messageCount = 0;

      while (System.currentTimeMillis() - start < timeout) {
         // ensure the message is not in the queue on node 2
         messageCount = getMessageCount(snfQueue);
         if (messageCount < TEST_SIZE * 2) {
            Thread.sleep(200);
         } else {
            break;
         }
      }

      // ensure the message is in the SnF queue
      Assert.assertEquals(TEST_SIZE * 2, getMessageCount(snfQueue));

      // trigger scaleDown from node 0 to node 1
      IntegrationTestLogger.LOGGER.info("============ Stopping " + servers[0].getNodeID());
      removeConsumer(0);
      removeConsumer(1);
      servers[0].stop();

      start = System.currentTimeMillis();

      while (System.currentTimeMillis() - start < timeout) {
         // ensure the messages are not in the queues on node 2
         messageCount = getMessageCount(((LocalQueueBinding) servers[2].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue());
         messageCount += getMessageCount(((LocalQueueBinding) servers[2].getPostOffice().getBinding(new SimpleString(queueName3))).getQueue());
         if (messageCount > 0) {
            Thread.sleep(200);
         } else {
            break;
         }
      }

      Assert.assertEquals(0, messageCount);

      Assert.assertEquals(TEST_SIZE, getMessageCount(((LocalQueueBinding) servers[2].getPostOffice().getBinding(new SimpleString(queueName2))).getQueue()));

      // get the messages from queue 1 on node 1
      addConsumer(0, 1, queueName1, null, true, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());
      addConsumer(1, 1, queueName3, null, true, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());

      // allow some time for redistribution to move the message to node 1
      start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout) {
         // ensure the message is not in the queue on node 2
         messageCount = getMessageCount(((LocalQueueBinding) servers[1].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue());
         messageCount += getMessageCount(((LocalQueueBinding) servers[1].getPostOffice().getBinding(new SimpleString(queueName3))).getQueue());
         if (messageCount < TEST_SIZE * 2) {
            Thread.sleep(200);
         } else {
            break;
         }
      }

      // ensure the message is in queue 1 on node 1 as expected
      Assert.assertEquals(TEST_SIZE * 2, messageCount);

      for (int i = 0; i < TEST_SIZE; i++) {
         ClientMessage clientMessage = consumers[0].getConsumer().receive(1000);
         Assert.assertNotNull(clientMessage);
         IntegrationTestLogger.LOGGER.info("Received: " + clientMessage);
         clientMessage.acknowledge();

         clientMessage = consumers[1].getConsumer().receive(1000);
         Assert.assertNotNull(clientMessage);
         IntegrationTestLogger.LOGGER.info("Received: " + clientMessage);
         clientMessage.acknowledge();
      }

      // ensure there are no more messages on queue 1
      ClientMessage clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);

      // ensure there are no more messages on queue 3
      clientMessage = consumers[1].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(1);
   }
}
