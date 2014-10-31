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
package org.hornetq.tests.integration.server;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ScaleDown3NodeTest extends ClusterTestBase
{
   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      servers[0].getConfiguration().getHAPolicy().setScaleDownGroupName("bill");
      servers[1].getConfiguration().getHAPolicy().setScaleDownGroupName("bill");

      servers[0].getConfiguration().getHAPolicy().setScaleDown(true);
      setupClusterConnection("cluster0", "testAddress", false, 1, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster0", "testAddress", false, 1, isNetty(), 1, 0, 2);
      setupClusterConnection("cluster0", "testAddress", false, 1, isNetty(), 2, 0, 1);
      String scaleDownConnector = servers[0].getConfiguration().getClusterConfigurations().get(0).getStaticConnectors().get(0);
      Assert.assertEquals(5446, servers[0].getConfiguration().getConnectorConfigurations().get(scaleDownConnector).getParams().get(TransportConstants.PORT_PROP_NAME));
      servers[0].getConfiguration().getHAPolicy().getScaleDownConnectors().add(scaleDownConnector);
      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());
      IntegrationTestLogger.LOGGER.info("===============================");
      IntegrationTestLogger.LOGGER.info("Node 0: " + servers[0].getClusterManager().getNodeId());
      IntegrationTestLogger.LOGGER.info("Node 1: " + servers[1].getClusterManager().getNodeId());
      IntegrationTestLogger.LOGGER.info("Node 2: " + servers[2].getClusterManager().getNodeId());
      IntegrationTestLogger.LOGGER.info("===============================");
   }

   protected boolean isNetty()
   {
      return true;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      closeAllConsumers();
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();
      servers[0].getConfiguration().getHAPolicy().setScaleDown(false);
      servers[1].getConfiguration().getHAPolicy().setScaleDown(false);
      servers[2].getConfiguration().getHAPolicy().setScaleDown(false);
      stopServers(0, 1, 2);
      super.tearDown();
   }

   @Test
   public void testBasicScaleDownWithDefaultReconnectAttempts() throws Exception
   {
      testBasicScaleDownInternal(HornetQDefaultConfiguration.getDefaultBridgeReconnectAttempts(), false);
   }

   @Test
   public void testBasicScaleDownWithoutBridgeReconnect() throws Exception
   {
      testBasicScaleDownInternal(0, false);
   }

   @Test
   public void testBasicScaleDownWithDefaultReconnectAttemptsAndLargeMessages() throws Exception
   {
      testBasicScaleDownInternal(HornetQDefaultConfiguration.getDefaultBridgeReconnectAttempts(), true);
   }

   private void testBasicScaleDownInternal(int reconnectAttempts, boolean large) throws Exception
   {
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setRedistributionDelay(0);
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
      createQueue(0, addressName, queueName1, null, false);
      createQueue(1, addressName, queueName1, null, false);
      createQueue(2, addressName, queueName1, null, false);

      // pause the SnF queue so that when the server tries to redistribute a message it won't actually go across the cluster bridge
      String snfAddress = "sf.cluster0." + servers[0].getNodeID().toString();
      Queue snfQueue = ((LocalQueueBinding) servers[2].getPostOffice().getBinding(SimpleString.toSimpleString(snfAddress))).getQueue();
      snfQueue.pause();

      ClientSession session = sfs[2].createSession(false, true, false);

      Message message;

      if (large)
      {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) servers[2].getStorageManager());

         fileMessage.setMessageID(1005);
         fileMessage.setDurable(true);

         for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
         {
            fileMessage.addBytes(new byte[]{UnitTestCase.getSamplebyte(i)});
         }

         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         fileMessage.releaseResources();

         message = fileMessage;
      }
      else
      {
         message = session.createMessage(false);
      }

      for (int i = 0; i < TEST_SIZE; i++)
      {
         ClientProducer producer = session.createProducer(addressName);
         producer.send(message);
      }

      if (large)
      {
         ((LargeServerMessageImpl)message).deleteFile();
      }

      // add a consumer to node 0 to trigger redistribution here
      addConsumer(0, 0, queueName1, null);

      // allow some time for redistribution to move the message to the SnF queue
      long timeout = 10000;
      long start = System.currentTimeMillis();
      long messageCount = 0;

      while (System.currentTimeMillis() - start < timeout)
      {
         // ensure the message is not in the queue on node 2
         messageCount = snfQueue.getMessageCount();
         if (messageCount < TEST_SIZE)
         {
            Thread.sleep(200);
         }
         else
         {
            break;
         }
      }

      // ensure the message is in the SnF queue
      Assert.assertEquals(TEST_SIZE, snfQueue.getMessageCount());

      // trigger scaleDown from node 0 to node 1
      IntegrationTestLogger.LOGGER.info("============ Stopping " + servers[0].getNodeID());
      removeConsumer(0);
      servers[0].stop();

      start = System.currentTimeMillis();

      while (System.currentTimeMillis() - start < timeout)
      {
         // ensure the message is not in the queue on node 2
         messageCount = ((LocalQueueBinding) servers[2].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue().getMessageCount();
         if (messageCount > 0)
         {
            Thread.sleep(200);
         }
         else
         {
            break;
         }
      }

      Assert.assertEquals(0, messageCount);

      // get the messages from queue 1 on node 1
      addConsumer(0, 1, queueName1, null);

      // allow some time for redistribution to move the message to node 1
      start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout)
      {
         // ensure the message is not in the queue on node 2
         messageCount = ((LocalQueueBinding) servers[1].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue().getMessageCount();
         if (messageCount < TEST_SIZE)
         {
            Thread.sleep(200);
         }
         else
         {
            break;
         }
      }

      // ensure the message is in queue 1 on node 1 as expected
      Assert.assertEquals(TEST_SIZE, messageCount);

      for (int i = 0; i < TEST_SIZE; i++)
      {
         ClientMessage clientMessage = consumers[0].getConsumer().receive(250);
         Assert.assertNotNull(clientMessage);
         if (large)
         {
            Assert.assertEquals(2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, clientMessage.getBodySize());

            for (int j = 0; j < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; j++)
            {
               Assert.assertEquals(UnitTestCase.getSamplebyte(j), clientMessage.getBodyBuffer().readByte());
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
   public void testScaleDownWithMultipleQueues() throws Exception
   {
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setRedistributionDelay(0);
      servers[0].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[1].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[2].getAddressSettingsRepository().addMatch("#", addressSettings);

      final int TEST_SIZE = 10;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";
      final String queueName3 = "testQueue3";

      // create a queue on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false);
      createQueue(1, addressName, queueName1, null, false);
      createQueue(2, addressName, queueName1, null, false);

      // create a queue on each node mapped to the same address
      createQueue(0, addressName, queueName2, null, false);
      createQueue(1, addressName, queueName2, null, false);
      createQueue(2, addressName, queueName2, null, false);

      // create a queue on each node mapped to the same address
      createQueue(0, addressName, queueName3, null, false);
      createQueue(1, addressName, queueName3, null, false);
      createQueue(2, addressName, queueName3, null, false);

      // pause the SnF queue so that when the server tries to redistribute a message it won't actually go across the cluster bridge
      String snfAddress = "sf.cluster0." + servers[0].getNodeID().toString();
      Queue snfQueue = ((LocalQueueBinding) servers[2].getPostOffice().getBinding(SimpleString.toSimpleString(snfAddress))).getQueue();
      snfQueue.pause();

      ClientSession session = sfs[2].createSession(false, true, false);

      Message message;
      message = session.createMessage(false);

      for (int i = 0; i < TEST_SIZE; i++)
      {
         ClientProducer producer = session.createProducer(addressName);
         producer.send(message);
      }

      // add a consumer to node 0 to trigger redistribution here
      addConsumer(0, 0, queueName1, null);
      addConsumer(1, 0, queueName3, null);

      // allow some time for redistribution to move the message to the SnF queue
      long timeout = 10000;
      long start = System.currentTimeMillis();
      long messageCount = 0;

      while (System.currentTimeMillis() - start < timeout)
      {
         // ensure the message is not in the queue on node 2
         messageCount = snfQueue.getMessageCount();
         if (messageCount < TEST_SIZE * 2)
         {
            Thread.sleep(200);
         }
         else
         {
            break;
         }
      }

      // ensure the message is in the SnF queue
      Assert.assertEquals(TEST_SIZE * 2, snfQueue.getMessageCount());

      // trigger scaleDown from node 0 to node 1
      IntegrationTestLogger.LOGGER.info("============ Stopping " + servers[0].getNodeID());
      removeConsumer(0);
      removeConsumer(1);
      servers[0].stop();

      start = System.currentTimeMillis();

      while (System.currentTimeMillis() - start < timeout)
      {
         // ensure the messages are not in the queues on node 2
         messageCount = ((LocalQueueBinding) servers[2].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue().getMessageCount();
         messageCount += ((LocalQueueBinding) servers[2].getPostOffice().getBinding(new SimpleString(queueName3))).getQueue().getMessageCount();
         if (messageCount > 0)
         {
            Thread.sleep(200);
         }
         else
         {
            break;
         }
      }

      Assert.assertEquals(0, messageCount);

      Assert.assertEquals(TEST_SIZE, ((LocalQueueBinding) servers[2].getPostOffice().getBinding(new SimpleString(queueName2))).getQueue().getMessageCount());

      // get the messages from queue 1 on node 1
      addConsumer(0, 1, queueName1, null);
      addConsumer(1, 1, queueName3, null);

      // allow some time for redistribution to move the message to node 1
      start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout)
      {
         // ensure the message is not in the queue on node 2
         messageCount = ((LocalQueueBinding) servers[1].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue().getMessageCount();
         messageCount += ((LocalQueueBinding) servers[1].getPostOffice().getBinding(new SimpleString(queueName3))).getQueue().getMessageCount();
         if (messageCount < TEST_SIZE * 2)
         {
            Thread.sleep(200);
         }
         else
         {
            break;
         }
      }

      // ensure the message is in queue 1 on node 1 as expected
      Assert.assertEquals(TEST_SIZE * 2, messageCount);

      for (int i = 0; i < TEST_SIZE; i++)
      {
         ClientMessage clientMessage = consumers[0].getConsumer().receive(250);
         Assert.assertNotNull(clientMessage);
         IntegrationTestLogger.LOGGER.info("Received: " + clientMessage);
         clientMessage.acknowledge();

         clientMessage = consumers[1].getConsumer().receive(250);
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
