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
package org.apache.activemq.artemis.tests.integration.cluster.distribution;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.core.postoffice.impl.BindingsImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.group.UnproposalListener;
import org.apache.activemq.artemis.core.server.group.impl.GroupBinding;
import org.apache.activemq.artemis.core.server.group.impl.GroupingHandlerConfiguration;
import org.apache.activemq.artemis.core.server.group.impl.Proposal;
import org.apache.activemq.artemis.core.server.group.impl.Response;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ClusteredGroupingTest extends ClusterTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testGroupingGroupTimeoutWithUnproposal() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0, 2000, 1000, 100);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1, 2000, 1000, 100);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2, 2000, 1000, 100);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 0, false);

      final CountDownLatch latch = new CountDownLatch(4);

      getServer(1).getManagementService().addNotificationListener(notification -> {
         if (!(notification.getType() instanceof CoreNotificationType))
            return;
         if (notification.getType() == CoreNotificationType.UNPROPOSAL) {
            latch.countDown();
         }
      });
      getServer(2).getManagementService().addNotificationListener(notification -> {
         if (!(notification.getType() instanceof CoreNotificationType))
            return;
         if (notification.getType() == CoreNotificationType.UNPROPOSAL) {
            latch.countDown();
         }
      });
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAll(10, 0);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));

      verifyReceiveAll(10, 0);

      QueueImpl queue0Server2 = (QueueImpl) servers[2].locateQueue(SimpleString.of("queue0"));

      assertEquals(2, queue0Server2.getGroupCount());

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      long timeLimit = System.currentTimeMillis() + 5000;
      while (timeLimit > System.currentTimeMillis() && queue0Server2.getGroupCount() != 0) {
         Thread.sleep(10);
      }

      assertEquals(0, queue0Server2.getGroupCount(), "Unproposal should cleanup the queue group as well");

      removeConsumer(0);

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAll(10, 0);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));

      verifyReceiveAll(10, 0);
   }

   @Test
   public void testGroupingGroupTimeoutSendRemote() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0, -1, 2000, 500);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 0, false);

      final CountDownLatch latch = new CountDownLatch(4);

      getServer(1).getManagementService().addNotificationListener(notification -> {
         if (!(notification.getType() instanceof CoreNotificationType))
            return;
         if (notification.getType() == CoreNotificationType.UNPROPOSAL) {
            latch.countDown();
         }
      });
      getServer(2).getManagementService().addNotificationListener(notification -> {
         if (!(notification.getType() instanceof CoreNotificationType))
            return;
         if (notification.getType() == CoreNotificationType.UNPROPOSAL) {
            latch.countDown();
         }
      });
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAll(10, 0);

      sendWithProperty(1, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));

      verifyReceiveAll(10, 0);

      removeConsumer(0);

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAll(10, 0);

      sendWithProperty(1, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));

      verifyReceiveAll(10, 0);
   }

   @Test
   public void testGroupingSimple() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAll(10, 0);

   }

   /**
    * This is the same test as testGroupingSimple() just with the "address" removed from the cluster-connection and grouping-handler
    */
   @Test
   public void testGroupingSimpleWithNoAddress() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      servers[0].getConfiguration().setGroupingHandlerConfiguration(new GroupingHandlerConfiguration().setName(SimpleString.of("grouparbitrator")).setType(GroupingHandlerConfiguration.TYPE.LOCAL).setTimeout(5000).setGroupTimeout(-1).setReaperPeriod(ActiveMQDefaultConfiguration.getDefaultGroupingHandlerReaperPeriod()));
      servers[1].getConfiguration().setGroupingHandlerConfiguration(new GroupingHandlerConfiguration().setName(SimpleString.of("grouparbitrator")).setType(GroupingHandlerConfiguration.TYPE.REMOTE).setTimeout(5000).setGroupTimeout(-1).setReaperPeriod(ActiveMQDefaultConfiguration.getDefaultGroupingHandlerReaperPeriod()));
      servers[2].getConfiguration().setGroupingHandlerConfiguration(new GroupingHandlerConfiguration().setName(SimpleString.of("grouparbitrator")).setType(GroupingHandlerConfiguration.TYPE.REMOTE).setTimeout(5000).setGroupTimeout(-1).setReaperPeriod(ActiveMQDefaultConfiguration.getDefaultGroupingHandlerReaperPeriod()));

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAll(10, 0);

   }

   // Fail a node where there's a consumer only.. with messages being sent by a node that is not the local
   @Test
   public void testGroupingSimpleFail2nd() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      final int TIMEOUT_GROUPS = 5000;
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0, TIMEOUT_GROUPS, -1, -1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1, TIMEOUT_GROUPS, -1, -1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2, TIMEOUT_GROUPS, -1, -1);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id3"));

      // It should receive one message on each server
      ClientMessage msg = consumers[0].getConsumer().receive(1000);
      assertNotNull(msg);
      msg.acknowledge();
      assertNull(consumers[0].getConsumer().receiveImmediate());

      msg = consumers[1].getConsumer().receive(1000);
      assertNotNull(msg);
      msg.acknowledge();
      SimpleString groupIDOnConsumer1 = msg.getSimpleStringProperty(Message.HDR_GROUP_ID);

      assertNull(consumers[1].getConsumer().receiveImmediate());

      msg = consumers[2].getConsumer().receive(1000);
      assertNotNull(msg);
      msg.acknowledge();
      assertNull(consumers[2].getConsumer().receiveImmediate());

      // it should be bound to server1 as we used the group from server1
      sendWithProperty(2, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, groupIDOnConsumer1);
      msg = consumers[1].getConsumer().receive(1000);
      assertNotNull(msg);
      msg.acknowledge();

      closeAllConsumers();
      closeAllSessionFactories();

      SimpleString node1ID = servers[1].getNodeID();

      // Validating if it's the right server
      Response response = servers[0].getGroupingHandler().getProposal(groupIDOnConsumer1.concat(".").concat("queue0"), false);
      assertTrue(response.getClusterName().toString().equals("queue0" + node1ID));

      stopServers(0, 1, 2);

      long time = System.currentTimeMillis();
      startServers(2, 0);
      assertTrue(System.currentTimeMillis() >= time + TIMEOUT_GROUPS, "The group start should have waited the timeout on groups");

      setupSessionFactory(0, isNetty());
      setupSessionFactory(2, isNetty());

      addConsumer(0, 0, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, false);

      waitForBindings(2, "queues.testaddress", 1, 1, false);

      sendWithProperty(2, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, groupIDOnConsumer1);

      // server1 is dead, so either 0 or 2 should receive since the group is now dead
      msg = consumers[0].getConsumer().receive(500);
      if (msg == null) {
         msg = consumers[2].getConsumer().receive(500);
      }

      response = servers[0].getGroupingHandler().getProposal(groupIDOnConsumer1.concat(".").concat("queue0"), false);

      assertFalse(response.getClusterName().toString().equals("queue0" + node1ID), "group should have been reassigned since server is not up yet");

      assertNotNull(msg);
      msg.acknowledge();

   }

   @Test
   public void testGroupTimeout() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0, 1000, 1000, 100);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1, 1000, 100, 100);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2, 1000, 100, 100);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id3"));

      assertNotNull(servers[0].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), false));

      // Group timeout
      Thread.sleep(1000);

      long timeLimit = System.currentTimeMillis() + 5000;
      while (timeLimit > System.currentTimeMillis() && servers[0].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), false) != null) {
         Thread.sleep(10);
      }
      Thread.sleep(1000);

      assertNull(servers[0].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), false), "Group should have timed out");

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      sendWithProperty(1, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      // Verify why this is failing... whyt it's creating a new one here????
      assertNotNull(servers[0].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), false));
      assertNotNull(servers[1].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), false));

      timeLimit = System.currentTimeMillis() + 1500;

      // We will keep bothering server1 as it will ping server0 eventually
      while (timeLimit > System.currentTimeMillis() && servers[1].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), true) != null) {
         assertNotNull(servers[0].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), false));
         Thread.sleep(10);
      }

      // Group timeout
      Thread.sleep(1000);

      timeLimit = System.currentTimeMillis() + 5000;
      while (timeLimit > System.currentTimeMillis() && servers[0].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), false) != null) {
         Thread.sleep(10);
      }

      assertNull(servers[0].getGroupingHandler().getProposal(SimpleString.of("id1.queue0"), false), "Group should have timed out");
   }

   @Test
   public void testGroupingWith3Nodes() throws Exception {
      final String ADDRESS = "queues.testaddress";
      final String QUEUE = "queue0";

      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);
      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0, 10000, 500, 750);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1, 10000, 500, 750);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2, 10000, 500, 750);

      startServers(0, 1, 2);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      servers[0].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[1].getAddressSettingsRepository().addMatch("#", addressSettings);
      servers[2].getAddressSettingsRepository().addMatch("#", addressSettings);

      setupSessionFactory(0, isNetty());

      // need to set up reconnect attempts on this session factory because the test will restart node 1
      setupSessionFactory(1, isNetty(), 15);

      setupSessionFactory(2, isNetty());

      createQueue(0, ADDRESS, QUEUE, null, true);
      createQueue(1, ADDRESS, QUEUE, null, true);
      createQueue(2, ADDRESS, QUEUE, null, true);

      waitForBindings(0, ADDRESS, 1, 0, true);
      waitForBindings(1, ADDRESS, 1, 0, true);
      waitForBindings(2, ADDRESS, 1, 0, true);

      waitForBindings(0, ADDRESS, 2, 0, false);
      waitForBindings(1, ADDRESS, 2, 0, false);
      waitForBindings(2, ADDRESS, 2, 0, false);

      final ClientSessionFactory sf0 = sfs[0];
      final ClientSessionFactory sf1 = sfs[1];
      final ClientSessionFactory sf2 = sfs[2];

      final ClientSession session = addClientSession(sf1.createSession(false, false, false));
      final ClientProducer producer = addClientProducer(session.createProducer(ADDRESS));
      List<String> groups = new ArrayList<>();

      final AtomicInteger totalMessageProduced = new AtomicInteger(0);

      // create a bunch of groups and save a few group IDs for use later
      for (int i = 0; i < 500; i++) {
         ClientMessage message = session.createMessage(true);
         String group = UUID.randomUUID().toString();
         message.putStringProperty(Message.HDR_GROUP_ID, SimpleString.of(group));
         SimpleString dupID = SimpleString.of(UUID.randomUUID().toString());
         message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID);
         if (i % 100 == 0) {
            groups.add(group);
         }
         producer.send(message);
         logger.trace("Sent message to server 1 with dupID: {}", dupID);
      }

      session.commit();
      totalMessageProduced.addAndGet(500);
      logger.trace("Sent block of 500 messages to server 1. Total sent: {}", totalMessageProduced.get());
      session.close();

      // need thread pool to service both consumers and producers plus a thread to cycle nodes
      ExecutorService executorService = Executors.newFixedThreadPool(groups.size() * 2 + 1, ActiveMQThreadFactory.defaultThreadFactory(getClass().getName()));

      final AtomicInteger producerCounter = new AtomicInteger(0);
      final CountDownLatch okToConsume = new CountDownLatch(groups.size() + 1);

      final AtomicInteger errors = new AtomicInteger(0);

      final long timeToRun = System.currentTimeMillis() + 5000;

      // spin up a bunch of threads to pump messages into some of the groups
      for (final String groupx : groups) {
         final Runnable r = () -> {

            String group = groupx;

            String basicID = UUID.randomUUID().toString();
            logger.debug("Starting producer thread...");
            ClientSessionFactory factory;
            ClientSession session12 = null;
            ClientProducer producer1 = null;
            int targetServer = 0;

            try {

               int count = producerCounter.incrementAndGet();
               if (count % 3 == 0) {
                  factory = sf2;
                  targetServer = 2;
               } else if (count % 2 == 0) {
                  factory = sf1;
                  targetServer = 1;
               } else {
                  factory = sf0;
               }
               logger.debug("Creating producer session factory to node {}", targetServer);
               session12 = addClientSession(factory.createSession(false, true, true));
               producer1 = addClientProducer(session12.createProducer(ADDRESS));
            } catch (Exception e) {
               errors.incrementAndGet();
               logger.warn("Producer thread couldn't establish connection", e);
               return;
            }

            int messageCount = 0;

            while (timeToRun > System.currentTimeMillis()) {
               ClientMessage message = session12.createMessage(true);
               message.putStringProperty(Message.HDR_GROUP_ID, SimpleString.of(group));
               SimpleString dupID = SimpleString.of(basicID + ":" + messageCount);
               message.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, dupID);
               try {
                  producer1.send(message);
                  totalMessageProduced.incrementAndGet();
                  messageCount++;
               } catch (ActiveMQException e) {
                  logger.warn("Producer thread threw exception while sending messages to {}: {}", targetServer, e.getMessage());
                  // in case of a failure we change the group to make possible errors more likely
                  group = group + "afterFail";
               } catch (Exception e) {
                  logger.warn("Producer thread threw unexpected exception while sending messages to {}: {}", targetServer, e.getMessage());
                  group = group + "afterFail";
                  break;
               }
            }

            okToConsume.countDown();
         };

         executorService.execute(r);
      }

      Runnable r = () -> {
         try {
            try {
               Thread.sleep(2000);
            } catch (InterruptedException e) {
               e.printStackTrace();
               // ignore
            }
            cycleServer(1);
         } finally {
            okToConsume.countDown();
         }
      };

      executorService.execute(r);

      final AtomicInteger consumerCounter = new AtomicInteger(0);
      final AtomicInteger totalMessagesConsumed = new AtomicInteger(0);
      final CountDownLatch okToEndTest = new CountDownLatch(groups.size());

      // spin up a bunch of threads to consume messages
      for (final String group : groups) {
         r = () -> {
            try {
               logger.debug("Waiting to start consumer thread...");
               okToConsume.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               e.printStackTrace();
               return;
            }
            logger.debug("Starting consumer thread...");
            ClientSessionFactory factory;
            ClientSession session1 = null;
            ClientConsumer consumer = null;
            int targetServer = 0;

            try {
               synchronized (consumerCounter) {
                  if (consumerCounter.get() % 3 == 0) {
                     factory = sf2;
                     targetServer = 2;
                  } else if (consumerCounter.get() % 2 == 0) {
                     factory = sf1;
                     targetServer = 1;
                  } else {
                     factory = sf0;
                  }
                  logger.debug("Creating consumer session factory to node {}", targetServer);
                  session1 = addClientSession(factory.createSession(false, false, true));
                  consumer = addClientConsumer(session1.createConsumer(QUEUE));
                  session1.start();
                  consumerCounter.incrementAndGet();
               }
            } catch (Exception e) {
               logger.debug("Consumer thread couldn't establish connection", e);
               errors.incrementAndGet();
               return;
            }

            while (true) {
               try {
                  ClientMessage m = consumer.receive(1000);
                  if (m == null) {
                     okToEndTest.countDown();
                     return;
                  }
                  m.acknowledge();
                  logger.trace("Consumed message {} from server {}. Total consumed: {}", m.getStringProperty(Message.HDR_DUPLICATE_DETECTION_ID), targetServer, totalMessagesConsumed.incrementAndGet());
               } catch (ActiveMQException e) {
                  errors.incrementAndGet();
                  logger.warn("Consumer thread threw exception while receiving messages from server {}.: {}", targetServer, e.getMessage());
               } catch (Exception e) {
                  errors.incrementAndGet();
                  logger.warn("Consumer thread threw unexpected exception while receiving messages from server {}.: {}", targetServer, e.getMessage());
                  return;
               }
            }
         };

         executorService.execute(r);
      }
      // wait for the threads to complete their consuming
      okToEndTest.await(20, TimeUnit.SECONDS);

      executorService.shutdownNow();
      executorService.awaitTermination(10, TimeUnit.SECONDS);

      assertEquals(0, errors.get());

      assertEquals(totalMessageProduced.longValue(), totalMessagesConsumed.longValue());
   }

   private void cycleServer(int node) {
      try {
         stopServers(node);
         startServers(node);
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Test
   public void testGroupingBindingNotPresentAtStart() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      int TIMEOUT = 50000;
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0, TIMEOUT);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id3"));

      verifyReceiveAll(1, 0, 1, 2);

      closeAllConsumers();
      closeSessionFactory(0);
      closeSessionFactory(1);
      closeSessionFactory(2);

      stopServers(0, 1, 2);

      long time = System.currentTimeMillis();
      startServers(1, 2, 0);
      assertTrue(System.currentTimeMillis() - time < TIMEOUT, "Server restart took a long wait even though it wasn't required as the server already had all the bindings");

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id3"));

      verifyReceiveAll(1, 0, 1, 2);
   }

   @Test
   public void testGroupingBindingsRemoved() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id3"));

      closeAllConsumers();
      closeSessionFactory(0);
      closeSessionFactory(1);
      closeSessionFactory(2);

      stopServers(0);

      stopServers(1);

      startServers(0);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(2, isNetty());

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 1, 0, false);
      waitForBindings(2, "queues.testaddress", 1, 1, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id3"));

      //check for 2 messages on 0
      verifyReceiveAll(1, 0);
      verifyReceiveAll(1, 0);

      //get the pinned message from 2
      addConsumer(1, 2, "queue0", null);

      verifyReceiveAll(1, 1);
   }

   @Test
   public void testTimeoutOnSending() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1, 0);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2, 0);

      final CountDownLatch latch = new CountDownLatch(BindingsImpl.MAX_GROUP_RETRY);

      setUpGroupHandler(new GroupingHandler() {

         @Override
         public void awaitBindings() throws Exception {

         }

         @Override
         public void addListener(UnproposalListener listener) {

         }

         @Override
         public void resendPending() throws Exception {

         }

         @Override
         public void remove(SimpleString groupid, SimpleString clusterName) throws Exception {

         }

         @Override
         public void forceRemove(SimpleString groupid, SimpleString clusterName) throws Exception {

         }

         @Override
         public SimpleString getName() {
            return null;
         }

         @Override
         public void remove(SimpleString id, SimpleString groupId, int distance) {
            //To change body of implemented methods use File | Settings | File Templates.
         }

         @Override
         public void start() throws Exception {
            //To change body of implemented methods use File | Settings | File Templates.
         }

         @Override
         public void stop() throws Exception {
            //To change body of implemented methods use File | Settings | File Templates.
         }

         @Override
         public boolean isStarted() {
            return false;
         }

         @Override
         public Response propose(final Proposal proposal) throws Exception {
            return null;
         }

         @Override
         public void proposed(final Response response) throws Exception {
         }

         @Override
         public void sendProposalResponse(final Response response, final int distance) throws Exception {
         }

         @Override
         public Response receive(final Proposal proposal, final int distance) throws Exception {
            latch.countDown();
            return null;
         }

         @Override
         public void onNotification(final Notification notification) {
         }

         @Override
         public void addGroupBinding(final GroupBinding groupBinding) {
         }

         @Override
         public Response getProposal(final SimpleString fullID, boolean touchTime) {
            return null;
         }

      }, 0);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      try {
         sendWithProperty(1, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

         // it should get the Retries on the latch
         assertTrue(latch.await(10, TimeUnit.SECONDS));
      } catch (Exception e) {
         e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
      }

   }

   @Test
   public void testGroupingSendTo2queues() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      sendInRange(1, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(10, 20, 0);

   }

   @Test
   public void testGroupingSendTo3queues() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      sendInRange(1, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(10, 20, 0);
      sendInRange(2, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(10, 20, 0);

   }

   @Test
   public void testGroupingSendTo3queuesRemoteArbitrator() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(0, 10, 1);
      sendInRange(2, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(10, 20, 1);
      sendInRange(0, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(20, 30, 1);
   }

   @Test
   public void testGroupingSendTo3queuesNoConsumerOnLocalQueue() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      // addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 1, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      int consumer = 0;
      if (consumers[0].consumer.receive(5000) != null) {
         consumer = 0;
      } else if (consumers[2].consumer.receive(5000) != null) {
         consumer = 2;
      } else {
         fail("Message was not received.");
      }

      sendInRange(1, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(0, 10, consumer);
      sendInRange(2, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(10, 20, consumer);
      sendInRange(0, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(20, 30, consumer);

   }

   @Test
   public void testGroupingRoundRobin() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      sendInRange(0, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, SimpleString.of("id2"));
      sendInRange(0, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, SimpleString.of("id3"));
      verifyReceiveAllWithGroupIDRoundRobin(0, 10, 0, 1, 2);

   }

   @Test
   public void testGroupingSendTo3queuesQueueRemoved() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      sendInRange(1, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(10, 20, 0);
      sendInRange(2, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(20, 30, 0);
      removeConsumer(0);
      removeConsumer(1);
      removeConsumer(2);
      deleteQueue(0, "queue0");
      deleteQueue(1, "queue0");
      deleteQueue(2, "queue0");
      createQueue(0, "queues.testaddress", "queue1", null, false);
      addConsumer(3, 0, "queue1", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      waitForBindings(2, "queues.testaddress", 1, 1, false);

      sendInRange(0, "queues.testaddress", 30, 40, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      verifyReceiveAllInRange(30, 40, 3);
      sendInRange(1, "queues.testaddress", 40, 50, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      verifyReceiveAllInRange(40, 50, 3);
      sendInRange(2, "queues.testaddress", 50, 60, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      verifyReceiveAllInRange(50, 60, 3);
   }

   @Test
   public void testGroupingSendTo3queuesPinnedNodeGoesDown() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, true, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(true, 0, 10, 0);

      closeAllConsumers();

      stopServers(1);

      startServers(1);

      closeSessionFactory(1);

      setupSessionFactory(1, isNetty());

      addConsumer(0, 1, "queue0", null);

      waitForBindings(2, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 2, 1, false);
      sendInRange(2, "queues.testaddress", 10, 20, true, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      verifyReceiveAllInRange(10, 20, 0);
   }

   @Test
   public void testGroupingSendTo3queuesPinnedNodeGoesDownSendBeforeStop() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, true, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(true, 0, 10, 0);

      closeAllConsumers();

      sendInRange(2, "queues.testaddress", 10, 20, true, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      stopServers(1);

      closeSessionFactory(1);

      startServers(1);

      setupSessionFactory(1, isNetty());

      addConsumer(1, 1, "queue0", null);

      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      verifyReceiveAllInRange(10, 20, 1);

   }

   @Test
   public void testGroupingSendTo3queuesPinnedNodeGoesDownSendAfterRestart() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, 0, 500, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      stopServers(1);

      closeSessionFactory(1);
      startServers(1);
      setupSessionFactory(1, isNetty());
      addConsumer(1, 1, "queue0", null);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);
      sendInRange(2, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAllInRange(10, 20, 1);

      sendInRange(0, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));
      verifyReceiveAllInRange(20, 30, 1);

   }

   @Test
   public void testGroupingMultipleQueuesOnAddress() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      createQueue(0, "queues.testaddress", "queue1", null, false);
      createQueue(1, "queues.testaddress", "queue1", null, false);
      createQueue(2, "queues.testaddress", "queue1", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      addConsumer(3, 0, "queue0", null);
      addConsumer(4, 1, "queue0", null);
      addConsumer(5, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 2, 2, true);
      waitForBindings(1, "queues.testaddress", 2, 2, true);
      waitForBindings(2, "queues.testaddress", 2, 2, true);

      waitForBindings(0, "queues.testaddress", 4, 4, false);
      waitForBindings(1, "queues.testaddress", 4, 4, false);
      waitForBindings(2, "queues.testaddress", 4, 4, false);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, SimpleString.of("id1"));

      verifyReceiveAll(10, 0);

   }

   @Test
   public void testGroupingMultipleSending() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      CountDownLatch latch = new CountDownLatch(1);
      Thread[] threads = new Thread[9];
      int range = 0;
      for (int i = 0; i < 9; i++, range += 10) {
         threads[i] = new Thread(new ThreadSender(range, range + 10, 1, SimpleString.of("id" + i), latch, i < 8));
      }
      for (Thread thread : threads) {
         thread.start();
      }

      verifyReceiveAllWithGroupIDRoundRobin(0, 30, 0, 1, 2);
   }

   public boolean isNetty() {
      return true;
   }

   class ThreadSender implements Runnable {

      private final int msgStart;

      private final int msgEnd;

      private final SimpleString id;

      private final CountDownLatch latch;

      private final boolean wait;

      private final int node;

      ThreadSender(final int msgStart,
                   final int msgEnd,
                   final int node,
                   final SimpleString id,
                   final CountDownLatch latch,
                   final boolean wait) {
         this.msgStart = msgStart;
         this.msgEnd = msgEnd;
         this.node = node;
         this.id = id;
         this.latch = latch;
         this.wait = wait;
      }

      @Override
      public void run() {
         if (wait) {
            try {
               latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
            }
         } else {
            latch.countDown();
         }
         try {
            sendInRange(node, "queues.testaddress", msgStart, msgEnd, false, Message.HDR_GROUP_ID, id);
         } catch (Exception e) {
            e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
         }
      }
   }
}
