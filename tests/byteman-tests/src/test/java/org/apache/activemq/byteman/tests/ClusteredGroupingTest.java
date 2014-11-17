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
package org.apache.activemq6.byteman.tests;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq6.api.core.HornetQNonExistentQueueException;
import org.apache.activemq6.api.core.Message;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.management.ManagementHelper;
import org.apache.activemq6.api.core.management.CoreNotificationType;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.group.impl.GroupingHandlerConfiguration;
import org.apache.activemq6.core.server.group.impl.Response;
import org.apache.activemq6.core.server.management.Notification;
import org.apache.activemq6.tests.integration.cluster.distribution.ClusterTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@RunWith(BMUnitRunner.class)
public class ClusteredGroupingTest extends ClusterTestBase
{
   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "blow-up",
                     targetClass = "org.apache.activemq6.core.server.group.impl.LocalGroupingHandler",
                     targetMethod = "removeGrouping",
                     targetLocation = "ENTRY",
                     action = "org.apache.activemq6.byteman.tests.ClusteredGroupingTest.pause($1);"
                  ),
               @BMRule
                  (
                     name = "blow-up2",
                     targetClass = "org.apache.activemq6.core.server.group.impl.GroupHandlingAbstract",
                     targetMethod = "forceRemove",
                     targetLocation = "ENTRY",
                     action = "org.apache.activemq6.byteman.tests.ClusteredGroupingTest.restart2();"
                  )
            }
      )
   public void test2serversLocalGoesDown() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, 0, 500, isNetty(), 0, 1);

      setupClusterConnection("cluster1", "queues", false, 1,  0, 500, isNetty(), 1, 0);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 0, false);

      sendWithProperty(0, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));

      latch = new CountDownLatch(1);
      latch2 = new CountDownLatch(1);

      crashAndWaitForFailure(getServer(1));

      assertTrue(latch2.await(20000, TimeUnit.MILLISECONDS));

      try
      {
         try
         {
            sendWithProperty(0, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));
         }
         catch (HornetQNonExistentQueueException e)
         {
            fail("did not handle removal of queue");
         }
      }
      finally
      {
         latch.countDown();
      }
   }

   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "blow-up",
                     targetClass = "org.apache.activemq6.core.server.group.impl.RemoteGroupingHandler",
                     targetMethod = "onNotification",
                     targetLocation = "ENTRY",
                     action = "org.apache.activemq6.byteman.tests.ClusteredGroupingTest.pause2($1);"
                  ),
               @BMRule(name = "blow-up2",
                       targetClass = "org.apache.activemq6.core.server.group.impl.RemoteGroupingHandler",
                       targetMethod = "remove",
                       targetLocation = "ENTRY",
                       action = "org.apache.activemq6.byteman.tests.ClusteredGroupingTest.restart2();")
            }
      )
   public void test3serversLocalGoesDown() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1,  0, 500, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1,  0, 500, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1,  0, 500, isNetty(), 2, 0, 1);

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

      addConsumer(0, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 0, false);

      sendWithProperty(1, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));

      latch = new CountDownLatch(1);
      latch2 = new CountDownLatch(1);

      main = Thread.currentThread();
      crashAndWaitForFailure(getServer(2));

      assertTrue(latch2.await(20000, TimeUnit.MILLISECONDS));

      try
      {
         try
         {
            sendWithProperty(1, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));
         }
         catch (HornetQNonExistentQueueException e)
         {
            fail("did not handle removal of queue");
         }
      }
      finally
      {
         latch.countDown();
      }

      assertHandlersAreSame(getServer(0), getServer(1));
   }

   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "blow-up",
                     targetClass = "org.apache.activemq6.core.server.group.impl.LocalGroupingHandler",
                     targetMethod = "onNotification",
                     targetLocation = "ENTRY",
                     action = "org.apache.activemq6.byteman.tests.ClusteredGroupingTest.pause2($1);"
                  ),
               @BMRule(name = "blow-up2",
                       targetClass = "org.apache.activemq6.core.server.group.impl.LocalGroupingHandler",
                       targetMethod = "remove",
                       targetLocation = "ENTRY",
                       action = "org.apache.activemq6.byteman.tests.ClusteredGroupingTest.restart2();")
            }
      )
   public void testLocal3serversLocalGoesDown() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1,  0, 500, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1,  0, 500, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1,  0, 500, isNetty(), 2, 0, 1);

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

      addConsumer(0, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 0, false);

      sendWithProperty(0, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));

      latch = new CountDownLatch(1);
      latch2 = new CountDownLatch(1);

      main = Thread.currentThread();
      crashAndWaitForFailure(getServer(2));

      assertTrue(latch2.await(20000, TimeUnit.MILLISECONDS));

      try
      {
         try
         {
            sendWithProperty(0, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));
         }
         catch (HornetQNonExistentQueueException e)
         {
            fail("did not handle removal of queue");
         }
      }
      finally
      {
         latch.countDown();
      }

      assertHandlersAreSame(getServer(0), getServer(1));
   }

   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "blow-up",
                     targetClass = "org.apache.activemq6.core.server.group.impl.LocalGroupingHandler",
                     targetMethod = "onNotification",
                     targetLocation = "ENTRY",
                     action = "org.apache.activemq6.byteman.tests.ClusteredGroupingTest.pause2($1);"
                  ),
               @BMRule(name = "blow-up2",
                       targetClass = "org.apache.activemq6.core.server.group.impl.LocalGroupingHandler",
                       targetMethod = "remove",
                       targetLocation = "ENTRY",
                       action = "org.apache.activemq6.byteman.tests.ClusteredGroupingTest.restart2();")
            }
      )
   public void testLocal4serversLocalGoesDown() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());
      setupServer(3, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1,  0, 500, isNetty(), 0, 1, 2, 3);

      setupClusterConnection("cluster1", "queues", false, 1,  0, 500, isNetty(), 1, 0, 2, 3);

      setupClusterConnection("cluster2", "queues", false, 1,  0, 500, isNetty(), 2, 0, 1, 3);

      setupClusterConnection("cluster3", "queues", false, 1,  0, 500, isNetty(), 3, 1, 2, 3);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 3);

      startServers(0, 1, 2, 3);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());
      setupSessionFactory(3, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);
      createQueue(3, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);
      waitForBindings(3, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 3, 1, false);
      waitForBindings(1, "queues.testaddress", 3, 1, false);
      waitForBindings(2, "queues.testaddress", 3, 0, false);
      waitForBindings(3, "queues.testaddress", 3, 1, false);

      sendWithProperty(0, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));

      latch = new CountDownLatch(1);
      latch2 = new CountDownLatch(1);

      main = Thread.currentThread();
      crashAndWaitForFailure(getServer(2));

      assertTrue(latch2.await(20000, TimeUnit.MILLISECONDS));

      try
      {
         try
         {
            sendWithProperty(0, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));
         }
         catch (HornetQNonExistentQueueException e)
         {
            fail("did not handle removal of queue");
         }
      }
      finally
      {
         latch.countDown();
      }
      //now restart server
      getServer(2).start();
      waitForBindings(2, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 3, 0, false);
      sendWithProperty(3, "queues.testaddress", 1, true, Message.HDR_GROUP_ID, new SimpleString("id1"));
      Thread.sleep(2000);
      assertHandlersAreSame(getServer(0), getServer(1), getServer(2), getServer(3));
   }

   private void assertHandlersAreSame(HornetQServer server, HornetQServer... qServers)
   {
      SimpleString id = server.getGroupingHandler().getProposal(new SimpleString("id1.queue0"), false).getClusterName();
      for (HornetQServer qServer : qServers)
      {
         Response proposal = qServer.getGroupingHandler().getProposal(new SimpleString("id1.queue0"), false);
         if (proposal != null)
         {
            assertEquals(qServer.getIdentity() + " is incorrect", id, proposal.getChosenClusterName());
         }
      }
   }

   static CountDownLatch latch;
   static CountDownLatch latch2;
   static Thread main;

   public static void pause(SimpleString clusterName)
   {
      if (clusterName.toString().startsWith("queue0"))
      {
         try
         {
            latch2.countDown();
            latch.await();
         }
         catch (InterruptedException e)
         {
            e.printStackTrace();
         }
      }
   }

   public static void pause2(Notification notification)
   {
      if (notification.getType() == CoreNotificationType.BINDING_REMOVED)
      {
         SimpleString clusterName = notification.getProperties()
            .getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);
         boolean inMain = main == Thread.currentThread();
         if (clusterName.toString().startsWith("queue0") && !inMain)
         {
            try
            {
               latch2.countDown();
               latch.await();
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }
         }
      }
   }

   public static void restart2()
   {
      latch.countDown();
   }


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      closeAllConsumers();
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();
      super.tearDown();
   }

   public boolean isNetty()
   {
      return true;
   }
}
