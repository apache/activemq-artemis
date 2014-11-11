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
package org.apache.activemq6.tests.integration.cluster.failover;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.activemq6.api.core.Message;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.client.impl.TopologyMemberImpl;
import org.apache.activemq6.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq6.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq6.core.server.group.impl.GroupingHandlerConfiguration;
import org.apache.activemq6.core.server.impl.SharedNothingBackupActivation;
import org.apache.activemq6.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq6.tests.util.ServiceTestBase;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 26, 2009
 */
public abstract class GroupingFailoverTestBase extends ClusterTestBase
{

   @Test
   public void testGroupingLocalHandlerFails() throws Exception
   {
      setupBackupServer(2, 0, isFileStorage(), isSharedStore(), isNetty());

      setupLiveServer(0, isFileStorage(), isSharedStore(), isNetty(), false);

      setupLiveServer(1, isFileStorage(), isSharedStore(), isNetty(), false);

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0);

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 2, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 2);
      if (!isSharedStore())
      {
         ((ReplicatedPolicyConfiguration)servers[0].getConfiguration().getHAPolicyConfiguration()).setGroupName("group1");
         ((ReplicatedPolicyConfiguration)servers[1].getConfiguration().getHAPolicyConfiguration()).setGroupName("group2");
         ((ReplicaPolicyConfiguration)servers[2].getConfiguration().getHAPolicyConfiguration()).setGroupName("group1");
      }

      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForTopology(servers[1], 2, 1);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAll(10, 0);

      if (!isSharedStore())
      {
         waitForBackupTopologyAnnouncement(sfs[0]);
      }

      Thread.sleep(1000);

      closeSessionFactory(0);

      servers[0].stop(true);

      waitForServerRestart(2);

      setupSessionFactory(2, isNetty());

      addConsumer(2, 2, "queue0", null);

      waitForBindings(2, "queues.testaddress", 1, 1, true);

      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAll(10, 2);
   }

   public void waitForBackupTopologyAnnouncement(ClientSessionFactory sf) throws Exception
   {
      long start = System.currentTimeMillis();

      ServerLocator locator = sf.getServerLocator();
      do
      {
         Collection<TopologyMemberImpl> members = locator.getTopology().getMembers();
         for (TopologyMemberImpl member : members)
         {
            if (member.getBackup() != null)
            {
               return;
            }
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < ServiceTestBase.WAIT_TIMEOUT);

      throw new IllegalStateException("Timed out waiting for backup announce");
   }

   @Test
   public void testGroupingLocalHandlerFailsMultipleGroups() throws Exception
   {
      setupBackupServer(2, 0, isFileStorage(), isSharedStore(), isNetty());

      setupLiveServer(0, isFileStorage(), isSharedStore(), isNetty(), false);

      setupLiveServer(1, isFileStorage(), isSharedStore(), isNetty(), false);

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0);

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 2, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 2);

      if (!isSharedStore())
      {
         ((ReplicatedPolicyConfiguration)servers[0].getConfiguration().getHAPolicyConfiguration()).setGroupName("group1");
         ((ReplicatedPolicyConfiguration)servers[1].getConfiguration().getHAPolicyConfiguration()).setGroupName("group2");
         ((ReplicaPolicyConfiguration)servers[2].getConfiguration().getHAPolicyConfiguration()).setGroupName("group1");
      }

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());

      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);

      waitForBindings(0, "queues.testaddress", 1, 0, true);

      createQueue(1, "queues.testaddress", "queue0", null, true);

      waitForBindings(1, "queues.testaddress", 1, 0, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForTopology(servers[1], 2);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id3"));
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id4"));
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id5"));
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id6"));

      verifyReceiveAllWithGroupIDRoundRobin(0, 30, 0, 1);

      if (!isSharedStore())
      {
         SharedNothingBackupActivation backupActivation =  (SharedNothingBackupActivation) servers[2].getActivation();
         assertTrue(backupActivation.waitForBackupSync(10, TimeUnit.SECONDS));
      }

      closeSessionFactory(0);

      servers[0].stop(true);

      waitForServerRestart(2);

      setupSessionFactory(2, isNetty());

      addConsumer(2, 2, "queue0", null);

      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(2, "queues.testaddress", 1, 1, false);

      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(1, "queues.testaddress", 1, 1, false);

      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id3"));
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id4"));
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id5"));
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id6"));

      verifyReceiveAllWithGroupIDRoundRobin(2, 30, 1, 2);
   }

   public boolean isNetty()
   {
      return true;
   }
}
