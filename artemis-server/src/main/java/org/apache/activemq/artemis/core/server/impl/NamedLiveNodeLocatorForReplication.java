/**
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
package org.apache.activemq.artemis.core.server.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.server.LiveNodeLocator;
import org.apache.activemq.artemis.core.server.cluster.qourum.SharedNothingBackupQuorum;

/**
 * NamedLiveNodeLocatorForReplication looks for a live server in the cluster with a specific backupGroupName
 *
 * @see org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy#getGroupName()
 */
public class NamedLiveNodeLocatorForReplication extends LiveNodeLocator
{
   private final Lock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   private final String backupGroupName;
   private Pair<TransportConfiguration, TransportConfiguration> liveConfiguration;

   private String nodeID;

   public NamedLiveNodeLocatorForReplication(String backupGroupName, SharedNothingBackupQuorum quorumManager)
   {
      super(quorumManager);
      this.backupGroupName = backupGroupName;
   }

   @Override
   public void locateNode() throws ActiveMQException
   {
      locateNode(-1L);
   }

   @Override
   public void locateNode(long timeout) throws ActiveMQException
   {
      try
      {
         lock.lock();
         if (liveConfiguration == null)
         {
            try
            {
               if (timeout != -1L)
               {
                  condition.await(timeout, TimeUnit.MILLISECONDS);
               }
               else
               {
                  condition.await();
               }
            }
            catch (InterruptedException e)
            {
               //ignore
            }
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   @Override
   public void nodeUP(TopologyMember topologyMember, boolean last)
   {
      try
      {
         lock.lock();
         if (backupGroupName.equals(topologyMember.getBackupGroupName()) && topologyMember.getLive() != null)
         {
            liveConfiguration =
               new Pair<TransportConfiguration, TransportConfiguration>(topologyMember.getLive(),
                                                                        topologyMember.getBackup());
            nodeID = topologyMember.getNodeId();
            condition.signal();
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   @Override
   public void nodeDown(long eventUID, String nodeID)
   {
      //no op
   }

   @Override
   public String getNodeID()
   {
      return nodeID;
   }

   @Override
   public Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration()
   {
      return liveConfiguration;
   }

   @Override
   public void notifyRegistrationFailed(boolean alreadyReplicating)
   {
      try
      {
         lock.lock();
         liveConfiguration = null;
         super.notifyRegistrationFailed(alreadyReplicating);
      }
      finally
      {
         lock.unlock();
      }
   }
}

