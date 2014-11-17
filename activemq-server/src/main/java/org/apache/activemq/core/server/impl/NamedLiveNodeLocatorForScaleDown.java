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

package org.apache.activemq.core.server.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.TopologyMember;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.LiveNodeLocator;

/**
 * NamedLiveNodeLocatorForScaleDown looks for a live server in the cluster with a specific scaleDownGroupName
 *
 * @author Justin Bertram
 * @see org.apache.activemq.core.config.Configuration#getScaleDownGroupName()
 */
public class NamedLiveNodeLocatorForScaleDown extends LiveNodeLocator
{
   private final Lock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   private final String scaleDownGroupName;
   private final HornetQServerImpl server;
   Map<String, Pair<TransportConfiguration, TransportConfiguration>> connectors = new HashMap<>();

   private String nodeID;
   private String myNodeID;

   public NamedLiveNodeLocatorForScaleDown(String scaleDownGroupName, HornetQServerImpl server)
   {
      super();
      this.server = server;
      this.scaleDownGroupName = scaleDownGroupName;
      this.myNodeID = server.getNodeID().toString();
   }

   @Override
   public void locateNode() throws HornetQException
   {
      locateNode(-1L);
   }

   @Override
   public void locateNode(long timeout) throws HornetQException
   {
      try
      {
         lock.lock();
         if (connectors.isEmpty())
         {
            try
            {
               if (timeout != -1L)
               {
                  if (!condition.await(timeout, TimeUnit.MILLISECONDS))
                  {
                     throw new HornetQException("Timeout elapsed while waiting for cluster node");
                  }
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
         Pair<TransportConfiguration, TransportConfiguration> connector =
            new Pair<TransportConfiguration, TransportConfiguration>(topologyMember.getLive(), topologyMember.getBackup());

         if (topologyMember.getNodeId().equals(myNodeID))
         {
            if (HornetQServerLogger.LOGGER.isTraceEnabled())
            {
               HornetQServerLogger.LOGGER.trace(this + "::informing node about itself, nodeUUID=" +
                                                   server.getNodeID() + ", connectorPair=" + topologyMember + ", this = " + this);
            }
            return;
         }

         if (scaleDownGroupName.equals(topologyMember.getScaleDownGroupName()) && topologyMember.getLive() != null &&
            server.checkLiveIsNotColocated(topologyMember.getNodeId()))
         {
            connectors.put(topologyMember.getNodeId(), connector);
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
      try
      {
         lock.lock();
         connectors.remove(nodeID);
         if (connectors.size() > 0)
         {
            condition.signal();
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   @Override
   public String getNodeID()
   {
      return nodeID;
   }

   @Override
   public Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration()
   {
      try
      {
         lock.lock();
         Iterator<String> iterator = connectors.keySet().iterator();
         //sanity check but this should never happen
         if (iterator.hasNext())
         {
            nodeID = iterator.next();
         }
         return connectors.get(nodeID);
      }
      finally
      {
         lock.unlock();
      }
   }

   @Override
   public void notifyRegistrationFailed(boolean alreadyReplicating)
   {
      try
      {
         lock.lock();
         connectors.remove(nodeID);
      }
      finally
      {
         lock.unlock();
      }
   }
}

