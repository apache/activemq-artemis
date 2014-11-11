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

package org.apache.activemq6.core.server.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.Pair;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.TopologyMember;
import org.apache.activemq6.core.server.HornetQServerLogger;
import org.apache.activemq6.core.server.LiveNodeLocator;

/**
 * This implementation looks for any available live node, once tried with no success it is marked as
 * tried and the next available is used.
 *
 * @author Justin Bertram
 */
public class AnyLiveNodeLocatorForScaleDown extends LiveNodeLocator
{
   private final Lock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   private final HornetQServerImpl server;
   Map<String, Pair<TransportConfiguration, TransportConfiguration>> connectors = new TreeMap<>();

   private String nodeID;
   private String myNodeID;

   public AnyLiveNodeLocatorForScaleDown(HornetQServerImpl server)
   {
      super();
      this.server = server;
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
         Pair<TransportConfiguration, TransportConfiguration> connector = new Pair<>(topologyMember.getLive(), topologyMember.getBackup());

         if (topologyMember.getNodeId().equals(myNodeID))
         {
            if (HornetQServerLogger.LOGGER.isTraceEnabled())
            {
               HornetQServerLogger.LOGGER.trace(this + "::informing node about itself, nodeUUID=" +
                                                   server.getNodeID() + ", connectorPair=" + topologyMember + ", this = " + this);
            }
            return;
         }

         if (server.checkLiveIsNotColocated(topologyMember.getNodeId()))
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

