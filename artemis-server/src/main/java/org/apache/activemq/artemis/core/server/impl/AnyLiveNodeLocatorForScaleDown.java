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
package org.apache.activemq.artemis.core.server.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.server.LiveNodeLocator;
import org.apache.activemq.artemis.utils.ConcurrentUtil;
import org.jboss.logging.Logger;

/**
 * This implementation looks for any available live node, once tried with no success it is marked as
 * tried and the next available is used.
 */
public class AnyLiveNodeLocatorForScaleDown extends LiveNodeLocator {

   private static final Logger logger = Logger.getLogger(AnyLiveNodeLocatorForScaleDown.class);

   private final Lock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   private final ActiveMQServerImpl server;
   Map<String, Pair<TransportConfiguration, TransportConfiguration>> connectors = new TreeMap<>();

   private String nodeID;
   private String myNodeID;

   public AnyLiveNodeLocatorForScaleDown(ActiveMQServerImpl server) {
      super();
      this.server = server;
      this.myNodeID = server.getNodeID().toString();
   }

   @Override
   public void locateNode() throws ActiveMQException {
      locateNode(-1L);
   }

   @Override
   public void locateNode(long timeout) throws ActiveMQException {
      try {
         lock.lock();
         if (connectors.isEmpty()) {
            try {
               if (timeout != -1L) {
                  if (!ConcurrentUtil.await(condition, timeout)) {
                     throw new ActiveMQException("Timeout elapsed while waiting for cluster node");
                  }
               } else {
                  while (connectors.isEmpty()) {
                     condition.await();
                  }
               }
            } catch (InterruptedException e) {

            }
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void nodeUP(TopologyMember topologyMember, boolean last) {
      try {
         lock.lock();
         Pair<TransportConfiguration, TransportConfiguration> connector = new Pair<>(topologyMember.getLive(), topologyMember.getBackup());

         if (topologyMember.getNodeId().equals(myNodeID)) {
            if (logger.isTraceEnabled()) {
               logger.trace(this + "::informing node about itself, nodeUUID=" +
                               server.getNodeID() + ", connectorPair=" + topologyMember + ", this = " + this);
            }
            return;
         }

         if (server.checkLiveIsNotColocated(topologyMember.getNodeId())) {
            connectors.put(topologyMember.getNodeId(), connector);
            condition.signal();
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void nodeDown(long eventUID, String nodeID) {
      try {
         lock.lock();
         connectors.remove(nodeID);
         if (connectors.size() > 0) {
            condition.signal();
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public String getNodeID() {
      return nodeID;
   }

   @Override
   public Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration() {
      try {
         lock.lock();
         Iterator<String> iterator = connectors.keySet().iterator();
         //sanity check but this should never happen
         if (iterator.hasNext()) {
            nodeID = iterator.next();
         }
         return connectors.get(nodeID);
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void notifyRegistrationFailed(boolean alreadyReplicating) {
      try {
         lock.lock();
         connectors.remove(nodeID);
      } finally {
         lock.unlock();
      }
   }
}
