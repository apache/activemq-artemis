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
package org.apache.activemq.artemis.core.client.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connector;
import org.jboss.logging.Logger;

public final class Topology {

   private static final Logger logger = Logger.getLogger(Topology.class);

   private final Set<ClusterTopologyListener> topologyListeners;

   private Executor executor;

   /**
    * Used to debug operations.
    * <p>
    * Someone may argue this is not needed. But it's impossible to debug anything related to
    * topology without knowing what node or what object missed a Topology update. Hence I added some
    * information to locate debugging here.
    */
   private volatile Object owner;

   private final TopologyManager manager;

   /**
    * topology describes the other cluster nodes that this server knows about:
    *
    * keys are node IDs
    * values are a pair of live/backup transport configurations
    */
   private final Map<String, TopologyMemberImpl> topology;

   private Map<String, Long> mapDelete;

   private static final class DirectExecutor implements Executor {

      @Override
      public void execute(final Runnable runnable) {
         runnable.run();
      }
   }

   public Topology(final Object owner) {
      this(owner, new DirectExecutor());
   }

   public Topology(final Object owner, final Executor executor) {
      this.topologyListeners = new HashSet<>();
      this.topology = new ConcurrentHashMap<>();
      if (executor == null) {
         throw new IllegalArgumentException("Executor is required");
      }
      this.executor = executor;
      this.owner = owner;
      if (owner instanceof TopologyManager) {
         manager = (TopologyManager)owner;
      } else {
         manager = null;
      }
      if (logger.isTraceEnabled()) {
         logger.trace("Topology@" + Integer.toHexString(System.identityHashCode(this)) + " CREATE", new Exception("trace"));
      }
   }

   public Topology setExecutor(Executor executor) {
      this.executor = executor;
      return this;
   }

   /**
    * It will remove all elements as if it haven't received anyone from the server.
    */
   public synchronized void clear() {
      topology.clear();
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener) {
      if (logger.isTraceEnabled()) {
         logger.trace(this + "::Adding topology listener " + listener, new Exception("Trace"));
      }
      synchronized (topologyListeners) {
         topologyListeners.add(listener);
      }
      this.sendTopology(listener);
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener) {
      if (logger.isTraceEnabled()) {
         logger.trace(this + "::Removing topology listener " + listener, new Exception("Trace"));
      }
      synchronized (topologyListeners) {
         topologyListeners.remove(listener);
      }
   }

   /**
    * This is called by the server when the node is activated from backup state. It will always succeed
    */
   public void updateAsLive(final String nodeId, final TopologyMemberImpl memberInput) {
      synchronized (this) {
         if (logger.isDebugEnabled()) {
            logger.debug(this + "::node " + nodeId + "=" + memberInput);
         }
         memberInput.setUniqueEventID(System.currentTimeMillis());
         topology.remove(nodeId);
         topology.put(nodeId, memberInput);
         sendMemberUp(nodeId, memberInput);
      }
   }

   /**
    * After the node is started, it will resend the notifyLive a couple of times to avoid gossip between two servers
    *
    * @param nodeId
    */
   public void resendNode(final String nodeId) {
      synchronized (this) {
         TopologyMemberImpl memberInput = topology.get(nodeId);
         if (memberInput != null) {
            memberInput.setUniqueEventID(System.currentTimeMillis());
            sendMemberUp(nodeId, memberInput);
         }
      }
   }

   /**
    * This is called by the server when the node is activated from backup state. It will always succeed
    */
   public TopologyMemberImpl updateBackup(final TopologyMemberImpl memberInput) {
      final String nodeId = memberInput.getNodeId();
      if (logger.isTraceEnabled()) {
         logger.trace(this + "::updateBackup::" + nodeId + ", memberInput=" + memberInput);
      }

      synchronized (this) {
         TopologyMemberImpl currentMember = getMember(nodeId);
         if (currentMember == null) {
            if (logger.isTraceEnabled()) {
               logger.trace("There's no live to be updated on backup update, node=" + nodeId + " memberInput=" + memberInput, new Exception("trace"));
            }

            currentMember = memberInput;
            topology.put(nodeId, currentMember);
         }

         TopologyMemberImpl newMember = new TopologyMemberImpl(nodeId, currentMember.getBackupGroupName(), currentMember.getScaleDownGroupName(), currentMember.getLive(), memberInput.getBackup());
         newMember.setUniqueEventID(System.currentTimeMillis());
         topology.remove(nodeId);
         topology.put(nodeId, newMember);
         sendMemberUp(nodeId, newMember);

         return newMember;
      }
   }

   /**
    * @param uniqueEventID an unique identifier for when the change was made. We will use current
    *                      time millis for starts, and a ++ of that number for shutdown.
    * @param nodeId
    * @param memberInput
    * @return {@code true} if an update did take place. Note that backups are *always* updated.
    */
   public boolean updateMember(final long uniqueEventID, final String nodeId, final TopologyMemberImpl memberInput) {

      Long deleteTme = getMapDelete().get(nodeId);
      if (deleteTme != null && uniqueEventID != 0 && uniqueEventID < deleteTme) {
         logger.debug("Update uniqueEvent=" + uniqueEventID +
                         ", nodeId=" +
                         nodeId +
                         ", memberInput=" +
                         memberInput +
                         " being rejected as there was a delete done after that");
         return false;
      }

      if (manager != null && !manager.updateMember(uniqueEventID, nodeId, memberInput)) {
         logger.debugf("TopologyManager rejected the update towards %s", memberInput);
         return false;
      }

      synchronized (this) {
         TopologyMemberImpl currentMember = topology.get(nodeId);

         if (currentMember == null) {
            if (logger.isTraceEnabled()) {
               logger.trace(this + "::NewMemberAdd nodeId=" + nodeId + " member = " + memberInput, new Exception("trace"));
            }
            memberInput.setUniqueEventID(uniqueEventID);
            topology.put(nodeId, memberInput);
            sendMemberUp(nodeId, memberInput);
            return true;
         }
         if (uniqueEventID > currentMember.getUniqueEventID() || (currentMember.getLive() == null && memberInput.getLive() != null)) {
            TopologyMemberImpl newMember = new TopologyMemberImpl(nodeId, memberInput.getBackupGroupName(), memberInput.getScaleDownGroupName(), memberInput.getLive(), memberInput.getBackup());

            if (newMember.getLive() == null && currentMember.getLive() != null) {
               newMember.setLive(currentMember.getLive());
            }

            if (newMember.getBackup() == null && currentMember.getBackup() != null) {
               newMember.setBackup(currentMember.getBackup());
            }

            if (logger.isTraceEnabled()) {
               logger.trace(this + "::updated currentMember=nodeID=" + nodeId + ", currentMember=" +
                               currentMember + ", memberInput=" + memberInput + "newMember=" +
                               newMember, new Exception("trace"));
            }

            if (uniqueEventID > currentMember.getUniqueEventID()) {
               newMember.setUniqueEventID(uniqueEventID);
            } else {
               newMember.setUniqueEventID(currentMember.getUniqueEventID());
            }

            topology.remove(nodeId);
            topology.put(nodeId, newMember);
            sendMemberUp(nodeId, newMember);

            return true;
         }
         /*
          * always add the backup, better to try to reconnect to something that's not there then to
          * not know about it at all
          */
         if (currentMember.getBackup() == null && memberInput.getBackup() != null) {
            currentMember.setBackup(memberInput.getBackup());
         }
         return false;
      }
   }

   /**
    * @param nodeId
    * @param memberToSend
    */
   private void sendMemberUp(final String nodeId, final TopologyMemberImpl memberToSend) {
      final ArrayList<ClusterTopologyListener> copy = copyListeners();

      if (logger.isTraceEnabled()) {
         logger.trace(this + "::prepare to send " + nodeId + " to " + copy.size() + " elements");
      }

      if (copy.size() > 0) {
         executor.execute(new Runnable() {
            @Override
            public void run() {
               for (ClusterTopologyListener listener : copy) {
                  if (logger.isTraceEnabled()) {
                     logger.trace(Topology.this + " informing " +
                                     listener +
                                     " about node up = " +
                                     nodeId +
                                     " connector = " +
                                     memberToSend.getConnector());
                  }

                  try {
                     listener.nodeUP(memberToSend, false);
                  } catch (Throwable e) {
                     ActiveMQClientLogger.LOGGER.errorSendingTopology(e);
                  }
               }
            }
         });
      }
   }

   /**
    * @return
    */
   private ArrayList<ClusterTopologyListener> copyListeners() {
      ArrayList<ClusterTopologyListener> listenersCopy;
      synchronized (topologyListeners) {
         listenersCopy = new ArrayList<>(topologyListeners);
      }
      return listenersCopy;
   }

   boolean removeMember(final long uniqueEventID, final String nodeId) {
      TopologyMemberImpl member;


      if (manager != null && !manager.removeMember(uniqueEventID, nodeId)) {
         logger.debugf("TopologyManager rejected the update towards %s", nodeId);
         return false;
      }

      synchronized (this) {
         member = topology.get(nodeId);
         if (member != null) {
            if (member.getUniqueEventID() > uniqueEventID) {
               logger.debug("The removeMember was issued before the node " + nodeId + " was started, ignoring call");
               member = null;
            } else {
               getMapDelete().put(nodeId, uniqueEventID);
               member = topology.remove(nodeId);
            }
         }
      }

      if (logger.isTraceEnabled()) {
         logger.trace("removeMember " + this +
                         " removing nodeID=" +
                         nodeId +
                         ", result=" +
                         member +
                         ", size = " +
                         topology.size(), new Exception("trace"));
      }

      if (member != null) {
         final ArrayList<ClusterTopologyListener> copy = copyListeners();

         executor.execute(new Runnable() {
            @Override
            public void run() {
               for (ClusterTopologyListener listener : copy) {
                  if (logger.isTraceEnabled()) {
                     logger.trace(this + " informing " + listener + " about node down = " + nodeId);
                  }
                  try {
                     listener.nodeDown(uniqueEventID, nodeId);
                  } catch (Exception e) {
                     ActiveMQClientLogger.LOGGER.errorSendingTopologyNodedown(e);
                  }
               }
            }
         });
      }
      return member != null;
   }

   public synchronized void sendTopology(final ClusterTopologyListener listener) {
      if (logger.isDebugEnabled()) {
         logger.debug(this + " is sending topology to " + listener);
      }

      executor.execute(new Runnable() {
         @Override
         public void run() {
            int count = 0;

            final Map<String, TopologyMemberImpl> copy;

            synchronized (Topology.this) {
               copy = new HashMap<>(topology);
            }

            for (Map.Entry<String, TopologyMemberImpl> entry : copy.entrySet()) {
               if (logger.isDebugEnabled()) {
                  logger.debug(Topology.this + " sending " +
                                  entry.getKey() +
                                  " / " +
                                  entry.getValue().getConnector() +
                                  " to " +
                                  listener);
               }
               listener.nodeUP(entry.getValue(), ++count == copy.size());
            }
         }
      });
   }

   public synchronized TopologyMemberImpl getMember(final String nodeID) {
      return topology.get(nodeID);
   }

   public synchronized TopologyMemberImpl getMember(final RemotingConnection rc) {
      for (TopologyMemberImpl member : topology.values()) {
         if (member.isMember(rc)) {
            return member;
         }
      }

      return null;
   }

   public synchronized boolean isEmpty() {
      return topology.isEmpty();
   }

   public Collection<TopologyMemberImpl> getMembers() {
      ArrayList<TopologyMemberImpl> members;
      synchronized (this) {
         members = new ArrayList<>(topology.values());
      }
      return members;
   }

   synchronized int nodes() {
      int count = 0;
      for (TopologyMemberImpl member : topology.values()) {
         if (member.getLive() != null) {
            count++;
         }
         if (member.getBackup() != null) {
            count++;
         }
      }
      return count;
   }

   public synchronized String describe() {
      return describe("");
   }

   private synchronized String describe(final String text) {
      StringBuilder desc = new StringBuilder(text + "topology on " + this + ":\n");
      for (Entry<String, TopologyMemberImpl> entry : new HashMap<>(topology).entrySet()) {
         desc.append("\t").append(entry.getKey()).append(" => ").append(entry.getValue()).append("\n");
      }
      desc.append("\t" + "nodes=").append(nodes()).append("\t").append("members=").append(members());
      if (topology.isEmpty()) {
         desc.append("\tEmpty");
      }
      return desc.toString();
   }

   private int members() {
      return topology.size();
   }

   /**
    * The owner exists mainly for debug purposes.
    * When enabling logging and tracing, the Topology updates will include the owner, what will enable to identify
    * what instances are receiving the updates, what will enable better debugging.
    */
   public void setOwner(final Object owner) {
      this.owner = owner;
   }

   public TransportConfiguration getBackupForConnector(final Connector connector) {
      for (TopologyMemberImpl member : topology.values()) {
         if (member.getLive() != null && connector.isEquivalent(member.getLive().getParams())) {
            return member.getBackup();
         }
      }
      return null;
   }

   @Override
   public String toString() {
      if (owner == null) {
         return "Topology@" + Integer.toHexString(System.identityHashCode(this));
      }
      return "Topology@" + Integer.toHexString(System.identityHashCode(this)) + "[owner=" + owner + "]";
   }

   private synchronized Map<String, Long> getMapDelete() {
      if (mapDelete == null) {
         mapDelete = new ConcurrentHashMap<>();
      }
      return mapDelete;
   }

}
