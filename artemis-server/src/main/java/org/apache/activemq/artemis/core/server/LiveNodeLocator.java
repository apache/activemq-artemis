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
package org.apache.activemq.artemis.core.server;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.server.cluster.qourum.SharedNothingBackupQuorum;

/**
 * A class that will locate a particular live server running in a cluster. How this live is chosen
 * is a job for the implementation.
 *
 * This is used for replication (which needs a QuorumManager) and scaling-down (which does not need a QuorumManager).
 */
public abstract class LiveNodeLocator implements ClusterTopologyListener {

   private SharedNothingBackupQuorum backupQuorum;

   public LiveNodeLocator(SharedNothingBackupQuorum backupQuorum) {
      this.backupQuorum = backupQuorum;
   }

   /**
    * Use this constructor when the LiveNodeLocator is used for scaling down rather than replicating
    */
   public LiveNodeLocator() {
   }

   /**
    * Locates a possible live server in a cluster with a timeout
    */
   public abstract void locateNode(long timeout) throws ActiveMQException;

   /**
    * Locates a possible live server in a cluster
    */
   public abstract void locateNode() throws ActiveMQException;

   /**
    * Returns the current connector
    */
   public abstract Pair<TransportConfiguration, TransportConfiguration> getLiveConfiguration();

   /**
    * Returns the node id for the current connector
    */
   public abstract String getNodeID();

   /**
    * tells the locator the the current connector has failed.
    */
   public void notifyRegistrationFailed(boolean alreadyReplicating) {
      if (backupQuorum != null) {
         if (alreadyReplicating) {
            backupQuorum.notifyAlreadyReplicating();
         } else {
            backupQuorum.notifyRegistrationFailed();
         }
      }
   }

   /**
    * connects to the cluster
    */
   public void connectToCluster(ServerLocatorInternal serverLocator) throws ActiveMQException {
      serverLocator.connect();
   }
}
