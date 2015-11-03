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

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

public final class TopologyMemberImpl implements TopologyMember {

   private static final long serialVersionUID = 1123652191795626133L;

   private final Pair<TransportConfiguration, TransportConfiguration> connector;

   private final String backupGroupName;

   private final String scaleDownGroupName;

   /**
    * transient to avoid serialization changes
    */
   private transient long uniqueEventID = System.currentTimeMillis();

   private final String nodeId;

   public TopologyMemberImpl(String nodeId,
                             final String backupGroupName,
                             final String scaleDownGroupName,
                             final TransportConfiguration a,
                             final TransportConfiguration b) {
      this.nodeId = nodeId;
      this.backupGroupName = backupGroupName;
      this.scaleDownGroupName = scaleDownGroupName;
      this.connector = new Pair<TransportConfiguration, TransportConfiguration>(a, b);
      uniqueEventID = System.currentTimeMillis();
   }

   @Override
   public TransportConfiguration getLive() {
      return connector.getA();
   }

   @Override
   public TransportConfiguration getBackup() {
      return connector.getB();
   }

   public void setBackup(final TransportConfiguration param) {
      connector.setB(param);
   }

   public void setLive(final TransportConfiguration param) {
      connector.setA(param);
   }

   @Override
   public String getNodeId() {
      return nodeId;
   }

   @Override
   public long getUniqueEventID() {
      return uniqueEventID;
   }

   @Override
   public String getBackupGroupName() {
      return backupGroupName;
   }

   @Override
   public String getScaleDownGroupName() {
      return scaleDownGroupName;
   }

   /**
    * @param uniqueEventID the uniqueEventID to set
    */
   public void setUniqueEventID(final long uniqueEventID) {
      this.uniqueEventID = uniqueEventID;
   }

   public Pair<TransportConfiguration, TransportConfiguration> getConnector() {
      return connector;
   }

   public boolean isMember(RemotingConnection connection) {
      TransportConfiguration connectorConfig = connection.getTransportConnection() != null ? connection.getTransportConnection().getConnectorConfig() : null;

      return isMember(connectorConfig);

   }

   public boolean isMember(TransportConfiguration configuration) {
      if (getConnector().getA() != null && getConnector().getA().isSameParams(configuration) || getConnector().getB() != null && getConnector().getB().isSameParams(configuration)) {
         return true;
      }
      else {
         return false;
      }
   }

   @Override
   public String toString() {
      return "TopologyMember[id = " + nodeId + ", connector=" + connector + ", backupGroupName=" + backupGroupName + ", scaleDownGroupName=" + scaleDownGroupName + "]";
   }
}
