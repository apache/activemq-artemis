/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.cluster.qourum;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;

import java.util.Map;

public class ServerConnectVote extends BooleanVote {

   private String nodeId;
   private String transportConfiguration;

   public ServerConnectVote(String nodeId) {
      super(false);
      this.nodeId = nodeId;
   }

   public ServerConnectVote() {
      super(false);
   }

   public ServerConnectVote(String nodeid, boolean isLive, String transportConfiguration) {
      super(isLive);
      this.nodeId = nodeid;
      this.transportConfiguration = transportConfiguration;
   }

   @Override
   public boolean isRequestServerVote() {
      return true;
   }

   @Override
   public Map<String, Object> getVoteMap() {
      return null;
   }

   @Override
   public void encode(ActiveMQBuffer buff) {
      super.encode(buff);
      buff.writeString(nodeId);
      buff.writeNullableString(transportConfiguration);
   }

   public String getTransportConfiguration() {
      return transportConfiguration;
   }

   @Override
   public void decode(ActiveMQBuffer buff) {
      super.decode(buff);
      nodeId = buff.readString();
      transportConfiguration = buff.readNullableString();
   }

   public String getNodeId() {
      return nodeId;
   }

   @Override
   public String toString() {
      return "ServerConnectVote [nodeId=" + nodeId + ", vote=" + vote + "]";
   }
}