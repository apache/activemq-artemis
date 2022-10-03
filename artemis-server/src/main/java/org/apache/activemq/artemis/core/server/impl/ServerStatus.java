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

package org.apache.activemq.artemis.core.server.impl;


import java.util.HashMap;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.apache.activemq.artemis.utils.JsonLoader;

public class ServerStatus {

   private final ActiveMQServerImpl server;
   private final HashMap<String, String> immutableStateValues = new HashMap<>();
   private JsonObject globalStatus = JsonLoader.createObjectBuilder().build();

   public ServerStatus(ActiveMQServerImpl activeMQServer) {
      this.server = activeMQServer;
      immutableStateValues.put("version", server.getVersion().getFullVersion());
   }

   public synchronized String asJson() {
      updateServerStatus();
      return globalStatus.toString();
   }

   private synchronized void updateServerStatus() {
      HashMap<String, String> snapshotOfServerStatusAttributes = new HashMap<>();
      snapshotOfServerStatusAttributes.putAll(immutableStateValues);
      snapshotOfServerStatusAttributes.put("identity", server.getIdentity());
      SimpleString nodeId = server.getNodeID();
      snapshotOfServerStatusAttributes.put("nodeId", nodeId == null ? null : nodeId.toString());
      snapshotOfServerStatusAttributes.put("uptime", server.getUptime());
      snapshotOfServerStatusAttributes.put("state", server.getState().toString());

      update("server", JsonUtil.toJsonObject(snapshotOfServerStatusAttributes));
   }

   public synchronized void update(String component, String statusJson) {
      JsonObjectBuilder jsonObjectBuilder = JsonLoader.createObjectBuilder();
      jsonObjectBuilder.add(component, JsonUtil.readJsonObject(statusJson));
      globalStatus = JsonUtil.mergeAndUpdate(globalStatus, jsonObjectBuilder.build());
   }

   public synchronized void update(String component, JsonObject componentStatus) {
      JsonObjectBuilder jsonObjectBuilder = JsonLoader.createObjectBuilder();
      jsonObjectBuilder.add(component, componentStatus);
      globalStatus = JsonUtil.mergeAndUpdate(globalStatus, jsonObjectBuilder.build());
   }

}
