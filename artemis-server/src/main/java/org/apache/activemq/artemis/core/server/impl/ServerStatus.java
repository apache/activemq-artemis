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

   public static final String SERVER_COMPONENT = "server";
   public static final String CONFIGURATION_COMPONENT = "configuration";
   public static final String JAAS_COMPONENT = SERVER_COMPONENT + "/jaas";

   private static final ServerStatus instance = new ServerStatus();

   public static synchronized ServerStatus getInstanceFor(ActiveMQServerImpl activeMQServer) {
      if (instance.server == null) {
         instance.server = activeMQServer;
         instance.immutableStateValues.put("version", instance.server.getVersion().getFullVersion());
      }
      return instance;
   }

   public static synchronized ServerStatus getInstance() {
      return instance;
   }

   private ActiveMQServerImpl server;
   private final HashMap<String, String> immutableStateValues = new HashMap<>();
   private JsonObject globalStatus = JsonLoader.createObjectBuilder().build();

   public synchronized String asJson() {
      updateServerStatus();
      return globalStatus.toString();
   }

   private synchronized void updateServerStatus() {
      if (instance.server != null) {
         HashMap<String, String> snapshotOfServerStatusAttributes = new HashMap<>();
         snapshotOfServerStatusAttributes.putAll(immutableStateValues);
         snapshotOfServerStatusAttributes.put("identity", server.getIdentity());
         SimpleString nodeId = server.getNodeID();
         snapshotOfServerStatusAttributes.put("nodeId", nodeId == null ? null : nodeId.toString());
         snapshotOfServerStatusAttributes.put("uptime", server.getUptime());
         snapshotOfServerStatusAttributes.put("state", server.getState().toString());

         update(SERVER_COMPONENT, JsonUtil.toJsonObject(snapshotOfServerStatusAttributes));
      }
   }

   public synchronized void update(String component, String statusJson) {
      update(component, JsonUtil.readJsonObject(statusJson));
   }

   public synchronized void update(String component, HashMap<String, String> statusAttributes) {
      update(component, JsonUtil.toJsonObject(statusAttributes));
   }

   public synchronized void update(String componentPath, JsonObject componentStatus) {
      JsonObjectBuilder jsonObjectBuilder = JsonUtil.objectBuilderWithValueAtPath(componentPath, componentStatus);
      globalStatus = JsonUtil.mergeAndUpdate(globalStatus, jsonObjectBuilder.build());
   }

}
