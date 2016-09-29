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
package org.apache.activemq.artemis.api.jms.management;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.apache.activemq.artemis.api.core.JsonUtil;

public class JMSSessionInfo {

   private final String sessionID;

   private final long creationTime;

   public JMSSessionInfo(String sessionID, long creationTime) {
      this.sessionID = sessionID;
      this.creationTime = creationTime;
   }

   public static JMSSessionInfo[] from(final String jsonString) {
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      JMSSessionInfo[] infos = new JMSSessionInfo[array.size()];
      for (int i = 0; i < array.size(); i++) {
         JsonObject obj = array.getJsonObject(i);

         JMSSessionInfo info = new JMSSessionInfo(obj.getString("sessionID"), obj.getJsonNumber("creationTime").longValue());
         infos[i] = info;
      }
      return infos;
   }

   public String getSessionID() {
      return sessionID;
   }

   public long getCreationTime() {
      return creationTime;
   }
}
