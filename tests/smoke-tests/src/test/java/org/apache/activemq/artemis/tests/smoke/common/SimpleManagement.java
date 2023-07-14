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

package org.apache.activemq.artemis.tests.smoke.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;

public class SimpleManagement {

   private static final String SIMPLE_OPTIONS  = "{\"field\":\"\",\"value\":\"\",\"operation\":\"\"}";

   /** Simple management function that will return a list of Pair<Name of Queue, Number of Messages> */
   public static Map<String, Integer> listQueues(String uri, String user, String password, int maxRows) throws Exception {
      Map<String, Integer> queues = new HashMap<>();
      ManagementHelper.doManagement(uri, user, password, t -> setupListQueue(t, maxRows), t -> listQueueResult(t, queues), SimpleManagement::failed);
      return queues;
   }

   private static void setupListQueue(ClientMessage m, int maxRows) throws Exception {
      ManagementHelper.putOperationInvocation(m, "broker", "listQueues", SIMPLE_OPTIONS, 1, maxRows);
   }

   private static void listQueueResult(ClientMessage message, Map<String, Integer> mapQueues) throws Exception {

      final String result = (String) ManagementHelper.getResult(message, String.class);


      JsonObject queuesAsJsonObject = JsonUtil.readJsonObject(result);
      JsonArray array = queuesAsJsonObject.getJsonArray("data");

      for (int i = 0; i < array.size(); i++) {
         JsonObject object = array.getJsonObject(i);
         String name = object.getString("name");
         String messageCount = object.getString("messageCount");
         mapQueues.put(name, Integer.parseInt(messageCount));
      }

   }

   private static void failed(ClientMessage message) throws Exception {

      final String result = (String) ManagementHelper.getResult(message, String.class);

      throw new Exception("Failed " + result);
   }

}
