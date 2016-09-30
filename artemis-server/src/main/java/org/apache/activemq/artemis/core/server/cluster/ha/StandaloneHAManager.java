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
package org.apache.activemq.artemis.core.server.cluster.ha;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.core.server.ActiveMQServer;

/*
* this implementation doesn't really do anything at the minute but this may change so Im leaving it here, Andy...
* */
public class StandaloneHAManager implements HAManager {

   Map<String, ActiveMQServer> servers = new HashMap<>();

   boolean isStarted = false;

   @Override
   public Map<String, ActiveMQServer> getBackupServers() {
      return servers;
   }

   @Override
   public void start() throws Exception {
      if (isStarted)
         return;
      isStarted = true;
   }

   @Override
   public void stop() throws Exception {
      if (!isStarted)
         return;
      isStarted = false;
   }

   @Override
   public boolean isStarted() {
      return isStarted;
   }
}
