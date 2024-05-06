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

package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckManagerProvider {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static HashMap<ActiveMQServer, AckManager> managerHashMap = new HashMap<>();

   public static void reset() {
      managerHashMap.clear();
   }

   public static void remove(ActiveMQServer server) {
      logger.debug("Removing {}", server);
      synchronized (managerHashMap) {
         managerHashMap.remove(server);
      }
   }

   public static int getSize() {
      synchronized (managerHashMap) {
         return managerHashMap.size();
      }
   }

   public static AckManager getManager(ActiveMQServer server) {
      synchronized (managerHashMap) {
         AckManager ackManager = managerHashMap.get(server);
         if (ackManager != null) {
            return ackManager;
         }

         ackManager = new AckManager(server);
         managerHashMap.put(server, ackManager);
         try {
            server.addExternalComponent(ackManager, false);
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }
         return ackManager;
      }

   }

}
