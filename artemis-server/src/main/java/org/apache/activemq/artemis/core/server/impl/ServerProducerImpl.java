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

import org.apache.activemq.artemis.core.server.ServerProducer;

public class ServerProducerImpl implements ServerProducer {
   private final String ID;
   private final String protocol;
   private final long creationTime;


   private final String address;
   private String sessionID;
   private String connectionID;

   public ServerProducerImpl(String ID, String protocol, String address) {
      this.ID = ID;
      this.protocol = protocol;
      this.address = address;
      this.creationTime = System.currentTimeMillis();
   }

   @Override
   public String getAddress() {
      return address;
   }

   @Override
   public String getProtocol() {
      return protocol;
   }

   @Override
   public void setSessionID(String sessionID) {
      this.sessionID = sessionID;
   }

   @Override
   public void setConnectionID(String connectionID) {

      this.connectionID = connectionID;
   }

   @Override
   public String getSessionID() {
      return sessionID;
   }

   @Override
   public String getConnectionID() {
      return connectionID;
   }

   @Override
   public String getID() {
      return ID;
   }

   @Override
   public long getCreationTime() {
      return creationTime;
   }
}
