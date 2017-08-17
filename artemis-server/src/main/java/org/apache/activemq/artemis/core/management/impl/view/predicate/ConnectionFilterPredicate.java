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
package org.apache.activemq.artemis.core.management.impl.view.predicate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

public class ConnectionFilterPredicate extends ActiveMQFilterPredicate<RemotingConnection> {

   enum Field {
      CONNECTION_ID, CLIENT_ID, USERS, PROTOCOL, SESSION_COUNT, REMOTE_ADDRESS, LOCAL_ADDRESS, SESSION_ID
   }

   private Field f;

   private ActiveMQServer server;

   public ConnectionFilterPredicate(ActiveMQServer server) {
      this.server = server;
   }

   @Override
   public boolean apply(RemotingConnection connection) {
      // Using switch over enum vs string comparison is better for perf.
      if (f == null)
         return true;
      switch (f) {
         case CONNECTION_ID:
            return matches(connection.getID());
         case CLIENT_ID:
            return matches(connection.getClientID());
         case USERS:
            List<ServerSession> sessions = server.getSessions(connection.getID().toString());
            Set<String> users = new HashSet<>();
            for (ServerSession session : sessions) {
               String username = session.getUsername() == null ? "" : session.getUsername();
               users.add(username);
            }
            return matchAny(users);
         case PROTOCOL:
            return matches(connection.getProtocolName());
         case SESSION_COUNT:
            return matches(server.getSessions(connection.getID().toString()).size());
         case REMOTE_ADDRESS:
            return matches(connection.getTransportConnection().getRemoteAddress());
         case LOCAL_ADDRESS:
            return matches(connection.getTransportConnection().getLocalAddress());
         case SESSION_ID:
            return matchAny(server.getSessions(connection.getID().toString()));
      }
      return true;
   }

   @Override
   public void setField(String field) {
      if (field != null && !field.equals("")) {
         this.f = Field.valueOf(field.toUpperCase());
      }
   }
}
