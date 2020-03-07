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
package org.apache.activemq.artemis.core.management.impl.view;

import javax.json.JsonObjectBuilder;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.management.impl.view.predicate.ConnectionFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.apache.activemq.artemis.utils.StringUtil;

public class ConnectionView extends ActiveMQAbstractView<RemotingConnection> {

   private static final String defaultSortColumn = "connectionID";

   private final ActiveMQServer server;

   public ConnectionView(ActiveMQServer server) {
      super();
      this.server = server;
      this.predicate = new ConnectionFilterPredicate(server);
   }

   @Override
   public Class getClassT() {
      return RemotingConnection.class;
   }

   @Override
   public JsonObjectBuilder toJson(RemotingConnection connection) {

      List<ServerSession> sessions = server.getSessions(connection.getID().toString());
      Set<String> users = new TreeSet<>();
      String jmsSessionClientID = null;
      for (ServerSession session : sessions) {
         String username = session.getUsername() == null ? "" : session.getUsername();
         users.add(username);
         //for the special case for JMS
         if (session.getMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY) != null) {
            jmsSessionClientID = session.getMetaData("jms-client-id");
         }
      }

      return JsonLoader.createObjectBuilder().add("connectionID", toString(connection.getID()))
         .add("remoteAddress", toString(connection.getRemoteAddress()))
         .add("users", StringUtil.joinStringList(users, ","))
         .add("creationTime", new Date(connection.getCreationTime()).toString())
         .add("implementation", toString(connection.getClass().getSimpleName()))
         .add("protocol", toString(connection.getProtocolName()))
         .add("clientID", toString(connection.getClientID() != null ? connection.getClientID() : jmsSessionClientID))
         .add("localAddress", toString(connection.getTransportLocalAddress()))
         .add("sessionCount", sessions.size());
   }

   @Override
   public Object getField(RemotingConnection connection, String fieldName) {
      switch (fieldName) {
         case "connectionID":
            return connection.getID();
         case "remoteAddress":
            return connection.getRemoteAddress();
         case "users":
            Set<String> users = new TreeSet<>();
            List<ServerSession> sessions = server.getSessions(connection.getID().toString());
            for (ServerSession session : sessions) {
               String username = session.getUsername() == null ? "" : session.getUsername();
               users.add(username);
            }
            return StringUtil.joinStringList(users, ",");
         case "creationTime":
            return new Date(connection.getCreationTime());
         case "implementation":
            return connection.getClass().getSimpleName();
         case "protocol":
            return connection.getProtocolName();
         case "clientID":
            return connection.getClientID();
         case "localAddress":
            return connection.getTransportLocalAddress();
         case "sessionCount":
            return server.getSessions(connection.getID().toString()).size();
         default:
            throw new IllegalArgumentException("Unsupported field, " + fieldName);
      }
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortColumn;
   }
}
