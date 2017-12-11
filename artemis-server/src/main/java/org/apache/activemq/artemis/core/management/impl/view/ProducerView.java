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

import org.apache.activemq.artemis.core.management.impl.view.predicate.ProducerFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerProducer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.utils.JsonLoader;

public class ProducerView extends ActiveMQAbstractView<ServerProducer> {

   private static final String defaultSortColumn = "creationTime";

   private final ActiveMQServer server;

   public ProducerView(ActiveMQServer server) {
      super();
      this.server = server;
      this.predicate = new ProducerFilterPredicate(server);
   }

   @Override
   public Class getClassT() {
      return ServerProducer.class;
   }

   @Override
   public JsonObjectBuilder toJson(ServerProducer producer) {
      ServerSession session = server.getSessionByID(producer.getSessionID());

      //if session is not available then consumer is not in valid state - ignore
      if (session == null) {
         return null;
      }

      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("id", toString(producer.getID()))
         .add("session", toString(session.getName()))
         .add("clientID", toString(session.getRemotingConnection().getClientID()))
         .add("user", toString(session.getUsername()))
         .add("protocol", toString(session.getRemotingConnection().getProtocolName()))
         .add("address", toString(producer.getAddress() != null ? producer.getAddress() : session.getDefaultAddress()))
         .add("localAddress", toString(session.getRemotingConnection().getTransportConnection().getLocalAddress()))
         .add("remoteAddress", toString(session.getRemotingConnection().getTransportConnection().getRemoteAddress()))
         .add("creationTime", toString(producer.getCreationTime()));
      return obj;
   }

   @Override
   public Object getField(ServerProducer producer, String fieldName) {
      ServerSession session = server.getSessionByID(producer.getSessionID());

      //if session is not available then consumer is not in valid state - ignore
      if (session == null) {
         return null;
      }

      switch (fieldName) {
         case "id":
            return producer.getID();
         case "session":
            return session.getName();
         case "user":
            return session.getUsername();
         case "clientID":
            return session.getRemotingConnection().getClientID();
         case "protocol":
            return session.getRemotingConnection().getProtocolName();
         case "address":
            return producer.getAddress() != null ? producer.getAddress() : session.getDefaultAddress();
         case "localAddress":
            return session.getRemotingConnection().getTransportConnection().getLocalAddress();
         case "remoteAddress":
            return session.getRemotingConnection().getTransportConnection().getRemoteAddress();
         case "creationTime":
            return producer.getCreationTime();
         default:
            throw new IllegalArgumentException("Unsupported field, " + fieldName);
      }
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortColumn;
   }
}
