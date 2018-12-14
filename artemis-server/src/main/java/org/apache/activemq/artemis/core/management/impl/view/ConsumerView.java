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

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.management.impl.view.predicate.ConsumerFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.utils.JsonLoader;

public class ConsumerView extends ActiveMQAbstractView<ServerConsumer> {

   private static final String defaultSortColumn = "id";

   private final ActiveMQServer server;

   public ConsumerView(ActiveMQServer server) {
      super();
      this.server = server;
      this.predicate = new ConsumerFilterPredicate(server);
   }

   @Override
   public Class getClassT() {
      return ServerConsumer.class;
   }

   @Override
   public JsonObjectBuilder toJson(ServerConsumer consumer) {
      ServerSession session = server.getSessionByID(consumer.getSessionID());

      //if session is not available then consumer is not in valid state - ignore
      if (session == null) {
         return null;
      }

      String jmsSessionClientID = null;
      //for the special case for JMS
      if (session.getMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY) != null) {
         jmsSessionClientID = session.getMetaData("jms-client-id");
      }

      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("id", toString(consumer.getSequentialID()))
         .add("session", toString(consumer.getSessionName()))
         .add("clientID", toString(consumer.getConnectionClientID() != null ? consumer.getConnectionClientID() : jmsSessionClientID))
         .add("user", toString(session.getUsername()))
         .add("protocol", toString(consumer.getConnectionProtocolName()))
         .add("queue", toString(consumer.getQueueName()))
         .add("queueType", toString(consumer.getQueueType()).toLowerCase())
         .add("filter", toString(consumer.getFilterString()))
         .add("address", toString(consumer.getQueueAddress()))
         .add("localAddress", toString(consumer.getConnectionLocalAddress()))
         .add("remoteAddress", toString(consumer.getConnectionRemoteAddress()))
         .add("creationTime", new Date(consumer.getCreationTime()).toString());
      return obj;
   }

   @Override
   public Object getField(ServerConsumer consumer, String fieldName) {
      ServerSession session = server.getSessionByID(consumer.getSessionID());

      //if session is not available then consumer is not in valid state - ignore
      if (session == null) {
         return null;
      }

      switch (fieldName) {
         case "id":
            return consumer.getSequentialID();
         case "session":
            return consumer.getSessionName();
         case "user":
            return session.getUsername();
         case "clientID":
            return consumer.getConnectionClientID();
         case "protocol":
            return consumer.getConnectionProtocolName();
         case "queue":
            return consumer.getQueueName();
         case "queueType":
            return consumer.getQueueType();
         case "filter":
            return consumer.getFilterString();
         case "localAddress":
            return consumer.getConnectionLocalAddress();
         case "remoteAddress":
            return consumer.getConnectionRemoteAddress();
         case "creationTime":
            return new Date(consumer.getCreationTime());
         default:
            throw new IllegalArgumentException("Unsupported field, " + fieldName);
      }
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortColumn;
   }
}
