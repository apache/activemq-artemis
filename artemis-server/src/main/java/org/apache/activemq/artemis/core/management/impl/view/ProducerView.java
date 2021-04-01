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

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.management.impl.view.predicate.ProducerFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerProducer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.utils.JsonLoader;

public class ProducerView extends ActiveMQAbstractView<ServerProducer> {

   private static final String defaultSortColumn = ProducerField.CREATION_TIME.getName();

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

      String sessionClientID = session.getRemotingConnection().getClientID();
      //for the special case for JMS
      if (sessionClientID == null && session.getMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY) != null) {
         sessionClientID = session.getMetaData("jms-client-id");
      }

      JsonObjectBuilder obj = JsonLoader.createObjectBuilder()
         .add(ProducerField.ID.getName(), toString(producer.getID()))
         .add(ProducerField.SESSION.getName(), toString(session.getName()))
         .add(ProducerField.CLIENT_ID.getName(), toString(sessionClientID))
         .add(ProducerField.USER.getName(), toString(session.getUsername()))
         .add(ProducerField.PROTOCOL.getName(), toString(session.getRemotingConnection().getProtocolName()))
         .add(ProducerField.ADDRESS.getName(), toString(producer.getAddress() != null ? producer.getAddress() : session.getDefaultAddress()))
         .add(ProducerField.LOCAL_ADDRESS.getName(), toString(session.getRemotingConnection().getTransportConnection().getLocalAddress()))
         .add(ProducerField.REMOTE_ADDRESS.getName(), toString(session.getRemotingConnection().getTransportConnection().getRemoteAddress()))
         .add(ProducerField.CREATION_TIME.getName(), toString(producer.getCreationTime()));
      return obj;
   }

   @Override
   public Object getField(ServerProducer producer, String fieldName) {
      ServerSession session = server.getSessionByID(producer.getSessionID());

      //if session is not available then consumer is not in valid state - ignore
      if (session == null) {
         return null;
      }

      ProducerField field = ProducerField.valueOfName(fieldName);

      switch (field) {
         case ID:
            return producer.getID();
         case SESSION:
            return session.getName();
         case USER:
            return session.getUsername();
         case CLIENT_ID:
            return session.getRemotingConnection().getClientID();
         case PROTOCOL:
            return session.getRemotingConnection().getProtocolName();
         case ADDRESS:
            return producer.getAddress() != null ? producer.getAddress() : session.getDefaultAddress();
         case LOCAL_ADDRESS:
            return session.getRemotingConnection().getTransportConnection().getLocalAddress();
         case REMOTE_ADDRESS:
            return session.getRemotingConnection().getTransportConnection().getRemoteAddress();
         case CREATION_TIME:
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
