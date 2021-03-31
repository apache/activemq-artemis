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

   private static final String defaultSortColumn = ConsumerField.ID.getName();

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

      String consumerClientID = consumer.getConnectionClientID();
      if (consumerClientID == null && session.getMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY) != null) {
         //for the special case for JMS
         consumerClientID = session.getMetaData("jms-client-id");
      }

      JsonObjectBuilder obj = JsonLoader.createObjectBuilder()
         .add(ConsumerField.ID.getName(), toString(consumer.getSequentialID()))
         .add(ConsumerField.SESSION.getName(), toString(consumer.getSessionName()))
         .add(ConsumerField.CLIENT_ID.getName(), toString(consumerClientID))
         .add(ConsumerField.USER.getName(), toString(session.getUsername()))
         .add(ConsumerField.PROTOCOL.getName(), toString(consumer.getConnectionProtocolName()))
         .add(ConsumerField.QUEUE.getName(), toString(consumer.getQueueName()))
         .add(ConsumerField.QUEUE_TYPE.getName(), toString(consumer.getQueueType()).toLowerCase())
         .add(ConsumerField.FILTER.getName(), toString(consumer.getFilterString()))
         .add(ConsumerField.ADDRESS.getName(), toString(consumer.getQueueAddress()))
         .add(ConsumerField.LOCAL_ADDRESS.getName(), toString(consumer.getConnectionLocalAddress()))
         .add(ConsumerField.REMOTE_ADDRESS.getName(), toString(consumer.getConnectionRemoteAddress()))
         .add(ConsumerField.CREATION_TIME.getName(), new Date(consumer.getCreationTime()).toString());
      return obj;
   }

   @Override
   public Object getField(ServerConsumer consumer, String fieldName) {
      ServerSession session = server.getSessionByID(consumer.getSessionID());

      //if session is not available then consumer is not in valid state - ignore
      if (session == null) {
         return null;
      }

      ConsumerField field = ConsumerField.valueOfName(fieldName);

      switch (field) {
         case ID:
            return consumer.getSequentialID();
         case SESSION:
            return consumer.getSessionName();
         case USER:
            return session.getUsername();
         case CLIENT_ID:
            return consumer.getConnectionClientID();
         case PROTOCOL:
            return consumer.getConnectionProtocolName();
         case QUEUE:
            return consumer.getQueueName();
         case QUEUE_TYPE:
            return consumer.getQueueType();
         case FILTER:
            return consumer.getFilterString();
         case LOCAL_ADDRESS:
            return consumer.getConnectionLocalAddress();
         case REMOTE_ADDRESS:
            return consumer.getConnectionRemoteAddress();
         case CREATION_TIME:
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
