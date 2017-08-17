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

import org.apache.activemq.artemis.core.management.impl.view.predicate.ConsumerFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.utils.JsonLoader;

public class ConsumerView extends ActiveMQAbstractView<ServerConsumer> {

   private static final String defaultSortColumn = "creationTime";

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
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("id", toString(consumer.sequentialID())).add("session", toString(session.getName())).add("clientID", toString(session.getRemotingConnection().getClientID())).add("user", toString(session.getUsername())).add("protocol", toString(session.getRemotingConnection().getProtocolName())).add("queue", toString(consumer.getQueue().getName())).add("queueType", toString(consumer.getQueue().getRoutingType()).toLowerCase()).add("address", toString(consumer.getQueue().getAddress().toString())).add("localAddress", toString(session.getRemotingConnection().getTransportConnection().getLocalAddress())).add("remoteAddress", toString(session.getRemotingConnection().getTransportConnection().getRemoteAddress())).add("creationTime", new Date(consumer.getCreationTime()).toString());
      return obj;
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortColumn;
   }
}
