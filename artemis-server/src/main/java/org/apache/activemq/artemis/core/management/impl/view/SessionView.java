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

import org.apache.activemq.artemis.core.management.impl.view.predicate.SessionFilterPredicate;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.utils.JsonLoader;

public class SessionView extends ActiveMQAbstractView<ServerSession> {

   private static final String defaultSortColumn = "id";

   public SessionView() {
      super();
      this.predicate = new SessionFilterPredicate();
   }

   @Override
   public Class getClassT() {
      return ServerSession.class;
   }

   @Override
   public JsonObjectBuilder toJson(ServerSession session) {
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("id", toString(session.getName()))
         .add("user", toString(session.getUsername()))
         .add("creationTime", new Date(session.getCreationTime()).toString())
         .add("consumerCount", session.getConsumerCount())
         .add("producerCount", session.getProducerCount())
         .add("connectionID", session.getConnectionID().toString());
      return obj;
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortColumn;
   }
}
