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

import org.apache.activemq.artemis.core.management.impl.view.ProducerField;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerProducer;

public class ProducerFilterPredicate extends ActiveMQFilterPredicate<ServerProducer> {

   private ProducerField f;

   private final ActiveMQServer server;

   public ProducerFilterPredicate(ActiveMQServer server) {
      super();
      this.server = server;
   }

   @Override
   public boolean test(ServerProducer producer) {
      // Using switch over enum vs string comparison is better for perf.
      if (f == null)
         return true;
      switch (f) {
         case ID:
            return matches(producer.getID());
         case CONNECTION_ID:
            return matches(producer.getConnectionID());
         case SESSION:
            return matches(producer.getSessionID());
         case USER:
            return matches(server.getSessionByID(producer.getSessionID()).getUsername());
         case ADDRESS:
            return matches(producer.getAddress() != null ? producer.getAddress() : server.getSessionByID(producer.getSessionID()).getDefaultAddress());
         case PROTOCOL:
            return matches(producer.getProtocol());
         case CLIENT_ID:
            return matches(server.getSessionByID(producer.getSessionID()).getRemotingConnection().getClientID());
         case LOCAL_ADDRESS:
            return matches(server.getSessionByID(producer.getSessionID()).getRemotingConnection().getTransportConnection().getLocalAddress());
         case REMOTE_ADDRESS:
            return matches(server.getSessionByID(producer.getSessionID()).getRemotingConnection().getTransportConnection().getRemoteAddress());
         default: return true;
      }
   }

   @Override
   public void setField(String field) {
      if (field != null && !field.equals("")) {
         this.f = ProducerField.valueOfName(field);

         //for backward compatibility
         if (this.f == null) {
            this.f = ProducerField.valueOf(field);
         }
      }
   }
}
