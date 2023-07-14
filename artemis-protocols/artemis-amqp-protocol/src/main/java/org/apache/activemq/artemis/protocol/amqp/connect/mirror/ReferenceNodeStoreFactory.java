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

package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.utils.collections.NodeStore;
import org.apache.activemq.artemis.utils.collections.NodeStoreFactory;

import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_BROKER_ID_EXTRA_PROPERTY;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_ID_EXTRA_PROPERTY;

public class ReferenceNodeStoreFactory implements NodeStoreFactory<MessageReference> {

   final ActiveMQServer server;

   private final String serverID;

   public ReferenceNodeStoreFactory(ActiveMQServer server) {
      this.server = server;
      this.serverID = server.getNodeID().toString();

   }

   @Override
   public NodeStore<MessageReference> newNodeStore() {
      return new ReferenceNodeStore(this);
   }

   public String getDefaultNodeID() {
      return serverID;
   }

   public String getServerID(MessageReference element) {
      return getServerID(element.getMessage());
   }


   public String getServerID(Message message) {
      Object nodeID = message.getBrokerProperty(INTERNAL_BROKER_ID_EXTRA_PROPERTY);
      if (nodeID != null) {
         return nodeID.toString();
      } else {
         // it is important to return null here, as the MirrorSource is expecting it to be null
         // in the case the nodeID being from the originating server.
         // don't be tempted to return this.serverID here.
         return null;
      }
   }

   public long getID(MessageReference element) {
      Message message = element.getMessage();
      Long id = getID(message);
      if (id == null) {
         return element.getMessageID();
      } else {
         return id;
      }
   }

   private Long getID(Message message) {
      return (Long)message.getBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY);
   }


}
