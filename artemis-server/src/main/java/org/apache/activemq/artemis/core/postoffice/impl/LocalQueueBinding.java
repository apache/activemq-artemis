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
package org.apache.activemq.artemis.core.postoffice.impl;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.Bindable;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.ServerMessage;

public class LocalQueueBinding implements QueueBinding {

   private final SimpleString address;

   private final Queue queue;

   private final Filter filter;

   private final SimpleString clusterName;

   private SimpleString name;

   public LocalQueueBinding(final SimpleString address, final Queue queue, final SimpleString nodeID) {
      this.address = address;

      this.queue = queue;

      this.name = queue.getName();

      filter = queue.getFilter();

      clusterName = queue.getName().concat(nodeID);
   }

   @Override
   public long getID() {
      return queue.getID();
   }

   @Override
   public Filter getFilter() {
      return filter;
   }

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public Bindable getBindable() {
      return queue;
   }

   @Override
   public Queue getQueue() {
      return queue;
   }

   @Override
   public SimpleString getRoutingName() {
      if (queue.getRoutingType() == RoutingType.ANYCAST) {
         return address;
      }
      return name;
   }

   @Override
   public SimpleString getUniqueName() {
      return queue.getName();
   }

   @Override
   public SimpleString getClusterName() {
      return clusterName;
   }

   @Override
   public boolean isExclusive() {
      return false;
   }

   @Override
   public int getDistance() {
      return 0;
   }

   @Override
   public boolean isHighAcceptPriority(final ServerMessage message) {
      // It's a high accept priority if the queue has at least one matching consumer

      return queue.hasMatchingConsumer(message);
   }

   @Override
   public void unproposed(SimpleString groupID) {
      queue.unproposed(groupID);
   }

   @Override
   public void route(final ServerMessage message, final RoutingContext context) throws Exception {
      if (isMatchRoutingType(message)) {
         queue.route(message, context);
      }
   }

   @Override
   public void routeWithAck(ServerMessage message, RoutingContext context) throws Exception {
      if (isMatchRoutingType(message)) {
         queue.routeWithAck(message, context);
      }
   }

   private boolean isMatchRoutingType(ServerMessage message) {
      if (message.containsProperty(MessageInternal.HDR_ROUTING_TYPE)) {
         return message.getByteProperty(MessageInternal.HDR_ROUTING_TYPE) == queue.getRoutingType().getType();
      }
      return true;
   }

   public boolean isQueueBinding() {
      return true;
   }

   @Override
   public int consumerCount() {
      return queue.getConsumerCount();
   }

   @Override
   public BindingType getType() {
      return BindingType.LOCAL_QUEUE;
   }

   @Override
   public void close() throws Exception {
      queue.close();
   }

   @Override
   public String toString() {
      return "LocalQueueBinding [address=" + address +
         ", queue=" +
         queue +
         ", filter=" +
         filter +
         ", name=" +
         name +
         ", clusterName=" +
         clusterName +
         "]";
   }

   @Override
   public String toManagementString() {
      return this.getClass().getSimpleName() + " [address=" + address + ", queue=" + queue + "]";
   }

   @Override
   public boolean isConnected() {
      return true;
   }
}
