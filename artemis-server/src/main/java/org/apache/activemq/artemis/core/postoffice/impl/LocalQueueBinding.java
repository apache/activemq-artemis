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
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.Bindable;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerMessage;

public class LocalQueueBinding implements QueueBinding {

   private final SimpleString address;

   private final Queue queue;

   private final Filter filter;

   private final SimpleString name;

   private final SimpleString clusterName;

   public LocalQueueBinding(final SimpleString address, final Queue queue, final SimpleString nodeID) {
      this.address = address;

      this.queue = queue;

      filter = queue.getFilter();

      name = queue.getName();

      clusterName = name.concat(nodeID);
   }

   public long getID() {
      return queue.getID();
   }

   public Filter getFilter() {
      return filter;
   }

   public SimpleString getAddress() {
      return address;
   }

   public Bindable getBindable() {
      return queue;
   }

   public Queue getQueue() {
      return queue;
   }

   public SimpleString getRoutingName() {
      return name;
   }

   public SimpleString getUniqueName() {
      return name;
   }

   public SimpleString getClusterName() {
      return clusterName;
   }

   public boolean isExclusive() {
      return false;
   }

   public int getDistance() {
      return 0;
   }

   public boolean isHighAcceptPriority(final ServerMessage message) {
      // It's a high accept priority if the queue has at least one matching consumer

      return queue.hasMatchingConsumer(message);
   }

   @Override
   public void unproposed(SimpleString groupID) {
      queue.unproposed(groupID);
   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception {
      queue.route(message, context);
   }

   public void routeWithAck(ServerMessage message, RoutingContext context) throws Exception {
      queue.routeWithAck(message, context);
   }

   public boolean isQueueBinding() {
      return true;
   }

   public int consumerCount() {
      return queue.getConsumerCount();
   }

   public BindingType getType() {
      return BindingType.LOCAL_QUEUE;
   }

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
