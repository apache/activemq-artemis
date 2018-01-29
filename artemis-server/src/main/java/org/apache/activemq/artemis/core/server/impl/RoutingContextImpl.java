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
package org.apache.activemq.artemis.core.server.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.transaction.Transaction;

public final class RoutingContextImpl implements RoutingContext {

   // The pair here is Durable and NonDurable
   private final Map<SimpleString, RouteContextList> map = new HashMap<>();

   private Transaction transaction;

   private int queueCount;

   private SimpleString address;

   private RoutingType routingType;

   public RoutingContextImpl(final Transaction transaction) {
      this.transaction = transaction;
   }

   @Override
   public void clear() {
      transaction = null;

      map.clear();

      queueCount = 0;
   }

   @Override
   public void addQueue(final SimpleString address, final Queue queue) {

      RouteContextList listing = getContextListing(address);

      if (queue.isDurableMessage()) {
         listing.getDurableQueues().add(queue);
      } else {
         listing.getNonDurableQueues().add(queue);
      }

      queueCount++;
   }

   @Override
   public void addQueueWithAck(SimpleString address, Queue queue) {
      addQueue(address, queue);
      RouteContextList listing = getContextListing(address);
      listing.addAckedQueue(queue);
   }

   @Override
   public boolean isAlreadyAcked(SimpleString address, Queue queue) {
      RouteContextList listing = map.get(address);
      return listing == null ? false : listing.isAlreadyAcked(queue);
   }

   @Override
   public void setAddress(SimpleString address) {
      this.address = address;
   }

   @Override
   public void setRoutingType(RoutingType routingType) {
      this.routingType = routingType;
   }

   @Override
   public SimpleString getAddress(Message message) {
      if (address == null && message != null) {
         return message.getAddressSimpleString();
      }
      return address;
   }

   @Override
   public RoutingType getRoutingType() {
      return routingType;
   }

   @Override
   public RouteContextList getContextListing(SimpleString address) {
      RouteContextList listing = map.get(address);
      if (listing == null) {
         listing = new ContextListing();
         map.put(address, listing);
      }
      return listing;
   }

   @Override
   public Transaction getTransaction() {
      return transaction;
   }

   @Override
   public void setTransaction(final Transaction tx) {
      transaction = tx;
   }

   @Override
   public List<Queue> getNonDurableQueues(SimpleString address) {
      return getContextListing(address).getNonDurableQueues();
   }

   @Override
   public List<Queue> getDurableQueues(SimpleString address) {
      return getContextListing(address).getDurableQueues();
   }

   @Override
   public int getQueueCount() {
      return queueCount;
   }

   @Override
   public Map<SimpleString, RouteContextList> getContexListing() {
      return this.map;
   }

   private static class ContextListing implements RouteContextList {

      private final List<Queue> durableQueue = new ArrayList<>(1);

      private final List<Queue> nonDurableQueue = new ArrayList<>(1);

      private final List<Queue> ackedQueues = new ArrayList<>();

      @Override
      public int getNumberOfDurableQueues() {
         return durableQueue.size();
      }

      @Override
      public int getNumberOfNonDurableQueues() {
         return nonDurableQueue.size();
      }

      @Override
      public List<Queue> getDurableQueues() {
         return durableQueue;
      }

      @Override
      public List<Queue> getNonDurableQueues() {
         return nonDurableQueue;
      }

      @Override
      public void addAckedQueue(Queue queue) {
         ackedQueues.add(queue);
      }

      @Override
      public boolean isAlreadyAcked(Queue queue) {
         return ackedQueues.contains(queue);
      }
   }

}
