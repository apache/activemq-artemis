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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.transaction.Transaction;

public final class RoutingContextImpl implements RoutingContext {

   // The pair here is Durable and NonDurable
   private final Map<SimpleString, RouteContextList> map = new HashMap<SimpleString, RouteContextList>();

   private Transaction transaction;

   private int queueCount;

   public RoutingContextImpl(final Transaction transaction) {
      this.transaction = transaction;
   }

   public void clear() {
      transaction = null;

      map.clear();

      queueCount = 0;
   }

   public void addQueue(final SimpleString address, final Queue queue) {

      RouteContextList listing = getContextListing(address);

      if (queue.isDurable()) {
         listing.getDurableQueues().add(queue);
      }
      else {
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

   public RouteContextList getContextListing(SimpleString address) {
      RouteContextList listing = map.get(address);
      if (listing == null) {
         listing = new ContextListing();
         map.put(address, listing);
      }
      return listing;
   }

   public Transaction getTransaction() {
      return transaction;
   }

   public void setTransaction(final Transaction tx) {
      transaction = tx;
   }

   public List<Queue> getNonDurableQueues(SimpleString address) {
      return getContextListing(address).getNonDurableQueues();
   }

   public List<Queue> getDurableQueues(SimpleString address) {
      return getContextListing(address).getDurableQueues();
   }

   public int getQueueCount() {
      return queueCount;
   }

   public Map<SimpleString, RouteContextList> getContexListing() {
      return this.map;
   }

   private static class ContextListing implements RouteContextList {

      private final List<Queue> durableQueue = new ArrayList<Queue>(1);

      private final List<Queue> nonDurableQueue = new ArrayList<Queue>(1);

      private final List<Queue> ackedQueues = new ArrayList<>();

      public int getNumberOfDurableQueues() {
         return durableQueue.size();
      }

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
