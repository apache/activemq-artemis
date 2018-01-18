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
package org.apache.activemq.artemis.core.server;

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.transaction.Transaction;

public interface RoutingContext {

   Transaction getTransaction();

   void setTransaction(Transaction transaction);

   void addQueue(SimpleString address, Queue queue);

   Map<SimpleString, RouteContextList> getContexListing();

   RouteContextList getContextListing(SimpleString address);

   List<Queue> getNonDurableQueues(SimpleString address);

   List<Queue> getDurableQueues(SimpleString address);

   int getQueueCount();

   void clear();

   void addQueueWithAck(SimpleString address, Queue queue);

   boolean isAlreadyAcked(SimpleString address, Queue queue);

   void setAddress(SimpleString address);

   void setRoutingType(RoutingType routingType);

   SimpleString getAddress(Message message);

   RoutingType getRoutingType();
}
