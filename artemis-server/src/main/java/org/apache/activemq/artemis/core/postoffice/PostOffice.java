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
package org.apache.activemq.artemis.core.postoffice;

import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueCreator;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 *
 * A PostOffice instance maintains a mapping of a String address to a Queue. Multiple Queue instances can be bound
 * with the same String address.
 *
 * Given a message and an address a PostOffice instance will route that message to all the Queue instances that are
 * registered with that address.
 *
 * Addresses can be any String instance.
 *
 * A Queue instance can only be bound against a single address in the post office.
 */
public interface PostOffice extends ActiveMQComponent
{
   void addBinding(Binding binding) throws Exception;

   Binding removeBinding(SimpleString uniqueName, Transaction tx) throws Exception;

   /**
    * It will lookup the Binding without creating an item on the Queue if non-existent
    * @param address
    * @throws Exception
    */
   Bindings lookupBindingsForAddress(SimpleString address) throws Exception;

   /**
    * Differently to lookupBindings, this will always create a new element on the Queue if non-existent
    * @param address
    * @throws Exception
    */
   Bindings getBindingsForAddress(SimpleString address) throws Exception;

   Binding getBinding(SimpleString uniqueName);

   Bindings getMatchingBindings(SimpleString address) throws Exception;

   Map<SimpleString, Binding> getAllBindings();

   void route(ServerMessage message, QueueCreator queueCreator, boolean direct) throws Exception;

   void route(ServerMessage message, QueueCreator queueCreator, Transaction tx, boolean direct) throws Exception;

   void route(ServerMessage message, QueueCreator queueCreator, Transaction tx, boolean direct, boolean rejectDuplicates) throws Exception;

   void route(ServerMessage message, QueueCreator queueCreator, RoutingContext context, boolean direct) throws Exception;

   void route(ServerMessage message, QueueCreator queueCreator, RoutingContext context, boolean direct, boolean rejectDuplicates) throws Exception;

   MessageReference reroute(ServerMessage message, Queue queue, Transaction tx) throws Exception;

   Pair<RoutingContext, ServerMessage> redistribute(ServerMessage message, final Queue originatingQueue, Transaction tx) throws Exception;

   void processRoute(final ServerMessage message, final RoutingContext context, final boolean direct) throws Exception;

   DuplicateIDCache getDuplicateIDCache(SimpleString address);

   void sendQueueInfoToQueue(SimpleString queueName, SimpleString address) throws Exception;

   Object getNotificationLock();

   // we can't start expiry scanner until the system is load otherwise we may get weird races - https://issues.jboss.org/browse/HORNETQ-1142
   void startExpiryScanner();

   boolean isAddressBound(final SimpleString address) throws Exception;

   Set<SimpleString> getAddresses();

}
