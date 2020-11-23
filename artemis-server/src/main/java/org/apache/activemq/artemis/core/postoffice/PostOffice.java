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

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.mirror.MirrorController;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
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
public interface PostOffice extends ActiveMQComponent {

   /**
    * @param addressInfo
    * @return true if the address was added, false if it wasn't added
    */
   boolean addAddressInfo(AddressInfo addressInfo) throws Exception;

   default void reloadAddressInfo(AddressInfo addressInfo) throws Exception {
      addAddressInfo(addressInfo);
   }

   AddressInfo removeAddressInfo(SimpleString address) throws Exception;

   AddressInfo removeAddressInfo(SimpleString address, boolean force) throws Exception;

   AddressInfo getAddressInfo(SimpleString address);

   AddressInfo updateAddressInfo(SimpleString addressName, EnumSet<RoutingType> routingTypes) throws Exception;

   @Deprecated
   QueueBinding updateQueue(SimpleString name,
                            RoutingType routingType,
                            Filter filter,
                            Integer maxConsumers,
                            Boolean purgeOnNoConsumers,
                            Boolean exclusive,
                            Boolean groupRebalance,
                            Integer groupBuckets,
                            SimpleString groupFirstKey,
                            Boolean nonDestructive,
                            Integer consumersBeforeDispatch,
                            Long delayBeforeDispatch,
                            SimpleString user,
                            Boolean configurationManaged) throws Exception;

   @Deprecated
   QueueBinding updateQueue(SimpleString name,
                            RoutingType routingType,
                            Filter filter,
                            Integer maxConsumers,
                            Boolean purgeOnNoConsumers,
                            Boolean exclusive,
                            Boolean groupRebalance,
                            Integer groupBuckets,
                            SimpleString groupFirstKey,
                            Boolean nonDestructive,
                            Integer consumersBeforeDispatch,
                            Long delayBeforeDispatch,
                            SimpleString user,
                            Boolean configurationManaged,
                            Long ringSize) throws Exception;

   QueueBinding updateQueue(QueueConfiguration queueConfiguration) throws Exception;

   /**
    * @param queueConfiguration
    * @param forceUpdate Setting to <code>true</code> will make <code>null</code> values override current values too
    * @return
    * @throws Exception
    */
   QueueBinding updateQueue(QueueConfiguration queueConfiguration, boolean forceUpdate) throws Exception;

   List<Queue> listQueuesForAddress(SimpleString address) throws Exception;



   void addBinding(Binding binding) throws Exception;

   Binding removeBinding(SimpleString uniqueName, Transaction tx, boolean deleteData) throws Exception;

   /**
    * It will lookup the Binding without creating an item on the Queue if non-existent
    *
    * @param address
    * @throws Exception
    */
   Bindings lookupBindingsForAddress(SimpleString address) throws Exception;

   /**
    * Differently to lookupBindings, this will always create a new element on the Queue if non-existent
    *
    * @param address
    * @throws Exception
    */
   Bindings getBindingsForAddress(SimpleString address) throws Exception;

   Binding getBinding(SimpleString uniqueName);

   Collection<Binding> getMatchingBindings(SimpleString address) throws Exception;

   Collection<Binding> getDirectBindings(SimpleString address) throws Exception;

   Stream<Binding> getAllBindings();

   SimpleString getMatchingQueue(SimpleString address, RoutingType routingType) throws Exception;

   SimpleString getMatchingQueue(SimpleString address, SimpleString queueName, RoutingType routingType) throws Exception;

   RoutingStatus route(Message message, boolean direct) throws Exception;

   RoutingStatus route(Message message,
                       Transaction tx,
                       boolean direct) throws Exception;

   RoutingStatus route(Message message,
                       Transaction tx,
                       boolean direct,
                       boolean rejectDuplicates) throws Exception;

   RoutingStatus route(Message message,
                       Transaction tx,
                       boolean direct,
                       boolean rejectDuplicates,
                       Binding binding) throws Exception;

   RoutingStatus route(Message message,
                       RoutingContext context,
                       boolean direct) throws Exception;

   RoutingStatus route(Message message,
                       RoutingContext context,
                       boolean direct,
                       boolean rejectDuplicates,
                       Binding binding) throws Exception;

   /**
    * This method was renamed as reload, use the new method instead
    * @param message
    * @param queue
    * @param tx
    * @return
    * @throws Exception
    */
   @Deprecated
   default MessageReference reroute(Message message, Queue queue, Transaction tx) throws Exception {
      return reload(message, queue, tx);
   }

   MessageReference reload(Message message, Queue queue, Transaction tx) throws Exception;

   Pair<RoutingContext, Message> redistribute(Message message,
                                                    Queue originatingQueue,
                                                    Transaction tx) throws Exception;

   void processRoute(Message message, RoutingContext context, boolean direct) throws Exception;

   DuplicateIDCache getDuplicateIDCache(SimpleString address);

   void sendQueueInfoToQueue(SimpleString queueName, SimpleString address) throws Exception;

   Object getNotificationLock();

   // we can't start expiry scanner until the system is load otherwise we may get weird races - https://issues.jboss.org/browse/HORNETQ-1142
   void startExpiryScanner();

   void startAddressQueueScanner();

   boolean isAddressBound(SimpleString address) throws Exception;

   Set<SimpleString> getAddresses();

   MirrorController getMirrorControlSource();

   PostOffice setMirrorControlSource(MirrorController mirrorControllerSource);

   void postAcknowledge(MessageReference ref, AckReason reason);

   default void scanAddresses(MirrorController mirrorController) throws Exception {
   }
}
