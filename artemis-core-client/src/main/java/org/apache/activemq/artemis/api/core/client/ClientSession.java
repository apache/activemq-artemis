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
package org.apache.activemq.artemis.api.core.client;

import javax.transaction.xa.XAResource;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.RoutingType;

/**
 * A ClientSession is a single-thread object required for producing and consuming messages.
 */
public interface ClientSession extends XAResource, AutoCloseable {

   /**
    * This is used to identify a ClientSession as used by the JMS Layer
    * The JMS Layer will add this through Meta-data, so the server or management layers
    * can identify session created over core API purely or through the JMS Layer
    */
   String JMS_SESSION_IDENTIFIER_PROPERTY = "jms-session";

   /**
    * Just like {@link ClientSession.AddressQuery#JMS_SESSION_IDENTIFIER_PROPERTY} this is
    * used to identify the ClientID over JMS Session.
    * However this is only used when the JMS Session.clientID is set (which is optional).
    * With this property management tools and the server can identify the jms-client-id used over JMS
    */
   String JMS_SESSION_CLIENT_ID_PROPERTY = "jms-client-id";

   /**
    * Information returned by a binding query
    *
    * @see ClientSession#addressQuery(SimpleString)
    */
   public interface AddressQuery {

      /**
       * Returns <code>true</code> if the binding exists, <code>false</code> else.
       */
      boolean isExists();

      /**
       * Returns the names of the queues bound to the binding.
       */
      List<SimpleString> getQueueNames();

      /**
       * Returns <code>true</code> if auto-queue-creation for this address is enabled, <code>false</code> else.
       */
      boolean isAutoCreateQueues();

      /**
       * Returns <code>true</code> if auto-address-creation for this address is enabled, <code>false</code> else.
       */
      boolean isAutoCreateAddresses();

      boolean isDefaultPurgeOnNoConsumers();

      int getDefaultMaxConsumers();

      Boolean isDefaultLastValueQueue();

      Boolean isDefaultExclusive();
   }

   /**
    * Information returned by a queue query
    *
    * @see ClientSession#queueQuery(SimpleString)
    */
   public interface QueueQuery {

      /**
       * Returns <code>true</code> if the queue exists, <code>false</code> else.
       */
      boolean isExists();

      /**
       * Return <code>true</code> if the queue is temporary, <code>false</code> else.
       */
      boolean isTemporary();

      /**
       * Returns <code>true</code> if the queue is durable, <code>false</code> else.
       */
      boolean isDurable();

      /**
       * Returns <code>true</code> if auto-creation for this queue is enabled and if the queue queried is a JMS queue,
       * <code>false</code> else.
       */
      boolean isAutoCreateQueues();

      /**
       * Returns the number of consumers attached to the queue.
       */
      int getConsumerCount();

      /**
       * Returns the number of messages in the queue.
       */
      long getMessageCount();

      /**
       * Returns the queue's filter string (or {@code null} if the queue has no filter).
       */
      SimpleString getFilterString();

      /**
       * Returns the address that the queue is bound to.
       */
      SimpleString getAddress();

      /**
       * Return the name of the queue
       *
       * @return
       */
      SimpleString getName();

      RoutingType getRoutingType();

      int getMaxConsumers();

      boolean isPurgeOnNoConsumers();

      boolean isAutoCreated();

      Boolean isExclusive();

      Boolean isLastValue();
   }

   // Lifecycle operations ------------------------------------------

   /**
    * Starts the session.
    * The session must be started before ClientConsumers created by the session can consume messages from the queue.
    *
    * @throws ActiveMQException if an exception occurs while starting the session
    */
   ClientSession start() throws ActiveMQException;

   /**
    * Stops the session.
    * ClientConsumers created by the session can not consume messages when the session is stopped.
    *
    * @throws ActiveMQException if an exception occurs while stopping the session
    */
   void stop() throws ActiveMQException;

   /**
    * Closes the session.
    *
    * @throws ActiveMQException if an exception occurs while closing the session
    */
   @Override
   void close() throws ActiveMQException;

   /**
    * Returns whether the session is closed or not.
    *
    * @return <code>true</code> if the session is closed, <code>false</code> else
    */
   boolean isClosed();

   /**
    * Adds a FailureListener to the session which is notified if a failure occurs on the session.
    *
    * @param listener the listener to add
    */
   void addFailureListener(SessionFailureListener listener);

   /**
    * Removes a FailureListener to the session.
    *
    * @param listener the listener to remove
    * @return <code>true</code> if the listener was removed, <code>false</code> else
    */
   boolean removeFailureListener(SessionFailureListener listener);

   /**
    * Adds a FailoverEventListener to the session which is notified if a failover event  occurs on the session.
    *
    * @param listener the listener to add
    */
   void addFailoverListener(FailoverEventListener listener);

   /**
    * Removes a FailoverEventListener to the session.
    *
    * @param listener the listener to remove
    * @return <code>true</code> if the listener was removed, <code>false</code> else
    */
   boolean removeFailoverListener(FailoverEventListener listener);

   /**
    * Returns the server's incrementingVersion.
    *
    * @return the server's <code>incrementingVersion</code>
    */
   int getVersion();

   /**
    * Create Address with a single initial routing type
    * @param address
    * @param autoCreated
    * @throws ActiveMQException
    */
   void createAddress(SimpleString address, EnumSet<RoutingType> routingTypes, boolean autoCreated) throws ActiveMQException;

   /**
    * Create Address with a single initial routing type
    * @param address
    * @param autoCreated
    * @throws ActiveMQException
    */
   @Deprecated
   void createAddress(SimpleString address, Set<RoutingType> routingTypes, boolean autoCreated) throws ActiveMQException;

   /**
    * Create Address with a single initial routing type
    * @param address
    * @param routingType
    * @param autoCreated
    * @throws ActiveMQException
    */
   void createAddress(SimpleString address, RoutingType routingType, boolean autoCreated) throws ActiveMQException;

   // Queue Operations ----------------------------------------------

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable   whether the queue is durable or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createQueue(SimpleString address, SimpleString queueName, boolean durable) throws ActiveMQException;

   /**
    * Creates a transient queue. A queue that will exist as long as there are consumers. When the last consumer is closed the queue will be deleted
    * <p>
    * Notice: you will get an exception if the address or the filter doesn't match to an already existent queue
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable   if the queue is durable
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createSharedQueue(SimpleString address, SimpleString queueName, boolean durable) throws ActiveMQException;

   /**
    * Creates a transient queue. A queue that will exist as long as there are consumers. When the last consumer is closed the queue will be deleted
    * <p>
    * Notice: you will get an exception if the address or the filter doesn't match to an already existent queue
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter    whether the queue is durable or not
    * @param durable   if the queue is durable
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createSharedQueue(SimpleString address,
                          SimpleString queueName,
                          SimpleString filter,
                          boolean durable) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable   whether the queue is durable or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createQueue(String address, String queueName, boolean durable) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue <em>non-durable</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createQueue(String address, String queueName) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue <em>non-durable</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createQueue(SimpleString address, SimpleString queueName) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @param durable   whether the queue is durable or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createQueue(SimpleString address,
                    SimpleString queueName,
                    SimpleString filter,
                    boolean durable) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em>queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable   whether the queue is durable or not
    * @param filter    only messages which match this filter will be put in the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createQueue(String address, String queueName, String filter, boolean durable) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address     the queue will be bound to this address
    * @param queueName   the name of the queue
    * @param filter      only messages which match this filter will be put in the queue
    * @param durable     whether the queue is durable or not
    * @param autoCreated whether to mark this queue as autoCreated or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createQueue(SimpleString address,
                    SimpleString queueName,
                    SimpleString filter,
                    boolean durable,
                    boolean autoCreated) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em>queue.
    *
    * @param address     the queue will be bound to this address
    * @param queueName   the name of the queue
    * @param filter      only messages which match this filter will be put in the queue
    * @param durable     whether the queue is durable or not
    * @param autoCreated whether to mark this queue as autoCreated or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createQueue(String address, String queueName, String filter, boolean durable, boolean autoCreated) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createTemporaryQueue(SimpleString address, SimpleString queueName) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createTemporaryQueue(String address, String queueName) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue with a filter.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createTemporaryQueue(SimpleString address,
                             SimpleString queueName,
                             SimpleString filter) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue with a filter.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   @Deprecated
   void createTemporaryQueue(String address, String queueName, String filter) throws ActiveMQException;

   /** Deprecate **/


   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param durable   whether the queue is durable or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, boolean durable) throws ActiveMQException;

   /**
    * Creates a transient queue. A queue that will exist as long as there are consumers. When the last consumer is closed the queue will be deleted
    * <p>
    * Notice: you will get an exception if the address or the filter doesn't match to an already existent queue
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param durable   if the queue is durable
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createSharedQueue(SimpleString address, RoutingType routingType, SimpleString queueName, boolean durable) throws ActiveMQException;

   /**
    * Creates a transient queue. A queue that will exist as long as there are consumers. When the last consumer is closed the queue will be deleted
    * <p>
    * Notice: you will get an exception if the address or the filter doesn't match to an already existent queue
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param filter    whether the queue is durable or not
    * @param durable   if the queue is durable
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createSharedQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                          boolean durable) throws ActiveMQException;

   /**
    * Creates Shared queue. A queue that will exist as long as there are consumers or is durable.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param filter    whether the queue is durable or not
    * @param durable   if the queue is durable
    * @param maxConsumers how many concurrent consumers will be allowed on this queue
    * @param purgeOnNoConsumers whether to delete the contents of the queue when the last consumer disconnects
    * @param exclusive    if the queue is exclusive queue
    * @param lastValue    if the queue is last value queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createSharedQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                          boolean durable, Integer maxConsumers, Boolean purgeOnNoConsumers, Boolean exclusive, Boolean lastValue) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param durable   whether the queue is durable or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createQueue(String address, RoutingType routingType, String queueName, boolean durable) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue <em>non-durable</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createQueue(String address, RoutingType routingType, String queueName) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue <em>non-durable</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, RoutingType routingType, SimpleString queueName) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @param durable   whether the queue is durable or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                    boolean durable) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em>queue.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @param durable   whether the queue is durable or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createQueue(String address, RoutingType routingType, String queueName, String filter, boolean durable) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address     the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName   the name of the queue
    * @param filter      only messages which match this filter will be put in the queue
    * @param durable     whether the queue is durable or not
    * @param autoCreated whether to mark this queue as autoCreated or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                    boolean durable, boolean autoCreated) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address      the queue will be bound to this address
    * @param routingType  the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName    the name of the queue
    * @param filter       only messages which match this filter will be put in the queue
    * @param durable      whether the queue is durable or not
    * @param autoCreated  whether to mark this queue as autoCreated or not
    * @param maxConsumers how many concurrent consumers will be allowed on this queue
    * @param purgeOnNoConsumers whether to delete the contents of the queue when the last consumer disconnects
    * @throws ActiveMQException
    */
   void createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                    boolean durable, boolean autoCreated, int maxConsumers, boolean purgeOnNoConsumers) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address      the queue will be bound to this address
    * @param routingType  the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName    the name of the queue
    * @param filter       only messages which match this filter will be put in the queue
    * @param durable      whether the queue is durable or not
    * @param autoCreated  whether to mark this queue as autoCreated or not
    * @param maxConsumers how many concurrent consumers will be allowed on this queue
    * @param purgeOnNoConsumers whether to delete the contents of the queue when the last consumer disconnects
    * @param exclusive whether the queue should be exclusive
    * @param lastValue whether the queue should be lastValue
    * @throws ActiveMQException
    */
   void createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                    boolean durable, boolean autoCreated, int maxConsumers, boolean purgeOnNoConsumers, Boolean exclusive, Boolean lastValue) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em>queue.
    *
    * @param address     the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName   the name of the queue
    * @param filter      only messages which match this filter will be put in the queue
    * @param durable     whether the queue is durable or not
    * @param autoCreated whether to mark this queue as autoCreated or not
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createQueue(String address, RoutingType routingType, String queueName, String filter, boolean durable, boolean autoCreated) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em>queue.
    *
    * @param address     the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName   the name of the queue
    * @param filter      only messages which match this filter will be put in the queue
    * @param durable     whether the queue is durable or not
    * @param autoCreated whether to mark this queue as autoCreated or not
    * @param maxConsumers how many concurrent consumers will be allowed on this queue
    * @param purgeOnNoConsumers whether to delete the contents of the queue when the last consumer disconnects
    * @throws ActiveMQException
    */
   void createQueue(String address, RoutingType routingType, String queueName, String filter, boolean durable, boolean autoCreated,
                           int maxConsumers, boolean purgeOnNoConsumers) throws ActiveMQException;

   /**
    * Creates a <em>non-temporary</em>queue.
    *
    * @param address     the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName   the name of the queue
    * @param filter      only messages which match this filter will be put in the queue
    * @param durable     whether the queue is durable or not
    * @param autoCreated whether to mark this queue as autoCreated or not
    * @param maxConsumers how many concurrent consumers will be allowed on this queue
    * @param purgeOnNoConsumers whether to delete the contents of the queue when the last consumer disconnects
    * @param exclusive whether the queue should be exclusive
    * @param lastValue whether the queue should be lastValue
    * @throws ActiveMQException
    */
   void createQueue(String address, RoutingType routingType, String queueName, String filter, boolean durable, boolean autoCreated,
                    int maxConsumers, boolean purgeOnNoConsumers, Boolean exclusive, Boolean lastValue) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(SimpleString address, RoutingType routingType, SimpleString queueName) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(String address, RoutingType routingType, String queueName) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue with a filter.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @param maxConsumers how many concurrent consumers will be allowed on this queue
    * @param purgeOnNoConsumers whether to delete the contents of the queue when the last consumer disconnects
    * @param exclusive    if the queue is exclusive queue
    * @param lastValue    if the queue is last value queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter, int maxConsumers,
                             boolean purgeOnNoConsumers, Boolean exclusive, Boolean lastValue) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue with a filter.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter) throws ActiveMQException;

   /**
    * Creates a <em>temporary</em> queue with a filter.
    *
    * @param address   the queue will be bound to this address
    * @param routingType the delivery mode for this queue, MULTICAST or ANYCAST
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @throws ActiveMQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(String address, RoutingType routingType, String queueName, String filter) throws ActiveMQException;

   /**
    * Deletes the queue.
    *
    * @param queueName the name of the queue to delete
    * @throws ActiveMQException if there is no queue for the given name or if the queue has consumers
    */
   void deleteQueue(SimpleString queueName) throws ActiveMQException;

   /**
    * Deletes the queue.
    *
    * @param queueName the name of the queue to delete
    * @throws ActiveMQException if there is no queue for the given name or if the queue has consumers
    */
   void deleteQueue(String queueName) throws ActiveMQException;

   // Consumer Operations -------------------------------------------

   /**
    * Creates a ClientConsumer to consume message from the queue with the given name.
    *
    * @param queueName name of the queue to consume messages from
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume messages from the queue with the given name.
    *
    * @param queueName name of the queue to consume messages from
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume messages matching the filter from the queue with the given name.
    *
    * @param queueName name of the queue to consume messages from
    * @param filter    only messages which match this filter will be consumed
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filter) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume messages matching the filter from the queue with the given name.
    *
    * @param queueName name of the queue to consume messages from
    * @param filter    only messages which match this filter will be consumed
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, String filter) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume or browse messages from the queue with the given name.
    * <p>
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages
    * from the queue but they will not be consumed (the messages will remain in the queue). Note
    * that paged messages will not be in the queue, and will therefore not be visible if
    * {@code browseOnly} is {@code true}.
    * <p>
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume
    * the messages from the queue and the messages will effectively be removed from the queue.
    *
    * @param queueName  name of the queue to consume messages from
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName, boolean browseOnly) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume or browse messages from the queue with the given name.
    * <p>
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages
    * from the queue but they will not be consumed (the messages will remain in the queue). Note
    * that paged messages will not be in the queue, and will therefore not be visible if
    * {@code browseOnly} is {@code true}.
    * <p>
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume
    * the messages from the queue and the messages will effectively be removed from the queue.
    *
    * @param queueName  name of the queue to consume messages from
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, boolean browseOnly) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume or browse messages matching the filter from the queue with
    * the given name.
    * <p>
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages
    * from the queue but they will not be consumed (the messages will remain in the queue). Note
    * that paged messages will not be in the queue, and will therefore not be visible if
    * {@code browseOnly} is {@code true}.
    * <p>
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume
    * the messages from the queue and the messages will effectively be removed from the queue.
    *
    * @param queueName  name of the queue to consume messages from
    * @param filter     only messages which match this filter will be consumed
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, String filter, boolean browseOnly) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume or browse messages matching the filter from the queue with
    * the given name.
    * <p>
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages
    * from the queue but they will not be consumed (the messages will remain in the queue). Note
    * that paged messages will not be in the queue, and will therefore not be visible if
    * {@code browseOnly} is {@code true}.
    * <p>
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume
    * the messages from the queue and the messages will effectively be removed from the queue.
    *
    * @param queueName  name of the queue to consume messages from
    * @param filter     only messages which match this filter will be consumed
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName,
                                 SimpleString filter,
                                 boolean browseOnly) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume or browse messages matching the filter from the queue with
    * the given name.
    * <p>
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages
    * from the queue but they will not be consumed (the messages will remain in the queue). Note
    * that paged messages will not be in the queue, and will therefore not be visible if
    * {@code browseOnly} is {@code true}.
    * <p>
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume
    * the messages from the queue and the messages will effectively be removed from the queue.
    *
    * @param queueName  name of the queue to consume messages from
    * @param filter     only messages which match this filter will be consumed
    * @param windowSize the consumer window size
    * @param maxRate    the maximum rate to consume messages
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName,
                                 SimpleString filter,
                                 int windowSize,
                                 int maxRate,
                                 boolean browseOnly) throws ActiveMQException;

   /**
    * Creates a ClientConsumer to consume or browse messages matching the filter from the queue with
    * the given name.
    * <p>
    * If <code>browseOnly</code> is <code>true</code>, the ClientConsumer will receive the messages
    * from the queue but they will not be consumed (the messages will remain in the queue). Note
    * that paged messages will not be in the queue, and will therefore not be visible if
    * {@code browseOnly} is {@code true}.
    * <p>
    * If <code>browseOnly</code> is <code>false</code>, the ClientConsumer will behave like consume
    * the messages from the queue and the messages will effectively be removed from the queue.
    *
    * @param queueName  name of the queue to consume messages from
    * @param filter     only messages which match this filter will be consumed
    * @param windowSize the consumer window size
    * @param maxRate    the maximum rate to consume messages
    * @param browseOnly whether the ClientConsumer will only browse the queue or consume messages.
    * @return a ClientConsumer
    * @throws ActiveMQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName,
                                 String filter,
                                 int windowSize,
                                 int maxRate,
                                 boolean browseOnly) throws ActiveMQException;

   // Producer Operations -------------------------------------------

   /**
    * Creates a producer with no default address.
    * Address must be specified every time a message is sent
    *
    * @return a ClientProducer
    * @see ClientProducer#send(SimpleString, org.apache.activemq.artemis.api.core.Message)
    */
   ClientProducer createProducer() throws ActiveMQException;

   /**
    * Creates a producer which sends messages to the given address
    *
    * @param address the address to send messages to
    * @return a ClientProducer
    * @throws ActiveMQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(SimpleString address) throws ActiveMQException;

   /**
    * Creates a producer which sends messages to the given address
    *
    * @param address the address to send messages to
    * @return a ClientProducer
    * @throws ActiveMQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(String address) throws ActiveMQException;

   /**
    * Creates a producer which sends messages to the given address
    *
    * @param address the address to send messages to
    * @param rate    the producer rate
    * @return a ClientProducer
    * @throws ActiveMQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(SimpleString address, int rate) throws ActiveMQException;

   // Message operations --------------------------------------------

   /**
    * Creates a ClientMessage.
    *
    * @param durable whether the created message is durable or not
    * @return a ClientMessage
    */
   ClientMessage createMessage(boolean durable);

   /**
    * Creates a ClientMessage.
    *
    * @param type    type of the message
    * @param durable whether the created message is durable or not
    * @return a ClientMessage
    */
   ClientMessage createMessage(byte type, boolean durable);

   /**
    * Creates a ClientMessage.
    *
    * @param type       type of the message
    * @param durable    whether the created message is durable or not
    * @param expiration the message expiration
    * @param timestamp  the message timestamp
    * @param priority   the message priority (between 0 and 9 inclusive)
    * @return a ClientMessage
    */
   ClientMessage createMessage(byte type, boolean durable, long expiration, long timestamp, byte priority);

   // Query operations ----------------------------------------------

   /**
    * Queries information on a queue.
    *
    * @param queueName the name of the queue to query
    * @return a QueueQuery containing information on the given queue
    * @throws ActiveMQException if an exception occurs while querying the queue
    */
   QueueQuery queueQuery(SimpleString queueName) throws ActiveMQException;

   /**
    * Queries information on a binding.
    *
    * @param address the address of the biding to query
    * @return an AddressQuery containing information on the binding attached to the given address
    * @throws ActiveMQException if an exception occurs while querying the binding
    */
   AddressQuery addressQuery(SimpleString address) throws ActiveMQException;

   // Transaction operations ----------------------------------------

   /**
    * Returns the XAResource associated to the session.
    *
    * @return the XAResource associated to the session
    */
   XAResource getXAResource();

   /**
    * Return <code>true</code> if the session supports XA, <code>false</code> else.
    *
    * @return <code>true</code> if the session supports XA, <code>false</code> else.
    */
   boolean isXA();

   /**
    * Commits the current transaction, blocking.
    *
    * @throws ActiveMQException if an exception occurs while committing the transaction
    */
   void commit() throws ActiveMQException;

   /**
    * Commits the current transaction.
    *
    * @param block if the commit will be blocking or not.
    * @throws ActiveMQException if an exception occurs while committing the transaction
    */
   void commit(boolean block) throws ActiveMQException;

   /**
    * Rolls back the current transaction.
    *
    * @throws ActiveMQException if an exception occurs while rolling back the transaction
    */
   void rollback() throws ActiveMQException;

   /**
    * Rolls back the current transaction.
    *
    * @param considerLastMessageAsDelivered the first message on deliveringMessage Buffer is considered as delivered
    * @throws ActiveMQException if an exception occurs while rolling back the transaction
    */
   void rollback(boolean considerLastMessageAsDelivered) throws ActiveMQException;

   /**
    * Returns <code>true</code> if the current transaction has been flagged to rollback, <code>false</code> else.
    *
    * @return <code>true</code> if the current transaction has been flagged to rollback, <code>false</code> else.
    */
   boolean isRollbackOnly();

   /**
    * Returns whether the session will <em>automatically</em> commit its transaction every time a message is sent
    * by a ClientProducer created by this session, <code>false</code> else
    *
    * @return <code>true</code> if the session <em>automatically</em> commit its transaction every time a message is sent, <code>false</code> else
    */
   boolean isAutoCommitSends();

   /**
    * Returns whether the session will <em>automatically</em> commit its transaction every time a message is acknowledged
    * by a ClientConsumer created by this session, <code>false</code> else
    *
    * @return <code>true</code> if the session <em>automatically</em> commit its transaction every time a message is acknowledged, <code>false</code> else
    */
   boolean isAutoCommitAcks();

   /**
    * Returns whether the ClientConsumer created by the session will <em>block</em> when they acknowledge a message.
    *
    * @return <code>true</code> if the session's ClientConsumer block when they acknowledge a message, <code>false</code> else
    */
   boolean isBlockOnAcknowledge();

   /**
    * Sets a <code>SendAcknowledgementHandler</code> for this session.
    *
    * @param handler a SendAcknowledgementHandler
    * @return this ClientSession
    */
   ClientSession setSendAcknowledgementHandler(SendAcknowledgementHandler handler);

   /**
    * Attach any metadata to the session.
    *
    * @throws ActiveMQException
    */
   void addMetaData(String key, String data) throws ActiveMQException;

   /**
    * Attach any metadata to the session. Throws an exception if there's already a metadata available.
    * You can use this metadata to ensure that there is no other session with the same meta-data you are passing as an argument.
    * This is useful to simulate unique client-ids, where you may want to avoid multiple instances of your client application connected.
    *
    * @throws ActiveMQException
    */
   void addUniqueMetaData(String key, String data) throws ActiveMQException;

   /**
    * Return the sessionFactory used to created this Session.
    *
    * @return
    */
   ClientSessionFactory getSessionFactory();
}
