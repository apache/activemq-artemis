/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.api.core.client;

import javax.transaction.xa.XAResource;
import java.util.List;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.SimpleString;

/**
 * A ClientSession is a single-thread object required for producing and consuming messages.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface ClientSession extends XAResource, AutoCloseable
{
   /**
    * Information returned by a binding query
    *
    * @see ClientSession#addressQuery(SimpleString)
    */
   public interface AddressQuery
   {
      /**
       * Returns <code>true</code> if the binding exists, <code>false</code> else.
       */
      boolean isExists();

      /**
       * Returns the names of the queues bound to the binding.
       */
      List<SimpleString> getQueueNames();
   }

   /**
    * @deprecated Use {@link org.apache.activemq.api.core.client.ClientSession.AddressQuery} instead
    */
   @Deprecated
   public interface BindingQuery extends AddressQuery
   {

   }

   /**
    * Information returned by a queue query
    *
    * @see ClientSession#queueQuery(SimpleString)
    */
   public interface QueueQuery
   {
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
   }

   // Lifecycle operations ------------------------------------------

   /**
    * Starts the session.
    * The session must be started before ClientConsumers created by the session can consume messages from the queue.
    *
    * @throws HornetQException if an exception occurs while starting the session
    */
   ClientSession start() throws HornetQException;

   /**
    * Stops the session.
    * ClientConsumers created by the session can not consume messages when the session is stopped.
    *
    * @throws HornetQException if an exception occurs while stopping the session
    */
   void stop() throws HornetQException;

   /**
    * Closes the session.
    *
    * @throws HornetQException if an exception occurs while closing the session
    */
   void close() throws HornetQException;

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

   // Queue Operations ----------------------------------------------

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable   whether the queue is durable or not
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, SimpleString queueName, boolean durable) throws HornetQException;

   /**
    * Creates a transient queue. A queue that will exist as long as there are consumers. When the last consumer is closed the queue will be deleted
    * <p>
    * Notice: you will get an exception if the address or the filter doesn't match to an already existent queue
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable   if the queue is durable
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createSharedQueue(SimpleString address, SimpleString queueName, boolean durable) throws HornetQException;

   /**
    * Creates a transient queue. A queue that will exist as long as there are consumers. When the last consumer is closed the queue will be deleted
    * <p>
    * Notice: you will get an exception if the address or the filter doesn't match to an already existent queue
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter    whether the queue is durable or not
    * @param durable   if the queue is durable
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createSharedQueue(SimpleString address, SimpleString queueName, SimpleString filter, boolean durable) throws HornetQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable   whether the queue is durable or not
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(String address, String queueName, boolean durable) throws HornetQException;

   /**
    * Creates a <em>non-temporary</em> queue <em>non-durable</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(String address, String queueName) throws HornetQException;

   /**
    * Creates a <em>non-temporary</em> queue <em>non-durable</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, SimpleString queueName) throws HornetQException;

   /**
    * Creates a <em>non-temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @param durable   whether the queue is durable or not
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(SimpleString address, SimpleString queueName, SimpleString filter, boolean durable) throws HornetQException;

   /**
    * Creates a <em>non-temporary</em>queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param durable   whether the queue is durable or not
    * @param filter    only messages which match this filter will be put in the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createQueue(String address, String queueName, String filter, boolean durable) throws HornetQException;

   /**
    * Creates a <em>temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(SimpleString address, SimpleString queueName) throws HornetQException;

   /**
    * Creates a <em>temporary</em> queue.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(String address, String queueName) throws HornetQException;

   /**
    * Creates a <em>temporary</em> queue with a filter.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws HornetQException;

   /**
    * Creates a <em>temporary</em> queue with a filter.
    *
    * @param address   the queue will be bound to this address
    * @param queueName the name of the queue
    * @param filter    only messages which match this filter will be put in the queue
    * @throws HornetQException in an exception occurs while creating the queue
    */
   void createTemporaryQueue(String address, String queueName, String filter) throws HornetQException;

   /**
    * Deletes the queue.
    *
    * @param queueName the name of the queue to delete
    * @throws HornetQException if there is no queue for the given name or if the queue has consumers
    */
   void deleteQueue(SimpleString queueName) throws HornetQException;

   /**
    * Deletes the queue.
    *
    * @param queueName the name of the queue to delete
    * @throws HornetQException if there is no queue for the given name or if the queue has consumers
    */
   void deleteQueue(String queueName) throws HornetQException;

   // Consumer Operations -------------------------------------------

   /**
    * Creates a ClientConsumer to consume message from the queue with the given name.
    *
    * @param queueName name of the queue to consume messages from
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName) throws HornetQException;

   /**
    * Creates a ClientConsumer to consume messages from the queue with the given name.
    *
    * @param queueName name of the queue to consume messages from
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName) throws HornetQException;

   /**
    * Creates a ClientConsumer to consume messages matching the filter from the queue with the given name.
    *
    * @param queueName name of the queue to consume messages from
    * @param filter    only messages which match this filter will be consumed
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filter) throws HornetQException;

   /**
    * Creates a ClientConsumer to consume messages matching the filter from the queue with the given name.
    *
    * @param queueName name of the queue to consume messages from
    * @param filter    only messages which match this filter will be consumed
    * @return a ClientConsumer
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, String filter) throws HornetQException;

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
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName, boolean browseOnly) throws HornetQException;

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
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, boolean browseOnly) throws HornetQException;

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
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, String filter, boolean browseOnly) throws HornetQException;

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
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName, SimpleString filter, boolean browseOnly) throws HornetQException;

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
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(SimpleString queueName,
                                 SimpleString filter,
                                 int windowSize,
                                 int maxRate,
                                 boolean browseOnly) throws HornetQException;

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
    * @throws HornetQException if an exception occurs while creating the ClientConsumer
    */
   ClientConsumer createConsumer(String queueName, String filter, int windowSize, int maxRate, boolean browseOnly) throws HornetQException;

   // Producer Operations -------------------------------------------

   /**
    * Creates a producer with no default address.
    * Address must be specified every time a message is sent
    *
    * @return a ClientProducer
    * @see ClientProducer#send(SimpleString, org.apache.activemq.api.core.Message)
    */
   ClientProducer createProducer() throws HornetQException;

   /**
    * Creates a producer which sends messages to the given address
    *
    * @param address the address to send messages to
    * @return a ClientProducer
    * @throws HornetQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(SimpleString address) throws HornetQException;

   /**
    * Creates a producer which sends messages to the given address
    *
    * @param address the address to send messages to
    * @return a ClientProducer
    * @throws HornetQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(String address) throws HornetQException;

   /**
    * Creates a producer which sends messages to the given address
    *
    * @param address the address to send messages to
    * @param rate    the producer rate
    * @return a ClientProducer
    * @throws HornetQException if an exception occurs while creating the ClientProducer
    */
   ClientProducer createProducer(SimpleString address, int rate) throws HornetQException;

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
    * @throws HornetQException if an exception occurs while querying the queue
    */
   QueueQuery queueQuery(SimpleString queueName) throws HornetQException;

   /**
    * Queries information on a binding.
    *
    * @param address the address of the biding to query
    * @return a AddressQuery containing information on the binding attached to the given address
    * @throws HornetQException if an exception occurs while querying the binding
    */
   AddressQuery addressQuery(SimpleString address) throws HornetQException;

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
    * Commits the current transaction.
    *
    * @throws HornetQException if an exception occurs while committing the transaction
    */
   void commit() throws HornetQException;

   /**
    * Rolls back the current transaction.
    *
    * @throws HornetQException if an exception occurs while rolling back the transaction
    */
   void rollback() throws HornetQException;

   /**
    * Rolls back the current transaction.
    *
    * @param considerLastMessageAsDelivered the first message on deliveringMessage Buffer is considered as delivered
    * @throws HornetQException if an exception occurs while rolling back the transaction
    */
   void rollback(boolean considerLastMessageAsDelivered) throws HornetQException;

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
    * @throws HornetQException
    */
   void addMetaData(String key, String data) throws HornetQException;

   /**
    * Attach any metadata to the session. Throws an exception if there's already a metadata available.
    * You can use this metadata to ensure that there is no other session with the same meta-data you are passing as an argument.
    * This is useful to simulate unique client-ids, where you may want to avoid multiple instances of your client application connected.
    *
    * @throws HornetQException
    */
   void addUniqueMetaData(String key, String data) throws HornetQException;

   /**
    * Return the sessionFactory used to created this Session.
    *
    * @return
    */
   ClientSessionFactory getSessionFactory();
}
