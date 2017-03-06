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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.LinkedListIterator;
import org.apache.activemq.artemis.utils.ReferenceCounter;

public interface Queue extends Bindable {

   int MAX_CONSUMERS_UNLIMITED = -1;

   SimpleString getName();

   long getID();

   Filter getFilter();

   PageSubscription getPageSubscription();

   RoutingType getRoutingType();

   void setRoutingType(RoutingType routingType);

   boolean isDurable();

   boolean isTemporary();

   boolean isAutoCreated();

   boolean isPurgeOnNoConsumers();

   void setPurgeOnNoConsumers(boolean value);

   int getMaxConsumers();

   void setMaxConsumer(int maxConsumers);

   void addConsumer(Consumer consumer) throws Exception;

   void removeConsumer(Consumer consumer);

   int getConsumerCount();

   /**
    * This will set a reference counter for every consumer present on the queue.
    * The ReferenceCounter will know what to do when the counter became zeroed.
    * This is used to control what to do with temporary queues, especially
    * on shared subscriptions where the queue needs to be deleted when all the
    * consumers are closed.
    */
   void setConsumersRefCount(ReferenceCounter referenceCounter);

   ReferenceCounter getConsumersRefCount();

   void reload(MessageReference ref);

   void addTail(MessageReference ref);

   void addTail(MessageReference ref, boolean direct);

   void addHead(MessageReference ref, boolean scheduling);

   void addHead(final List<MessageReference> refs, boolean scheduling);

   void acknowledge(MessageReference ref) throws Exception;

   void acknowledge(final MessageReference ref, AckReason reason) throws Exception;

   void acknowledge(Transaction tx, MessageReference ref) throws Exception;

   void acknowledge(final Transaction tx, final MessageReference ref, AckReason reason) throws Exception;

   void reacknowledge(Transaction tx, MessageReference ref) throws Exception;

   void cancel(Transaction tx, MessageReference ref);

   void cancel(Transaction tx, MessageReference ref, boolean ignoreRedeliveryCheck);

   void cancel(MessageReference reference, long timeBase) throws Exception;

   void deliverAsync();

   void unproposed(SimpleString groupID);

   /**
    * This method will make sure that any pending message (including paged message) will be delivered
    */
   void forceDelivery();

   void deleteQueue() throws Exception;

   void deleteQueue(boolean removeConsumers) throws Exception;

   void destroyPaging() throws Exception;

   long getMessageCount();

   int getDeliveringCount();

   void referenceHandled();

   int getScheduledCount();

   List<MessageReference> getScheduledMessages();

   /**
    * Return a Map consisting of consumer.toString and its messages
    * Delivering message is a property of the consumer, this method will aggregate the results per Server's consumer object
    *
    * @return
    */
   Map<String, List<MessageReference>> getDeliveringMessages();

   long getMessagesAdded();

   long getMessagesAcknowledged();

   long getMessagesExpired();

   long getMessagesKilled();

   MessageReference removeReferenceWithID(long id) throws Exception;

   MessageReference getReference(long id) throws ActiveMQException;

   int deleteAllReferences() throws Exception;

   int deleteAllReferences(final int flushLimit) throws Exception;

   boolean deleteReference(long messageID) throws Exception;

   int deleteMatchingReferences(Filter filter) throws Exception;

   int deleteMatchingReferences(int flushLImit, Filter filter) throws Exception;

   boolean expireReference(long messageID) throws Exception;

   /**
    * Expire all the references in the queue which matches the filter
    */
   int expireReferences(Filter filter) throws Exception;

   void expireReferences() throws Exception;

   void expire(MessageReference ref) throws Exception;

   boolean sendMessageToDeadLetterAddress(long messageID) throws Exception;

   int sendMessagesToDeadLetterAddress(Filter filter) throws Exception;

   void sendToDeadLetterAddress(final Transaction tx, final MessageReference ref) throws Exception;

   boolean changeReferencePriority(long messageID, byte newPriority) throws Exception;

   int changeReferencesPriority(Filter filter, byte newPriority) throws Exception;

   boolean moveReference(long messageID, SimpleString toAddress) throws Exception;

   boolean moveReference(long messageID, SimpleString toAddress, boolean rejectDuplicates) throws Exception;

   int moveReferences(Filter filter, SimpleString toAddress) throws Exception;

   int moveReferences(final int flushLimit,
                      Filter filter,
                      SimpleString toAddress,
                      boolean rejectDuplicates) throws Exception;

   int retryMessages(Filter filter) throws Exception;

   void addRedistributor(long delay);

   void cancelRedistributor() throws Exception;

   boolean hasMatchingConsumer(Message message);

   Collection<Consumer> getConsumers();

   boolean checkRedelivery(MessageReference ref, long timeBase, boolean ignoreRedeliveryDelay) throws Exception;

   /**
    * It will iterate thorugh memory only (not paging)
    *
    * @return
    */
   LinkedListIterator<MessageReference> iterator();

   LinkedListIterator<MessageReference> browserIterator();

   SimpleString getExpiryAddress();

   /**
    * Pauses the queue. It will receive messages but won't give them to the consumers until resumed.
    * If a queue is paused, pausing it again will only throw a warning.
    * To check if a queue is paused, invoke <i>isPaused()</i>
    */
   void pause();

   /**
    * Pauses the queue. It will receive messages but won't give them to the consumers until resumed.
    * If a queue is paused, pausing it again will only throw a warning.
    * To check if a queue is paused, invoke <i>isPaused()</i>
    */
   void pause(boolean persist);

   void reloadPause(long recordID);

   /**
    * Resumes the delivery of message for the queue.
    * If a queue is resumed, resuming it again will only throw a warning.
    * To check if a queue is resumed, invoke <i>isPaused()</i>
    */
   void resume();

   /**
    * @return true if paused, false otherwise.
    */
   boolean isPaused();

   /**
    * if the pause was persisted
    *
    * @return
    */
   boolean isPersistedPause();

   Executor getExecutor();

   void resetAllIterators();

   boolean flushExecutor();

   void close() throws Exception;

   boolean isDirectDeliver();

   SimpleString getAddress();

   /**
    * We can't send stuff to DLQ on queues used on clustered-bridge-communication
    *
    * @return
    */
   boolean isInternalQueue();

   void setInternalQueue(boolean internalQueue);

   void resetMessagesAdded();

   void resetMessagesAcknowledged();

   void resetMessagesExpired();

   void resetMessagesKilled();

   void incrementMesssagesAdded();

   /**
    * cancels scheduled messages and send them to the head of the queue.
    */
   void deliverScheduledMessages() throws ActiveMQException;

   void postAcknowledge(MessageReference ref);

   float getRate();

   /**
    * @return the user who created this queue
    */
   SimpleString getUser();

   void decDelivering(int size);

}
