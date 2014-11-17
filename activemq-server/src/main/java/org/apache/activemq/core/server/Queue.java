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
package org.apache.activemq6.core.server;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.filter.Filter;
import org.apache.activemq6.core.paging.cursor.PageSubscription;
import org.apache.activemq6.core.transaction.Transaction;
import org.apache.activemq6.utils.LinkedListIterator;
import org.apache.activemq6.utils.ReferenceCounter;

/**
 * A Queue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public interface Queue extends Bindable
{
   SimpleString getName();

   long getID();

   Filter getFilter();

   PageSubscription getPageSubscription();

   boolean isDurable();

   boolean isTemporary();

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
   void setConsumersRefCount(HornetQServer server);

   ReferenceCounter getConsumersRefCount();

   void reload(MessageReference ref);

   void addTail(MessageReference ref);

   void addTail(MessageReference ref, boolean direct);

   void addHead(MessageReference ref);

   void addHead(final List<MessageReference> refs);

   void acknowledge(MessageReference ref) throws Exception;

   void acknowledge(Transaction tx, MessageReference ref) throws Exception;

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

   MessageReference removeReferenceWithID(long id) throws Exception;

   MessageReference getReference(long id);

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

   boolean changeReferencePriority(long messageID, byte newPriority) throws Exception;

   int changeReferencesPriority(Filter filter, byte newPriority) throws Exception;

   boolean moveReference(long messageID, SimpleString toAddress) throws Exception;

   boolean moveReference(long messageID, SimpleString toAddress, boolean rejectDuplicates) throws Exception;

   int moveReferences(Filter filter, SimpleString toAddress) throws Exception;

   int moveReferences(final int flushLimit, Filter filter, SimpleString toAddress, boolean rejectDuplicates) throws Exception;

   void addRedistributor(long delay);

   void cancelRedistributor() throws Exception;

   boolean hasMatchingConsumer(ServerMessage message);

   Collection<Consumer> getConsumers();

   boolean checkRedelivery(MessageReference ref, long timeBase, boolean ignoreRedeliveryDelay) throws Exception;

   LinkedListIterator<MessageReference> iterator();

   LinkedListIterator<MessageReference> totalIterator();

   SimpleString getExpiryAddress();

   /**
    * Pauses the queue. It will receive messages but won't give them to the consumers until resumed.
    * If a queue is paused, pausing it again will only throw a warning.
    * To check if a queue is paused, invoke <i>isPaused()</i>
    */
   void pause();

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

   void incrementMesssagesAdded();

   List<MessageReference> cancelScheduledMessages();

   void postAcknowledge(MessageReference ref);

   float getRate();
}
