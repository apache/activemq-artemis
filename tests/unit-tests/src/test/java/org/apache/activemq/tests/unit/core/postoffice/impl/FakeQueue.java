/**
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
package org.apache.activemq.tests.unit.core.postoffice.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.filter.Filter;
import org.apache.activemq.core.paging.cursor.PageSubscription;
import org.apache.activemq.core.server.Consumer;
import org.apache.activemq.core.server.MessageReference;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.RoutingContext;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.transaction.Transaction;
import org.apache.activemq.utils.LinkedListIterator;
import org.apache.activemq.utils.ReferenceCounter;

/**
 * A FakeQueue
 */
public class FakeQueue implements Queue
{

   @Override
   public boolean isInternalQueue()
   {
      // no-op
      return false;
   }

   @Override
   public void deleteQueue(boolean removeConsumers) throws Exception
   {
   }

   public void unproposed(SimpleString groupID)
   {

   }

   @Override
   public void setConsumersRefCount(ReferenceCounter referenceCounter)
   {

   }

   @Override
   public void setInternalQueue(boolean internalQueue)
   {
      // no-op

   }

   @Override
   public void cancel(Transaction tx, MessageReference ref, boolean ignoreRedeliveryCheck)
   {
      // no-op
   }

   PageSubscription subs;

   public boolean isDirectDeliver()
   {
      // no-op
      return false;
   }

   public void close()
   {
      // no-op

   }

   public void forceCheckQueueSize()
   {
      // no-op

   }

   public void reload(MessageReference ref)
   {
      // no-op

   }

   public boolean flushExecutor()
   {
      return true;
   }

   public void addHead(MessageReference ref)
   {
      // no-op

   }

   public void addHead(List<MessageReference> ref)
   {
      // no-op

   }

   public void addTail(MessageReference ref, boolean direct)
   {
      // no-op

   }

   public void addTail(MessageReference ref)
   {
      // no-op

   }

   public void resetAllIterators()
   {
      // no-op

   }

   private final SimpleString name;

   private final long id;

   private long messageCount;

   public FakeQueue(final SimpleString name)
   {
      this(name, 0);
   }

   public FakeQueue(final SimpleString name, final long id)
   {
      this.name = name;
      this.id = id;
   }

   @Override
   public void acknowledge(final MessageReference ref) throws Exception
   {
      // no-op

   }

   @Override
   public void acknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      // no-op

   }

   @Override
   public void addConsumer(final Consumer consumer) throws Exception
   {
      // no-op

   }

   @Override
   public void addRedistributor(final long delay)
   {
      // no-op

   }

   @Override
   public void cancel(final MessageReference reference, final long timeBase) throws Exception
   {
      // no-op

   }

   @Override
   public void cancel(final Transaction tx, final MessageReference ref)
   {
      // no-op

   }

   @Override
   public void cancelRedistributor() throws Exception
   {
      // no-op

   }

   @Override
   public boolean changeReferencePriority(final long messageID, final byte newPriority) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public int changeReferencesPriority(Filter filter, byte newPriority) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public boolean checkRedelivery(final MessageReference ref, final long timeBase, final boolean check) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public int deleteAllReferences() throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public int deleteMatchingReferences(final Filter filter) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public boolean deleteReference(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public void deliverAsync()
   {
      // no-op

   }

   @Override
   public void expire(final MessageReference ref) throws Exception
   {
      // no-op

   }

   @Override
   public boolean expireReference(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public void expireReferences() throws Exception
   {
      // no-op

   }

   @Override
   public int expireReferences(final Filter filter) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public int getConsumerCount()
   {
      // no-op
      return 0;
   }

   @Override
   public ReferenceCounter getConsumersRefCount()
   {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public Set<Consumer> getConsumers()
   {
      // no-op
      return null;
   }

   @Override
   public int getDeliveringCount()
   {
      // no-op
      return 0;
   }

   @Override
   public Filter getFilter()
   {
      // no-op
      return null;
   }

   @Override
   public long getMessageCount()
   {
      return messageCount;
   }

   public void setMessageCount(long messageCount)
   {
      this.messageCount = messageCount;
   }

   @Override
   public long getMessagesAdded()
   {
      // no-op
      return 0;
   }

   @Override
   public long getMessagesAcknowledged()
   {
      // no-op
      return 0;
   }

   @Override
   public void resetMessagesAdded()
   {
      // no-op

   }

   @Override
   public void resetMessagesAcknowledged()
   {
      // no-op

   }

   @Override
   public void incrementMesssagesAdded()
   {

   }

   @Override
   public void deliverScheduledMessages()
   {

   }

   @Override
   public SimpleString getName()
   {
      return name;
   }

   public SimpleString getAddress()
   {
      // no-op
      return null;
   }

   @Override
   public long getID()
   {
      return id;
   }

   @Override
   public MessageReference getReference(final long id1)
   {
      // no-op
      return null;
   }

   @Override
   public int getScheduledCount()
   {
      // no-op
      return 0;
   }

   @Override
   public List<MessageReference> getScheduledMessages()
   {
      // no-op
      return null;
   }

   @Override
   public boolean isDurable()
   {
      // no-op
      return false;
   }

   @Override
   public boolean isPaused()
   {
      // no-op
      return false;
   }

   @Override
   public boolean isTemporary()
   {
      // no-op
      return false;
   }

   @Override
   public boolean isAutoCreated()
   {
      return false;
   }

   @Override
   public LinkedListIterator<MessageReference> iterator()
   {
      // no-op
      return null;
   }

   @Override
   public boolean moveReference(final long messageID, final SimpleString toAddress) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public int moveReferences(final Filter filter, final SimpleString toAddress) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public void pause()
   {
      // no-op

   }

   @Override
   public void reacknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      // no-op

   }

   public void referenceHandled()
   {
      // no-op

   }

   public void removeConsumer(final Consumer consumer)
   {
   }

   public MessageReference removeFirstReference(final long id1) throws Exception
   {
      // no-op
      return null;
   }

   public MessageReference removeReferenceWithID(final long id1) throws Exception
   {
      // no-op
      return null;
   }

   public void resume()
   {
      // no-op

   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   public int sendMessagesToDeadLetterAddress(Filter filter) throws Exception
   {
      // no-op
      return 0;
   }


   @Override
   public SimpleString getExpiryAddress()
   {
      return null;
   }

   @Override
   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      // no-op

   }

   @Override
   public void routeWithAck(ServerMessage message, RoutingContext context)
   {

   }

   public boolean hasMatchingConsumer(final ServerMessage message)
   {
      // no-op
      return false;
   }

   public Executor getExecutor()
   {
      // no-op
      return null;
   }

   public void addLast(MessageReference ref, boolean direct)
   {
      // no-op

   }

   @Override
   public PageSubscription getPageSubscription()
   {
      return subs;
   }

   public void setPageSubscription(PageSubscription sub)
   {
      this.subs = sub;
   }

   @Override
   public boolean moveReference(long messageID, SimpleString toAddress, boolean rejectDuplicates) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public int deleteAllReferences(int flushLimit) throws Exception
   {
      return 0;
   }

   @Override
   public int deleteMatchingReferences(int flushLImit, Filter filter) throws Exception
   {
      return 0;
   }

   @Override
   public int moveReferences(int flushLimit, Filter filter, SimpleString toAddress, boolean rejectDuplicates) throws Exception
   {
      return 0;
   }

   @Override
   public void forceDelivery()
   {
      // no-op

   }

   @Override
   public void deleteQueue() throws Exception
   {
      // no-op
   }

   /* (non-Javadoc)
   * @see org.apache.activemq.core.server.Queue#destroyPaging()
   */
   public void destroyPaging()
   {
   }

   /* (non-Javadoc)
    * @see org.apache.activemq.core.server.Queue#getDeliveringMessages()
    */
   @Override
   public Map<String, List<MessageReference>> getDeliveringMessages()
   {
      return null;
   }

   @Override
   public LinkedListIterator<MessageReference> totalIterator()
   {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public void postAcknowledge(MessageReference ref)
   {
   }

   @Override
   public float getRate()
   {
      return 0.0f;
   }
}