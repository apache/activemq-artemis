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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.paging.cursor.NonExistentPage;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.jboss.logging.Logger;

public class RefsOperation extends TransactionOperationAbstract {

   private static final Logger logger = Logger.getLogger(RefsOperation.class);

   private final StorageManager storageManager;
   private Queue queue;
   List<MessageReference> refsToAck = new ArrayList<>();

   List<MessageReference> pagedMessagesToPostACK = null;

   /**
    * It will ignore redelivery check, which is used during consumer.close
    * to not perform reschedule redelivery check
    */
   protected boolean ignoreRedeliveryCheck = false;

   public RefsOperation(Queue queue, StorageManager storageManager) {
      this.queue = queue;
      this.storageManager = storageManager;
   }

   // once turned on, we shouldn't turn it off, that's why no parameters
   public void setIgnoreRedeliveryCheck() {
      ignoreRedeliveryCheck = true;
   }

   synchronized void addAck(final MessageReference ref) {
      refsToAck.add(ref);
      if (ref.isPaged()) {
         if (pagedMessagesToPostACK == null) {
            pagedMessagesToPostACK = new ArrayList<>();
         }
         pagedMessagesToPostACK.add(ref);
      }
   }

   @Override
   public void afterRollback(final Transaction tx) {
      Map<QueueImpl, LinkedList<MessageReference>> queueMap = new HashMap<>();

      long timeBase = System.currentTimeMillis();

      //add any already acked refs, this means that they have been transferred via a producer.send() and have no
      // previous state persisted.
      List<MessageReference> ackedRefs = new ArrayList<>();

      for (MessageReference ref : refsToAck) {
         ref.emptyConsumerID();

         if (logger.isTraceEnabled()) {
            logger.trace("rolling back " + ref);
         }
         try {
            if (ref.isAlreadyAcked()) {
               ackedRefs.add(ref);
            }
            rollbackRedelivery(tx, ref, timeBase, queueMap);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorCheckingDLQ(e);
         }
      }

      for (Map.Entry<QueueImpl, LinkedList<MessageReference>> entry : queueMap.entrySet()) {
         LinkedList<MessageReference> refs = entry.getValue();

         QueueImpl queue = entry.getKey();

         synchronized (queue) {
            queue.postRollback(refs);
         }
      }

      if (!ackedRefs.isEmpty()) {
         //since pre acked refs have no previous state we need to actually create this by storing the message and the
         //message references
         try {
            Transaction ackedTX = new TransactionImpl(storageManager);
            for (MessageReference ref : ackedRefs) {
               Message message = ref.getMessage();
               if (message.isDurable()) {
                  int durableRefCount = message.incrementDurableRefCount();

                  if (durableRefCount == 1) {
                     storageManager.storeMessageTransactional(ackedTX.getID(), message);
                  }
                  Queue queue = ref.getQueue();

                  storageManager.storeReferenceTransactional(ackedTX.getID(), queue.getID(), message.getMessageID());

                  ackedTX.setContainsPersistent();
               }

               message.incrementRefCount();
            }
            ackedTX.commit(true);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.failedToProcessMessageReferenceAfterRollback(e);
         }
      }
   }

   protected void rollbackRedelivery(Transaction tx, MessageReference ref, long timeBase, Map<QueueImpl, LinkedList<MessageReference>> queueMap) throws Exception {
      // if ignore redelivery check, we just perform redelivery straight
      if (ref.getQueue().checkRedelivery(ref, timeBase, ignoreRedeliveryCheck)) {
         LinkedList<MessageReference> toCancel = queueMap.get(ref.getQueue());

         if (toCancel == null) {
            toCancel = new LinkedList<>();

            queueMap.put((QueueImpl) ref.getQueue(), toCancel);
         }

         toCancel.addFirst(ref);
      }
   }

   @Override
   public void afterCommit(final Transaction tx) {
      for (MessageReference ref : refsToAck) {
         synchronized (ref.getQueue()) {
            queue.postAcknowledge(ref);
         }
      }

      if (pagedMessagesToPostACK != null) {
         for (MessageReference refmsg : pagedMessagesToPostACK) {
            if (((PagedReference) refmsg).isLargeMessage()) {
               decrementRefCount(refmsg);
            }
         }
      }
   }

   private void decrementRefCount(MessageReference refmsg) {
      try {
         refmsg.getMessage().decrementRefCount();
      } catch (NonExistentPage e) {
         // This could happen on after commit, since the page could be deleted on file earlier by another thread
         logger.debug(e);
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.failedToDecrementMessageReferenceCount(e);
      }
   }

   @Override
   public synchronized List<MessageReference> getRelatedMessageReferences() {
      List<MessageReference> listRet = new LinkedList<>();
      listRet.addAll(listRet);
      return listRet;
   }

   @Override
   public synchronized List<MessageReference> getListOnConsumer(long consumerID) {
      List<MessageReference> list = new LinkedList<>();
      for (MessageReference ref : refsToAck) {
         if (ref.hasConsumerId() && ref.getConsumerId() == consumerID) {
            list.add(ref);
         }
      }

      return list;
   }

   public List<MessageReference> getReferencesToAcknowledge() {
      return refsToAck;
   }

}
