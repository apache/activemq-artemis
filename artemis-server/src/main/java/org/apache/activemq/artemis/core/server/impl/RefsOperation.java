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

import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RefsOperation extends TransactionOperationAbstract {

   private final StorageManager storageManager;
   private Queue queue;
   List<MessageReference> refsToAck = new ArrayList<MessageReference>();

   List<ServerMessage> pagedMessagesToPostACK = null;

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
            pagedMessagesToPostACK = new ArrayList<ServerMessage>();
         }
         pagedMessagesToPostACK.add(ref.getMessage());
      }
   }

   @Override
   public void afterRollback(final Transaction tx) {
      Map<QueueImpl, LinkedList<MessageReference>> queueMap = new HashMap<QueueImpl, LinkedList<MessageReference>>();

      long timeBase = System.currentTimeMillis();

      //add any already acked refs, this means that they have been transferred via a producer.send() and have no
      // previous state persisted.
      List<MessageReference> ackedRefs = new ArrayList<>();

      for (MessageReference ref : refsToAck) {
         ref.setConsumerId(null);

         if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
            ActiveMQServerLogger.LOGGER.trace("rolling back " + ref);
         }
         try {
            if (ref.isAlreadyAcked()) {
               ackedRefs.add(ref);
            }
            // if ignore redelivery check, we just perform redelivery straight
            if (ref.getQueue().checkRedelivery(ref, timeBase, ignoreRedeliveryCheck)) {
               LinkedList<MessageReference> toCancel = queueMap.get(ref.getQueue());

               if (toCancel == null) {
                  toCancel = new LinkedList<MessageReference>();

                  queueMap.put((QueueImpl) ref.getQueue(), toCancel);
               }

               toCancel.addFirst(ref);
            }
         }
         catch (Exception e) {
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
               ServerMessage message = ref.getMessage();
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
         }
         catch (Exception e) {
            e.printStackTrace();
         }
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
         for (ServerMessage msg : pagedMessagesToPostACK) {
            try {
               msg.decrementRefCount();
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }
      }
   }

   @Override
   public synchronized List<MessageReference> getRelatedMessageReferences() {
      List<MessageReference> listRet = new LinkedList<MessageReference>();
      listRet.addAll(listRet);
      return listRet;
   }

   @Override
   public synchronized List<MessageReference> getListOnConsumer(long consumerID) {
      List<MessageReference> list = new LinkedList<MessageReference>();
      for (MessageReference ref : refsToAck) {
         if (ref.getConsumerId() != null && ref.getConsumerId().equals(consumerID)) {
            list.add(ref);
         }
      }

      return list;
   }

   public List<MessageReference> getReferencesToAcknowledge() {
      return refsToAck;
   }

}
