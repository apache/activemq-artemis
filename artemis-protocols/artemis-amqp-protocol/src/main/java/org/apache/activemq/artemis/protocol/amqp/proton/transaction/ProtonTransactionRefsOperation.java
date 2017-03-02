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
package org.apache.activemq.artemis.protocol.amqp.proton.transaction;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.server.impl.RefsOperation;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.qpid.proton.engine.Delivery;

/**
 * AMQP Protocol has different TX Rollback behaviour for Acks depending on whether an AMQP delivery has been settled
 * or not.  This class extends the Core RefsOperation used for normal acks.  In the case where deliveries have been
 * settled, normal Ack rollback is applied.  For cases where deliveries are unsettled and rolled back, we increment
 * the delivery count and return to the consumer.
 */
public class ProtonTransactionRefsOperation extends RefsOperation {

   /* We need to track the Message reference against the AMQP objects, so we can check whether the corresponding
   deliveries have been settled.  We also need to ensure we are settling on the correct link.  Hence why we keep a ref
   to the ProtonServerSenderContext here.
    */
   private final Map<MessageReference, Pair<Delivery, ProtonServerSenderContext>> deliveries = new HashMap<>();

   public ProtonTransactionRefsOperation(final Queue queue, StorageManager storageManager) {
      super(queue, storageManager);
   }

   @Override
   public void rollbackRedelivery(Transaction txn, MessageReference ref, long timeBase, Map<QueueImpl, LinkedList<MessageReference>> queueMap) throws Exception {
      ProtonTransactionImpl tx = (ProtonTransactionImpl) txn;

      if (deliveries.containsKey(ref)) {
         Delivery del = deliveries.get(ref).getA();
         ServerConsumer consumer = (ServerConsumer) deliveries.get(ref).getB().getBrokerConsumer();
         // Rollback normally if the delivery is not settled or a forced TX rollback is done (e.g. connection drop).
         if (del.remotelySettled() || !tx.discharged()) {
            super.rollbackRedelivery(tx, ref, timeBase, queueMap);
         } else {
            ref.incrementDeliveryCount();
            consumer.backToDelivering(ref);
            del.disposition(del.getLocalState() == null ? del.getDefaultDeliveryState() : del.getLocalState());
         }
      } else {
         super.rollbackRedelivery(tx, ref, timeBase, queueMap);
      }
   }

   @Override
   public void afterRollback(Transaction tx) {
      super.afterRollback(tx);
      // Send the TX settle disposition on the controller link.
      ((ProtonTransactionImpl) tx).settle();
   }

   public void addDelivery(Delivery delivery, ProtonServerSenderContext context) {
      deliveries.put(((MessageReference) delivery.getContext()), new Pair<>(delivery, context));
   }

   @Override
   public void afterCommit(final Transaction tx) {
      super.afterCommit(tx);

      // Settle all unsettled deliveries after commit
      for (Pair<Delivery, ProtonServerSenderContext> p : deliveries.values()) {
         if (!p.getA().isSettled()) p.getB().settle(p.getA());
      }
      // Send the TX settle disposition on the controller link.
      ((ProtonTransactionImpl) tx).settle();
   }
}
