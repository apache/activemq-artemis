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

import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.RefsOperation;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.engine.Delivery;


/**
 * AMQP Protocol has different TX Rollback behaviour for Acks depending on whether an AMQP delivery has been settled
 * or not.  This class extends the Core TransactionImpl used for normal TX behaviour.  In the case where deliveries
 * have been settled, normal Ack rollback is applied.  For cases where deliveries are unsettled and rolled back,
 * we increment the delivery count and return to the consumer.
 */
public class ProtonTransactionImpl extends TransactionImpl {

   /* The discharge delivery.  We use this to settle once the TX has completed all operations.  If this is null we
   know that the TX was forced rollback.
    */
   private Delivery discharge;

   public ProtonTransactionImpl(final Xid xid, final StorageManager storageManager, final int timeoutSeconds) {
      super(xid, storageManager, timeoutSeconds);
   }

   @Override
   public RefsOperation createRefsOperation(Queue queue) {
      return new ProtonTransactionRefsOperation(queue, storageManager);
   }

   public void discharge(Delivery delivery) {
      this.discharge = delivery;
   }

   public boolean discharged() {
      return discharge != null;
   }

   public void addDelivery(Delivery delivery, ProtonServerSenderContext context) {
      ((ProtonTransactionRefsOperation) getProperty(TransactionPropertyIndexes.REFS_OPERATION)).addDelivery(delivery, context);
   }
   /**
    * Settles the discharge delivery.
    */
   public void settle() {
      if (discharge != null) {
         discharge.disposition(new Accepted());
         discharge.settle();
      }
   }
}


