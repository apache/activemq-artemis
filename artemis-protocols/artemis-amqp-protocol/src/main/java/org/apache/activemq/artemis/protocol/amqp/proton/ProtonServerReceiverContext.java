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
package org.apache.activemq.artemis.protocol.amqp.proton;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.util.DeliveryUtil;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

public class ProtonServerReceiverContext extends ProtonInitializable implements ProtonDeliveryHandler {

   private static final Logger log = Logger.getLogger(ProtonServerReceiverContext.class);

   protected final AMQPConnectionContext connection;

   protected final AMQPSessionContext protonSession;

   protected final Receiver receiver;

   protected String address;

   protected final AMQPSessionCallback sessionSPI;

   /*
    The maximum number of credits we will allocate to clients.
    This number is also used by the broker when refresh client credits.
     */
   private static int maxCreditAllocation = 100;

   // Used by the broker to decide when to refresh clients credit.  This is not used when client requests credit.
   private static int minCreditRefresh = 30;
   private TerminusExpiryPolicy expiryPolicy;

   public ProtonServerReceiverContext(AMQPSessionCallback sessionSPI,
                                      AMQPConnectionContext connection,
                                      AMQPSessionContext protonSession,
                                      Receiver receiver) {
      this.connection = connection;
      this.protonSession = protonSession;
      this.receiver = receiver;
      this.sessionSPI = sessionSPI;
   }

   @Override
   public void onFlow(int credits, boolean drain) {
      flow(Math.min(credits, maxCreditAllocation), maxCreditAllocation);
   }

   @Override
   public void initialise() throws Exception {
      super.initialise();
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      if (target != null) {
         if (target.getDynamic()) {
            //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
            // will be deleted on closing of the session
            address = sessionSPI.tempQueueName();

            try {
               sessionSPI.createTemporaryQueue(address, RoutingType.ANYCAST);
            } catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
            expiryPolicy = target.getExpiryPolicy() != null ? target.getExpiryPolicy() : TerminusExpiryPolicy.LINK_DETACH;
            target.setAddress(address);
         } else {
            //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
            //be a queue bound to it so we nee to check this.
            address = target.getAddress();
            if (address == null) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.targetAddressNotSet();
            }

            try {
               if (!sessionSPI.bindingQuery(address)) {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist();
               }
            } catch (ActiveMQAMQPNotFoundException e) {
               throw e;
            } catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
         }
      }
      flow(maxCreditAllocation, minCreditRefresh);
   }

   /*
   * called when Proton receives a message to be delivered via a Delivery.
   *
   * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
   *
   * */
   @Override
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      Receiver receiver;
      try {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable()) {
            return;
         }

         if (delivery.isPartial()) {
            return;
         }

         ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(10 * 1024);
         try {
            synchronized (connection.getLock()) {
               DeliveryUtil.readDelivery(receiver, buffer);

               receiver.advance();

               Transaction tx = null;
               if (delivery.getRemoteState() instanceof TransactionalState) {

                  TransactionalState txState = (TransactionalState) delivery.getRemoteState();
                  tx = this.sessionSPI.getTransaction(txState.getTxnId());
               }
               sessionSPI.serverSend(tx, receiver, delivery, address, delivery.getMessageFormat(), buffer);

               flow(maxCreditAllocation, minCreditRefresh);
            }
         } finally {
            buffer.release();
         }
      } catch (Exception e) {
         log.warn(e.getMessage(), e);
         Rejected rejected = new Rejected();
         ErrorCondition condition = new ErrorCondition();
         condition.setCondition(Symbol.valueOf("failed"));
         condition.setDescription(e.getMessage());
         rejected.setError(condition);
         delivery.disposition(rejected);
         delivery.settle();
      }
   }

   @Override
   public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
      protonSession.removeReceiver(receiver);
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();
      if (target != null && target.getDynamic() && (target.getExpiryPolicy() == TerminusExpiryPolicy.LINK_DETACH || target.getExpiryPolicy() == TerminusExpiryPolicy.SESSION_END)) {
         try {
            sessionSPI.removeTemporaryQueue(target.getAddress());
         } catch (Exception e) {
            //ignore on close, its temp anyway and will be removed later
         }
      }
   }

   @Override
   public void close(ErrorCondition condition) throws ActiveMQAMQPException {
      receiver.setCondition(condition);
      close(false);
   }

   public void flow(int credits, int threshold) {
      // Use the SessionSPI to allocate producer credits, or default, always allocate credit.
      if (sessionSPI != null) {
         sessionSPI.offerProducerCredit(address, credits, threshold, receiver);
      } else {
         synchronized (connection.getLock()) {
            receiver.flow(credits);
            connection.flush();
         }
      }

   }

   public void drain(int credits) {
      synchronized (connection.getLock()) {
         receiver.drain(credits);
      }
      connection.flush();
   }

   public int drained() {
      return receiver.drained();
   }

   public boolean isDraining() {
      return receiver.draining();
   }

}
