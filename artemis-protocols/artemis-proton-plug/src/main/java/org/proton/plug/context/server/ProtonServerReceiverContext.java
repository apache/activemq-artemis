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
package org.proton.plug.context.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.context.AbstractConnectionContext;
import org.proton.plug.context.AbstractProtonReceiverContext;
import org.proton.plug.context.AbstractProtonSessionContext;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.exceptions.ActiveMQAMQPInternalErrorException;
import org.proton.plug.logger.ActiveMQAMQPProtocolMessageBundle;

import static org.proton.plug.util.DeliveryUtil.readDelivery;

public class ProtonServerReceiverContext extends AbstractProtonReceiverContext {

   private final int numberOfCredits = 100;

   public ProtonServerReceiverContext(AMQPSessionCallback sessionSPI,
                                      AbstractConnectionContext connection,
                                      AbstractProtonSessionContext protonSession,
                                      Receiver receiver) {
      super(sessionSPI, connection, protonSession, receiver);
   }

   public void onFlow(int credits) {
   }

   @Override
   public void initialise() throws Exception {
      super.initialise();
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      if (target != null) {
         if (target.getDynamic()) {
            //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
            // will be deleted on closing of the session
            String queue = sessionSPI.tempQueueName();

            try {
               sessionSPI.createTemporaryQueue(queue);
            }
            catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
            target.setAddress(queue.toString());
         }
         else {
            //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
            //be a queue bound to it so we nee to check this.
            String address = target.getAddress();
            if (address == null) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.targetAddressNotSet();
            }
            try {
               if (!sessionSPI.queueQuery(address)) {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist();
               }
            }
            catch (Exception e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorFindingTemporaryQueue(e.getMessage());
            }
         }
      }

      flow(numberOfCredits);
   }

   /*
   * called when Proton receives a message to be delivered via a Delivery.
   *
   * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
   *
   * */
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      Receiver receiver;
      try {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable()) {
            System.err.println("!!!!! Readable!!!!!!!");
            return;
         }

         ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(10 * 1024);
         try {
            synchronized (connection.getLock()) {
               readDelivery(receiver, buffer);

               receiver.advance();

               sessionSPI.serverSend(receiver, delivery, address, delivery.getMessageFormat(), buffer);
               delivery.disposition(Accepted.getInstance());
               delivery.settle();

               if (receiver.getRemoteCredit() < numberOfCredits / 2) {
                  flow(numberOfCredits);
               }
            }
         }
         finally {
            buffer.release();
         }
      }
      catch (Exception e) {
         e.printStackTrace();
         Rejected rejected = new Rejected();
         ErrorCondition condition = new ErrorCondition();
         condition.setCondition(Symbol.valueOf("failed"));
         condition.setDescription(e.getMessage());
         rejected.setError(condition);
         delivery.disposition(rejected);
      }
   }

}
