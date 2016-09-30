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
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.util.DeliveryUtil;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.jboss.logging.Logger;

/**
 * handles an amqp Coordinator to deal with transaction boundaries etc
 */
public class ProtonTransactionHandler implements ProtonDeliveryHandler {

   private static final Logger log = Logger.getLogger(ProtonTransactionHandler.class);

   final AMQPSessionCallback sessionSPI;

   public ProtonTransactionHandler(AMQPSessionCallback sessionSPI) {
      this.sessionSPI = sessionSPI;
   }

   @Override
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);

      final Receiver receiver;
      try {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable()) {
            return;
         }

         DeliveryUtil.readDelivery(receiver, buffer);

         receiver.advance();

         MessageImpl msg = DeliveryUtil.decodeMessageImpl(buffer);

         Object action = ((AmqpValue) msg.getBody()).getValue();

         if (action instanceof Declare) {
            Binary txID = sessionSPI.newTransaction();
            Declared declared = new Declared();
            declared.setTxnId(txID);
            delivery.disposition(declared);
            delivery.settle();
         } else if (action instanceof Discharge) {
            Discharge discharge = (Discharge) action;

            Binary txID = discharge.getTxnId();
            if (discharge.getFail()) {
               try {
                  sessionSPI.rollbackTX(txID, true);
                  delivery.disposition(new Accepted());
               } catch (Exception e) {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorRollingbackCoordinator(e.getMessage());
               }
            } else {
               try {
                  sessionSPI.commitTX(txID);
                  delivery.disposition(new Accepted());
               } catch (ActiveMQAMQPException amqpE) {
                  throw amqpE;
               } catch (Exception e) {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCommittingCoordinator(e.getMessage());
               }
            }
         }
      } catch (ActiveMQAMQPException amqpE) {
         delivery.disposition(createRejected(amqpE.getAmqpError(), amqpE.getMessage()));
      } catch (Exception e) {
         log.warn(e.getMessage(), e);
         delivery.disposition(createRejected(Symbol.getSymbol("failed"), e.getMessage()));
      } finally {
         delivery.settle();
         buffer.release();
      }
   }

   private Rejected createRejected(Symbol amqpError, String message) {
      Rejected rejected = new Rejected();
      ErrorCondition condition = new ErrorCondition();
      condition.setCondition(amqpError);
      condition.setDescription(message);
      rejected.setError(condition);
      return rejected;
   }

   @Override
   public void onFlow(int credits, boolean drain) {
   }

   @Override
   public void close(boolean linkRemoteClose) throws ActiveMQAMQPException {
      // no op
   }

   @Override
   public void close(ErrorCondition condition) throws ActiveMQAMQPException {
      // no op
   }
}
