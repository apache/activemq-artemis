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

import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonDeliveryHandler;
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

   public static final int DEFAULT_COORDINATOR_CREDIT = 100;
   public static final int CREDIT_LOW_WATERMARK = 30;

   final AMQPSessionCallback sessionSPI;
   final AMQPConnectionContext connection;

   public ProtonTransactionHandler(AMQPSessionCallback sessionSPI, AMQPConnectionContext connection) {
      this.sessionSPI = sessionSPI;
      this.connection = connection;
   }

   @Override
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      final Receiver receiver;
      try {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable()) {
            return;
         }

         byte[] buffer;

         synchronized (connection.getLock()) {
            // Replenish coordinator receiver credit on exhaustion so sender can continue
            // transaction declare and discahrge operations.
            if (receiver.getCredit() < CREDIT_LOW_WATERMARK) {
               receiver.flow(DEFAULT_COORDINATOR_CREDIT);
            }

            buffer = new byte[delivery.available()];
            receiver.recv(buffer, 0, buffer.length);
            receiver.advance();
         }



         MessageImpl msg = DeliveryUtil.decodeMessageImpl(buffer);

         Object action = ((AmqpValue) msg.getBody()).getValue();

         if (action instanceof Declare) {
            Binary txID = sessionSPI.newTransaction();
            Declared declared = new Declared();
            declared.setTxnId(txID);
            synchronized (connection.getLock()) {
               delivery.disposition(declared);
            }
         } else if (action instanceof Discharge) {
            Discharge discharge = (Discharge) action;

            Binary txID = discharge.getTxnId();
            ProtonTransactionImpl tx = (ProtonTransactionImpl)sessionSPI.getTransaction(txID, true);
            tx.discharge();

            if (discharge.getFail()) {
               tx.rollback();
               synchronized (connection.getLock()) {
                  delivery.disposition(new Accepted());
               }
               connection.flush();
            } else {
               tx.commit();
               synchronized (connection.getLock()) {
                  delivery.disposition(new Accepted());
               }
               connection.flush();
            }
         }
      } catch (ActiveMQAMQPException amqpE) {
         log.warn(amqpE.getMessage(), amqpE);
         synchronized (connection.getLock()) {
            delivery.disposition(createRejected(amqpE.getAmqpError(), amqpE.getMessage()));
         }
         connection.flush();
      } catch (Throwable e) {
         log.warn(e.getMessage(), e);
         synchronized (connection.getLock()) {
            delivery.disposition(createRejected(Symbol.getSymbol("failed"), e.getMessage()));
         }
         connection.flush();
      } finally {
         synchronized (connection.getLock()) {
            delivery.settle();
         }
         connection.flush();
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
