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
package org.proton.plug.context;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.exceptions.HornetQAMQPException;
import org.proton.plug.logger.HornetQAMQPProtocolMessageBundle;

import static org.proton.plug.util.DeliveryUtil.decodeMessageImpl;
import static org.proton.plug.util.DeliveryUtil.readDelivery;

/**
 * handles an amqp Coordinator to deal with transaction boundaries etc
 */
public class ProtonTransactionHandler implements ProtonDeliveryHandler
{

   final AMQPSessionCallback sessionSPI;

   public ProtonTransactionHandler(AMQPSessionCallback sessionSPI)
   {
      this.sessionSPI = sessionSPI;
   }

   @Override
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);

      final Receiver receiver;
      try
      {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable())
         {
            return;
         }

         readDelivery(receiver, buffer);

         receiver.advance();

         MessageImpl msg = decodeMessageImpl(buffer);

         Object action = ((AmqpValue) msg.getBody()).getValue();

         if (action instanceof Declare)
         {
            Binary txID = sessionSPI.getCurrentTXID();
            Declared declared = new Declared();
            declared.setTxnId(txID);
            delivery.disposition(declared);
            delivery.settle();
         }
         else if (action instanceof Discharge)
         {
            Discharge discharge = (Discharge) action;
            if (discharge.getFail())
            {
               try
               {
                  sessionSPI.rollbackCurrentTX();
               }
               catch (Exception e)
               {
                  throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorRollingbackCoordinator(e.getMessage());
               }
            }
            else
            {
               try
               {
                  sessionSPI.commitCurrentTX();
               }
               catch (Exception e)
               {
                  throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCommittingCoordinator(e.getMessage());
               }
            }
            delivery.settle();
         }

      }
      catch (Exception e)
      {
         e.printStackTrace();
         Rejected rejected = new Rejected();
         ErrorCondition condition = new ErrorCondition();
         condition.setCondition(Symbol.valueOf("failed"));
         condition.setDescription(e.getMessage());
         rejected.setError(condition);
         delivery.disposition(rejected);
      }
      finally
      {
         buffer.release();
      }
   }

   public void onFlow(int credits)
   {

   }

   @Override
   public void close() throws HornetQAMQPException
   {
      //noop
   }
}
