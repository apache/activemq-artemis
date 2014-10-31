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
package org.hornetq.core.protocol.proton;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;

/**
 * handles an amqp Coordinator to deal with transaction boundaries etc
 */
public class TransactionHandler implements ProtonDeliveryHandler
{
   private final ProtonRemotingConnection connection;
   private final Coordinator coordinator;
   private final ProtonProtocolManager protonProtocolManager;
   private final ProtonSession protonSession;
   private final HornetQBuffer buffer;

   public TransactionHandler(ProtonRemotingConnection connection, Coordinator coordinator, ProtonProtocolManager protonProtocolManager, ProtonSession protonSession)
   {
      this.connection = connection;
      this.coordinator = coordinator;
      this.protonProtocolManager = protonProtocolManager;
      this.protonSession = protonSession;
      buffer = connection.createBuffer(1024);
   }

   @Override
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      Receiver receiver = null;
      try
      {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable())
         {
            return;
         }

         protonProtocolManager.handleTransaction(receiver, buffer, delivery, protonSession);

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
   }

   @Override
   public void checkState()
   {
      //noop
   }

   @Override
   public void close() throws HornetQAMQPException
   {
      //noop
   }
}
