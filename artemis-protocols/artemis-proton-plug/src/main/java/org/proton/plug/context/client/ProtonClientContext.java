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
package org.proton.plug.context.client;

import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.proton.plug.AMQPClientSenderContext;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.context.AbstractConnectionContext;
import org.proton.plug.context.AbstractProtonContextSender;
import org.proton.plug.context.AbstractProtonSessionContext;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.util.FutureRunnable;

public class ProtonClientContext extends AbstractProtonContextSender implements AMQPClientSenderContext {

   FutureRunnable catchUpRunnable = new FutureRunnable();

   public ProtonClientContext(AbstractConnectionContext connection,
                              Sender sender,
                              AbstractProtonSessionContext protonSession,
                              AMQPSessionCallback server) {
      super(connection, sender, protonSession, server);
   }

   @Override
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      if (delivery.getRemoteState() instanceof Accepted) {
         if (delivery.getContext() instanceof FutureRunnable) {
            ((FutureRunnable) delivery.getContext()).countDown();
         }
      }
   }

   @Override
   public void send(ProtonJMessage message) {
      if (sender.getSenderSettleMode() != SenderSettleMode.SETTLED) {
         catchUpRunnable.countUp();
      }
      performSend(message, catchUpRunnable);
   }

   public boolean sync(long timeout, TimeUnit unit) {
      try {
         return catchUpRunnable.await(timeout, unit);
      }
      catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return false;
      }
   }

   @Override
   public String getAddress() {
      return sender.getRemoteTarget().getAddress();
   }
}
