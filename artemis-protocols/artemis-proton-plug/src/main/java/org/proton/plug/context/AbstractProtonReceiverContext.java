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
package org.proton.plug.context;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.exceptions.ActiveMQAMQPException;

/**
 * handles incoming messages via a Proton Receiver and forwards them to ActiveMQ
 */
public abstract class AbstractProtonReceiverContext extends ProtonInitializable implements ProtonDeliveryHandler {

   protected final AbstractConnectionContext connection;

   protected final AbstractProtonSessionContext protonSession;

   protected final Receiver receiver;

   protected String address;

   protected final AMQPSessionCallback sessionSPI;

   public AbstractProtonReceiverContext(AMQPSessionCallback sessionSPI,
                                        AbstractConnectionContext connection,
                                        AbstractProtonSessionContext protonSession,
                                        Receiver receiver) {
      this.connection = connection;
      this.protonSession = protonSession;
      this.receiver = receiver;
      this.sessionSPI = sessionSPI;
   }

   @Override
   public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
      protonSession.removeReceiver(receiver);
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
      }
      else {
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
