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

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.context.AbstractConnectionContext;
import org.proton.plug.context.AbstractProtonContextSender;
import org.proton.plug.context.AbstractProtonReceiverContext;
import org.proton.plug.context.AbstractProtonSessionContext;
import org.proton.plug.context.ProtonTransactionHandler;
import org.proton.plug.exceptions.ActiveMQAMQPException;

public class ProtonServerSessionContext extends AbstractProtonSessionContext {

   public ProtonServerSessionContext(AMQPSessionCallback sessionSPI,
                                     AbstractConnectionContext connection,
                                     Session session) {
      super(sessionSPI, connection, session);
   }

   protected Map<Object, AbstractProtonContextSender> serverSenders = new HashMap<Object, AbstractProtonContextSender>();

   /**
    * The consumer object from the broker or the key used to store the sender
    *
    * @param message
    * @param consumer
    * @param deliveryCount
    * @return the number of bytes sent
    */
   public int serverDelivery(Object message, Object consumer, int deliveryCount) throws Exception {
      ProtonServerSenderContext protonSender = (ProtonServerSenderContext) serverSenders.get(consumer);
      if (protonSender != null) {
         return protonSender.deliverMessage(message, deliveryCount);
      }
      return 0;
   }

   public void addTransactionHandler(Coordinator coordinator, Receiver receiver) {
      ProtonTransactionHandler transactionHandler = new ProtonTransactionHandler(sessionSPI);
      receiver.setContext(transactionHandler);
      receiver.open();
      receiver.flow(100);
   }

   public void addSender(Sender sender) throws Exception {
      ProtonServerSenderContext protonSender = new ProtonServerSenderContext(connection, sender, this, sessionSPI);

      try {
         protonSender.initialise();
         senders.put(sender, protonSender);
         serverSenders.put(protonSender.getBrokerConsumer(), protonSender);
         sender.setContext(protonSender);
         sender.open();
         protonSender.start();
      }
      catch (ActiveMQAMQPException e) {
         senders.remove(sender);
         sender.setSource(null);
         sender.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         sender.close();
      }
   }

   public void removeSender(Sender sender) throws ActiveMQAMQPException {
      ProtonServerSenderContext senderRemoved = (ProtonServerSenderContext) senders.remove(sender);
      if (senderRemoved != null) {
         serverSenders.remove(senderRemoved.getBrokerConsumer());
      }
   }

   public void addReceiver(Receiver receiver) throws Exception {
      try {
         AbstractProtonReceiverContext protonReceiver = new ProtonServerReceiverContext(sessionSPI, connection, this, receiver);
         protonReceiver.initialise();
         receivers.put(receiver, protonReceiver);
         receiver.setContext(protonReceiver);
         receiver.open();
      }
      catch (ActiveMQAMQPException e) {
         receivers.remove(receiver);
         receiver.setTarget(null);
         receiver.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         receiver.close();
      }
   }

}
