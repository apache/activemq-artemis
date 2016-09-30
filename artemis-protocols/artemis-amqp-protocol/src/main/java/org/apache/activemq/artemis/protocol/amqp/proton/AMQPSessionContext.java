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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.jboss.logging.Logger;

public class AMQPSessionContext extends ProtonInitializable {

   private static final Logger log = Logger.getLogger(AMQPSessionContext.class);
   protected final AMQPConnectionContext connection;

   protected final AMQPSessionCallback sessionSPI;

   protected final Session session;

   private long currentTag = 0;

   protected Map<Receiver, ProtonServerReceiverContext> receivers = new HashMap<>();

   protected Map<Sender, ProtonServerSenderContext> senders = new HashMap<>();

   protected boolean closed = false;

   public AMQPSessionContext(AMQPSessionCallback sessionSPI, AMQPConnectionContext connection, Session session) {
      this.connection = connection;
      this.sessionSPI = sessionSPI;
      this.session = session;
   }

   protected Map<Object, ProtonServerSenderContext> serverSenders = new HashMap<>();

   @Override
   public void initialise() throws Exception {
      if (!isInitialized()) {
         super.initialise();

         if (sessionSPI != null) {
            try {
               sessionSPI.init(this, connection.getSASLResult());
            } catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
         }
      }
   }

   /**
    * @param consumer
    * @param queueName
    */
   public void disconnect(Object consumer, String queueName) {
      ProtonServerSenderContext protonConsumer = senders.remove(consumer);
      if (protonConsumer != null) {
         try {
            protonConsumer.close(false);
         } catch (ActiveMQAMQPException e) {
            protonConsumer.getSender().setTarget(null);
            protonConsumer.getSender().setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         }
      }
   }

   /**
    * The consumer object from the broker or the key used to store the sender
    *
    * @param message
    * @param consumer
    * @param deliveryCount
    * @return the number of bytes sent
    */
   public int serverDelivery(Object message, Object consumer, int deliveryCount) throws Exception {
      ProtonServerSenderContext protonSender = serverSenders.get(consumer);
      if (protonSender != null) {
         return protonSender.deliverMessage(message, deliveryCount);
      }
      return 0;
   }

   public byte[] getTag() {
      return Long.toHexString(currentTag++).getBytes();
   }

   public void replaceTag(byte[] tag) {
      // TODO: do we need to reuse this?
   }

   public void close() {
      if (closed) {
         return;
      }

      // Making a copy to avoid ConcurrentModificationException during the iteration
      Set<ProtonServerReceiverContext> receiversCopy = new HashSet<>();
      receiversCopy.addAll(receivers.values());

      for (ProtonServerReceiverContext protonProducer : receiversCopy) {
         try {
            protonProducer.close(false);
         } catch (Exception e) {
            log.warn(e.getMessage(), e);
         }
      }
      receivers.clear();

      Set<ProtonServerSenderContext> protonSendersClone = new HashSet<>();
      protonSendersClone.addAll(senders.values());

      for (ProtonServerSenderContext protonConsumer : protonSendersClone) {
         try {
            protonConsumer.close(false);
         } catch (Exception e) {
            log.warn(e.getMessage(), e);
         }
      }
      senders.clear();
      try {
         if (sessionSPI != null) {
            sessionSPI.close();
         }
      } catch (Exception e) {
         log.warn(e.getMessage(), e);
      }
      closed = true;
   }

   public void removeReceiver(Receiver receiver) {
      receivers.remove(receiver);
   }

   public void addTransactionHandler(Coordinator coordinator, Receiver receiver) {
      ProtonTransactionHandler transactionHandler = new ProtonTransactionHandler(sessionSPI);

      coordinator.setCapabilities(Symbol.getSymbol("amqp:local-transactions"), Symbol.getSymbol("amqp:multi-txns-per-ssn"), Symbol.getSymbol("amqp:multi-ssns-per-txn"));

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
      } catch (ActiveMQAMQPException e) {
         senders.remove(sender);
         sender.setSource(null);
         sender.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         sender.close();
      }
   }

   public void removeSender(Sender sender) throws ActiveMQAMQPException {
      senders.remove(sender);
      ProtonServerSenderContext senderRemoved = senders.remove(sender);
      if (senderRemoved != null) {
         serverSenders.remove(senderRemoved.getBrokerConsumer());
      }
   }

   public void addReceiver(Receiver receiver) throws Exception {
      try {
         ProtonServerReceiverContext protonReceiver = new ProtonServerReceiverContext(sessionSPI, connection, this, receiver);
         protonReceiver.initialise();
         receivers.put(receiver, protonReceiver);
         receiver.setContext(protonReceiver);
         receiver.open();
      } catch (ActiveMQAMQPException e) {
         receivers.remove(receiver);
         receiver.setTarget(null);
         receiver.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
         receiver.close();
      }
   }

}
