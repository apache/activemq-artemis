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

import java.util.Map;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.context.AbstractConnectionContext;
import org.proton.plug.context.AbstractProtonContextSender;
import org.proton.plug.context.AbstractProtonSessionContext;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.exceptions.ActiveMQAMQPInternalErrorException;
import org.proton.plug.logger.ActiveMQAMQPProtocolMessageBundle;
import org.proton.plug.context.ProtonPlugSender;
import org.apache.qpid.proton.amqp.messaging.Source;

public class ProtonServerSenderContext extends AbstractProtonContextSender implements ProtonPlugSender
{

   private static final Symbol SELECTOR = Symbol.getSymbol("jms-selector");
   private static final Symbol COPY = Symbol.valueOf("copy");

   private Object brokerConsumer;

   public ProtonServerSenderContext(AbstractConnectionContext connection, Sender sender, AbstractProtonSessionContext protonSession, AMQPSessionCallback server)
   {
      super(connection, sender, protonSession, server);
   }

   public Object getBrokerConsumer()
   {
      return brokerConsumer;
   }

   public void onFlow(int currentCredits)
   {
      super.onFlow(currentCredits);
      sessionSPI.onFlowConsumer(brokerConsumer, currentCredits);
   }

   /*
* start the session
* */
   public void start() throws ActiveMQAMQPException
   {
      super.start();
      // protonSession.getServerSession().start();

      //todo add flow control
      try
      {
         // to do whatever you need to make the broker start sending messages to the consumer
         sessionSPI.startSender(brokerConsumer);
         //protonSession.getServerSession().receiveConsumerCredits(consumerID, -1);
      }
      catch (Exception e)
      {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorStartingConsumer(e.getMessage());
      }
   }

   /**
    * create the actual underlying ActiveMQ Artemis Server Consumer
    */
   @Override
   public void initialise() throws Exception
   {
      super.initialise();

      Source source = (Source) sender.getRemoteSource();

      String queue;

      String selector = null;
      Map filter = source == null ? null : source.getFilter();
      if (filter != null)
      {
         DescribedType value = (DescribedType) filter.get(SELECTOR);
         if (value != null)
         {
            selector = value.getDescribed().toString();
         }
      }

      if (source != null)
      {
         if (source.getDynamic())
         {
            //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
            // will be deleted on closing of the session
            queue = java.util.UUID.randomUUID().toString();
            try
            {
               sessionSPI.createTemporaryQueue(queue);
               //protonSession.getServerSession().createQueue(queue, queue, null, true, false);
            }
            catch (Exception e)
            {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
            }
            source.setAddress(queue);
         }
         else
         {
            //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
            //be a queue bound to it so we nee to check this.
            queue = source.getAddress();
            if (queue == null)
            {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressNotSet();
            }

            try
            {
               if (!sessionSPI.queueQuery(queue))
               {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.sourceAddressDoesntExist();
               }
            }
            catch (Exception e)
            {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
         }

         boolean browseOnly = source.getDistributionMode() != null && source.getDistributionMode().equals(COPY);
         try
         {
            brokerConsumer = sessionSPI.createSender(this, queue, selector, browseOnly);
         }
         catch (Exception e)
         {
            throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCreatingConsumer(e.getMessage());
         }
      }
   }

   /*
   * close the session
   * */
   public void close() throws ActiveMQAMQPException
   {
      super.close();
      try
      {
         sessionSPI.closeSender(brokerConsumer);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw new ActiveMQAMQPInternalErrorException(e.getMessage());
      }
   }


   public void onMessage(Delivery delivery) throws ActiveMQAMQPException
   {
      Object message = delivery.getContext();

      boolean preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;


      DeliveryState remoteState = delivery.getRemoteState();

      if (remoteState != null)
      {
         if (remoteState instanceof Accepted)
         {
            //we have to individual ack as we can't guarantee we will get the delivery updates (including acks) in order
            // from dealer, a perf hit but a must
            try
            {
               sessionSPI.ack(brokerConsumer, message);
            }
            catch (Exception e)
            {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorAcknowledgingMessage(message.toString(), e.getMessage());
            }
         }
         else if (remoteState instanceof Released)
         {
            try
            {
               sessionSPI.cancel(brokerConsumer, message, false);
            }
            catch (Exception e)
            {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         }
         else if (remoteState instanceof Rejected || remoteState instanceof Modified)
         {
            try
            {
               sessionSPI.cancel(brokerConsumer, message, true);
            }
            catch (Exception e)
            {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.errorCancellingMessage(message.toString(), e.getMessage());
            }
         }
         //todo add tag caching
         if (!preSettle)
         {
            protonSession.replaceTag(delivery.getTag());
         }

         synchronized (connection.getLock())
         {
            delivery.settle();
            sender.offer(1);
         }

      }
      else
      {
         //todo not sure if we need to do anything here
      }
   }

   @Override
   public synchronized void checkState()
   {
      super.checkState();
      sessionSPI.resumeDelivery(brokerConsumer);
   }


   /**
    * handle an out going message from ActiveMQ Artemis, send via the Proton Sender
    */
   public int deliverMessage(Object message, int deliveryCount) throws Exception
   {
      if (closed)
      {
         System.err.println("Message can't be delivered as it's closed");
         return 0;
      }

      //encode the message
      ProtonJMessage serverMessage;
      try
      {
         // This can be done a lot better here
         serverMessage = sessionSPI.encodeMessage(message, deliveryCount);
      }
      catch (Throwable e)
      {
         e.printStackTrace();
         throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
      }

      return performSend(serverMessage, message);
   }


}
