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
package org.apache.activemq.jms.client;

import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.MessageHandler;
import org.apache.activemq.api.jms.HornetQJMSConstants;

/**
 *
 * A JMSMessageListenerWrapper
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JMSMessageListenerWrapper implements MessageHandler
{
   private final HornetQConnection connection;

   private final HornetQSession session;

   private final MessageListener listener;

   private final ClientConsumer consumer;

   private final boolean transactedOrClientAck;

   private final boolean individualACK;

   protected JMSMessageListenerWrapper(final HornetQConnection connection,
                                       final HornetQSession session,
                                       final ClientConsumer consumer,
                                       final MessageListener listener,
                                       final int ackMode)
   {
      this.connection = connection;

      this.session = session;

      this.consumer = consumer;

      this.listener = listener;

      transactedOrClientAck = (ackMode == Session.SESSION_TRANSACTED || ackMode == Session.CLIENT_ACKNOWLEDGE) || session.isXA();

      individualACK = (ackMode == HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
   }

   /**
    * In this method we apply the JMS acknowledgement and redelivery semantics
    * as per JMS spec
    */
   public void onMessage(final ClientMessage message)
   {
      HornetQMessage msg = HornetQMessage.createMessage(message, session.getCoreSession());

      if (individualACK)
      {
         msg.setIndividualAcknowledge();
      }

      try
      {
         msg.doBeforeReceive();
      }
      catch (Exception e)
      {
         HornetQJMSClientLogger.LOGGER.errorPreparingMessageForReceipt(e);

         return;
      }

      if (transactedOrClientAck)
      {
         try
         {
            message.acknowledge();
         }
         catch (HornetQException e)
         {
            HornetQJMSClientLogger.LOGGER.errorProcessingMessage(e);
         }
      }

      try
      {
         connection.getThreadAwareContext().setCurrentThread(false);
         listener.onMessage(msg);
      }
      catch (RuntimeException e)
      {
         // See JMS 1.1 spec, section 4.5.2

         HornetQJMSClientLogger.LOGGER.onMessageError(e);

         if (!transactedOrClientAck)
         {
            try
            {
               if (individualACK)
               {
                  message.individualAcknowledge();
               }

               session.getCoreSession().rollback(true);

               session.setRecoverCalled(true);
            }
            catch (Exception e2)
            {
               HornetQJMSClientLogger.LOGGER.errorRecoveringSession(e2);
            }
         }
      }
      finally
      {
         connection.getThreadAwareContext().clearCurrentThread(false);
      }
      if (!session.isRecoverCalled() && !individualACK)
      {
         try
         {
            // We don't want to call this if the consumer was closed from inside onMessage
            if (!consumer.isClosed() && !transactedOrClientAck)
            {
               message.acknowledge();
            }
         }
         catch (HornetQException e)
         {
            HornetQJMSClientLogger.LOGGER.errorProcessingMessage(e);
         }
      }

      session.setRecoverCalled(false);
   }
}
