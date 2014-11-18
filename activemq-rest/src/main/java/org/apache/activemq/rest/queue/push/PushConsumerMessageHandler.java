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
package org.apache.activemq.rest.queue.push;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.MessageHandler;
import org.apache.activemq.rest.HornetQRestLogger;

public class PushConsumerMessageHandler implements MessageHandler
{
   private ClientSession session;
   private PushConsumer pushConsumer;

   PushConsumerMessageHandler(PushConsumer pushConsumer, ClientSession session)
   {
      this.pushConsumer = pushConsumer;
      this.session = session;
   }

   @Override
   public void onMessage(ClientMessage clientMessage)
   {
      HornetQRestLogger.LOGGER.debug(this + ": receiving " + clientMessage);

      try
      {
         clientMessage.acknowledge();
         HornetQRestLogger.LOGGER.debug(this + ": acknowledged " + clientMessage);
      }
      catch (ActiveMQException e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }

      HornetQRestLogger.LOGGER.debug(this + ": pushing " + clientMessage + " via " + pushConsumer.getStrategy());
      boolean acknowledge = pushConsumer.getStrategy().push(clientMessage);

      if (acknowledge)
      {
         try
         {
            HornetQRestLogger.LOGGER.debug("Acknowledging: " + clientMessage.getMessageID());
            session.commit();
            return;
         }
         catch (ActiveMQException e)
         {
            throw new RuntimeException(e);
         }
      }
      else
      {
         try
         {
            session.rollback();
         }
         catch (ActiveMQException e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }
         if (pushConsumer.getRegistration().isDisableOnFailure())
         {
            HornetQRestLogger.LOGGER.errorPushingMessage(pushConsumer.getRegistration().getTarget());
            pushConsumer.disableFromFailure();
            return;
         }
      }
   }
}
