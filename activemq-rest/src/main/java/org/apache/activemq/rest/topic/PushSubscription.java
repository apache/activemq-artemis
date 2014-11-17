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
package org.apache.activemq.rest.topic;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.rest.HornetQRestLogger;
import org.apache.activemq.rest.queue.push.PushConsumer;
import org.apache.activemq.rest.queue.push.PushStore;
import org.apache.activemq.rest.queue.push.xml.PushRegistration;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class PushSubscription extends PushConsumer
{
   public PushSubscription(ClientSessionFactory factory, String destination, String id, PushRegistration registration, PushStore store)
   {
      super(factory, destination, id, registration, store);
   }

   @Override
   public void disableFromFailure()
   {
      super.disableFromFailure();
      if (registration.isDurable()) deleteSubscriberQueue();
   }

   protected void deleteSubscriberQueue()
   {
      String subscriptionName = registration.getDestination();
      ClientSession session = null;
      try
      {
         session = factory.createSession();

         session.deleteQueue(subscriptionName);
      }
      catch (ActiveMQException e)
      {
         HornetQRestLogger.LOGGER.errorDeletingSubscriberQueue(e);
      }
      finally
      {
         try
         {
            if (session != null)
               session.close();
         }
         catch (ActiveMQException e)
         {
         }
      }
   }
}
