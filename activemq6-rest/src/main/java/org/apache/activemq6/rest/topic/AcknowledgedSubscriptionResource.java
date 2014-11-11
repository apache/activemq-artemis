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
package org.apache.activemq6.rest.topic;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.rest.queue.AcknowledgedQueueConsumer;
import org.apache.activemq6.rest.queue.DestinationServiceManager;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class AcknowledgedSubscriptionResource extends AcknowledgedQueueConsumer implements Subscription
{
   private boolean durable;
   private long timeout;
   private boolean deleteWhenIdle;

   public AcknowledgedSubscriptionResource(ClientSessionFactory factory, String destination, String id, DestinationServiceManager serviceManager, String selector, boolean durable, Long timeout) throws HornetQException
   {
      super(factory, destination, id, serviceManager, selector);
      this.timeout = timeout;
      this.durable = durable;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public void setDurable(boolean durable)
   {
      this.durable = durable;
   }

   public long getTimeout()
   {
      return timeout;
   }

   public void setTimeout(long timeout)
   {
      this.timeout = timeout;
   }

   public boolean isDeleteWhenIdle()
   {
      return deleteWhenIdle;
   }

   public void setDeleteWhenIdle(boolean deleteWhenIdle)
   {
      this.deleteWhenIdle = deleteWhenIdle;
   }
}
