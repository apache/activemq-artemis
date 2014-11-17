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
package org.apache.activemq.core.server.impl;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.TransientQueueManager;
import org.apache.activemq.utils.ReferenceCounterUtil;

/**
 * @author Clebert Suconic
 */

public class TransientQueueManagerImpl implements TransientQueueManager
{
   private final SimpleString queueName;

   private final HornetQServer server;

   private final Runnable runnable = new Runnable()
   {
      public void run()
      {
         try
         {
            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug("deleting temporary queue " + queueName);
            }

            try
            {
               server.destroyQueue(queueName, null, false);
            }
            catch (ActiveMQException e)
            {
               HornetQServerLogger.LOGGER.warn("Error on deleting queue " + queueName + ", " + e.getMessage(), e);
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorRemovingTempQueue(e, queueName);
         }
      }
   };

   private final ReferenceCounterUtil referenceCounterUtil = new ReferenceCounterUtil(runnable);

   public TransientQueueManagerImpl(HornetQServer server, SimpleString queueName)
   {
      this.server = server;

      this.queueName = queueName;
   }

   @Override
   public int increment()
   {
      return referenceCounterUtil.increment();
   }

   @Override
   public int decrement()
   {
      return referenceCounterUtil.decrement();
   }

   @Override
   public SimpleString getQueueName()
   {
      return queueName;
   }
}
