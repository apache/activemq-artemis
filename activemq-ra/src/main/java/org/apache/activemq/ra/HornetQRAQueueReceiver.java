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
package org.apache.activemq.ra;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;


/**
 * A wrapper for a queue receiver
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAQueueReceiver extends HornetQRAMessageConsumer implements QueueReceiver
{
   /** Whether trace is enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param consumer the queue receiver
    * @param session the session
    */
   public HornetQRAQueueReceiver(final QueueReceiver consumer, final HornetQRASession session)
   {
      super(consumer, session);

      if (HornetQRAQueueReceiver.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + consumer + ", " + session + ")");
      }
   }

   /**
    * Get queue
    * @return The queue
    * @exception JMSException Thrown if an error occurs
    */
   public Queue getQueue() throws JMSException
   {
      if (HornetQRAQueueReceiver.trace)
      {
         HornetQRALogger.LOGGER.trace("getQueue()");
      }

      checkState();
      return ((QueueReceiver)consumer).getQueue();
   }
}
