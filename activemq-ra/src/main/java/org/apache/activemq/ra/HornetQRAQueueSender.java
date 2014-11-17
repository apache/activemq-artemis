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
package org.apache.activemq6.ra;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;


/**
 * HornetQQueueSender.
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAQueueSender extends HornetQRAMessageProducer implements QueueSender
{
   /** Whether trace is enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param producer the producer
    * @param session the session
    */
   public HornetQRAQueueSender(final QueueSender producer, final HornetQRASession session)
   {
      super(producer, session);

      if (HornetQRAQueueSender.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + producer + ", " + session + ")");
      }
   }

   /**
    * Get queue
    * @return The queue
    * @exception JMSException Thrown if an error occurs
    */
   public Queue getQueue() throws JMSException
   {
      if (HornetQRAQueueSender.trace)
      {
         HornetQRALogger.LOGGER.trace("getQueue()");
      }

      return ((QueueSender)producer).getQueue();
   }

   /**
    * Send message
    * @param destination The destination
    * @param message The message
    * @param deliveryMode The delivery mode
    * @param priority The priority
    * @param timeToLive The time to live
    * @exception JMSException Thrown if an error occurs
    */
   public void send(final Queue destination,
                    final Message message,
                    final int deliveryMode,
                    final int priority,
                    final long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAQueueSender.trace)
         {
            HornetQRALogger.LOGGER.trace("send " + this +
                                           " destination=" +
                                           destination +
                                           " message=" +
                                           message +
                                           " deliveryMode=" +
                                           deliveryMode +
                                           " priority=" +
                                           priority +
                                           " ttl=" +
                                           timeToLive);
         }

         checkState();
         producer.send(destination, message, deliveryMode, priority, timeToLive);

         if (HornetQRAQueueSender.trace)
         {
            HornetQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Send message
    * @param destination The destination
    * @param message The message
    * @exception JMSException Thrown if an error occurs
    */
   public void send(final Queue destination, final Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAQueueSender.trace)
         {
            HornetQRALogger.LOGGER.trace("send " + this + " destination=" + destination + " message=" + message);
         }

         checkState();
         producer.send(destination, message);

         if (HornetQRAQueueSender.trace)
         {
            HornetQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
      }
      finally
      {
         session.unlock();
      }
   }
}
