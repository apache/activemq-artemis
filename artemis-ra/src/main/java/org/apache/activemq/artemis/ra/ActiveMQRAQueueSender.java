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
package org.apache.activemq.artemis.ra;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * ActiveMQQueueSender.
 */
public class ActiveMQRAQueueSender extends ActiveMQRAMessageProducer implements QueueSender {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   /**
    * Create a new wrapper
    *
    * @param producer the producer
    * @param session  the session
    */
   public ActiveMQRAQueueSender(final QueueSender producer, final ActiveMQRASession session) {
      super(producer, session);

      if (logger.isTraceEnabled()) {
         logger.trace("constructor({}, {})", producer, session);
      }
   }

   /**
    * Get queue
    *
    * @return The queue
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Queue getQueue() throws JMSException {
      logger.trace("getQueue()");

      return ((QueueSender) producer).getQueue();
   }

   /**
    * Send message
    *
    * @param destination  The destination
    * @param message      The message
    * @param deliveryMode The delivery mode
    * @param priority     The priority
    * @param timeToLive   The time to live
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void send(final Queue destination,
                    final Message message,
                    final int deliveryMode,
                    final int priority,
                    final long timeToLive) throws JMSException {
      session.lock();
      try {
         if (logger.isTraceEnabled()) {
            logger.trace("send {} destination={} message={} deliveryMode={} priority={} ttl={}",
               this, destination, message, deliveryMode, priority, timeToLive);
         }

         checkState();
         producer.send(destination, message, deliveryMode, priority, timeToLive);

         logger.trace("sent {} result={}", this, message);
      } finally {
         session.unlock();
      }
   }

   /**
    * Send message
    *
    * @param destination The destination
    * @param message     The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void send(final Queue destination, final Message message) throws JMSException {
      session.lock();
      try {
         if (logger.isTraceEnabled()) {
            logger.trace("send {} destination={} message={}", this, destination, message);
         }

         checkState();
         producer.send(destination, message);

         logger.trace("sent {} result={}", this, message);
      } finally {
         session.unlock();
      }
   }
}
