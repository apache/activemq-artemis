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
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * ActiveMQQueueSender.
 */
public class ActiveMQRATopicPublisher extends ActiveMQRAMessageProducer implements TopicPublisher {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   /**
    * Create a new wrapper
    *
    * @param producer the producer
    * @param session  the session
    */
   public ActiveMQRATopicPublisher(final TopicPublisher producer, final ActiveMQRASession session) {
      super(producer, session);

      if (logger.isTraceEnabled()) {
         logger.trace("constructor(" + producer + ", " + session + ")");
      }
   }

   /**
    * Get the topic
    *
    * @return The topic
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Topic getTopic() throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("getTopic()");
      }

      return ((TopicPublisher) producer).getTopic();
   }

   /**
    * Publish message
    *
    * @param message      The message
    * @param deliveryMode The delivery mode
    * @param priority     The priority
    * @param timeToLive   The time to live
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void publish(final Message message,
                       final int deliveryMode,
                       final int priority,
                       final long timeToLive) throws JMSException {
      session.lock();
      try {
         if (logger.isTraceEnabled()) {
            logger.trace("send " + this +
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

         ((TopicPublisher) producer).publish(message, deliveryMode, priority, timeToLive);

         if (logger.isTraceEnabled()) {
            logger.trace("sent " + this + " result=" + message);
         }
      } finally {
         session.unlock();
      }
   }

   /**
    * Publish message
    *
    * @param message The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void publish(final Message message) throws JMSException {
      session.lock();
      try {
         if (logger.isTraceEnabled()) {
            logger.trace("send " + this + " message=" + message);
         }

         checkState();

         ((TopicPublisher) producer).publish(message);

         if (logger.isTraceEnabled()) {
            logger.trace("sent " + this + " result=" + message);
         }
      } finally {
         session.unlock();
      }
   }

   /**
    * Publish message
    *
    * @param destination  The destination
    * @param message      The message
    * @param deliveryMode The delivery mode
    * @param priority     The priority
    * @param timeToLive   The time to live
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void publish(final Topic destination,
                       final Message message,
                       final int deliveryMode,
                       final int priority,
                       final long timeToLive) throws JMSException {
      session.lock();
      try {
         if (logger.isTraceEnabled()) {
            logger.trace("send " + this +
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

         ((TopicPublisher) producer).publish(destination, message, deliveryMode, priority, timeToLive);

         if (logger.isTraceEnabled()) {
            logger.trace("sent " + this + " result=" + message);
         }
      } finally {
         session.unlock();
      }
   }

   /**
    * Publish message
    *
    * @param destination The destination
    * @param message     The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void publish(final Topic destination, final Message message) throws JMSException {
      session.lock();
      try {
         if (logger.isTraceEnabled()) {
            logger.trace("send " + this + " destination=" + destination + " message=" + message);
         }

         checkState();

         ((TopicPublisher) producer).publish(destination, message);

         if (logger.isTraceEnabled()) {
            logger.trace("sent " + this + " result=" + message);
         }
      } finally {
         session.unlock();
      }
   }
}
