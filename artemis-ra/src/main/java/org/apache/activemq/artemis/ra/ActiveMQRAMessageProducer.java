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

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * ActiveMQMessageProducer.
 */
public class ActiveMQRAMessageProducer implements MessageProducer {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   /**
    * The wrapped message producer
    */
   protected MessageProducer producer;

   /**
    * The session for this consumer
    */
   protected ActiveMQRASession session;

   /**
    * Create a new wrapper
    *
    * @param producer the producer
    * @param session  the session
    */
   public ActiveMQRAMessageProducer(final MessageProducer producer, final ActiveMQRASession session) {
      this.producer = producer;
      this.session = session;

      if (logger.isTraceEnabled()) {
         logger.trace("new ActiveMQMessageProducer {}  producer={} session={}", this, producer, session);
      }
   }

   /**
    * Close
    *
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void close() throws JMSException {
      logger.trace("close {}", this);

      try {
         closeProducer();
      } finally {
         session.removeProducer(this);
      }
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
   public void send(final Destination destination,
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
   public void send(final Destination destination, final Message message) throws JMSException {
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

   /**
    * Send message
    *
    * @param message      The message
    * @param deliveryMode The delivery mode
    * @param priority     The priority
    * @param timeToLive   The time to live
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void send(final Message message,
                    final int deliveryMode,
                    final int priority,
                    final long timeToLive) throws JMSException {
      session.lock();
      try {
         if (logger.isTraceEnabled()) {
            logger.trace("send {} message={} deliveryMode={} priority={} ttl={}",
               this, message, deliveryMode, priority, timeToLive);
         }

         checkState();

         producer.send(message, deliveryMode, priority, timeToLive);

         logger.trace("sent {} result={}", this, message);
      } finally {
         session.unlock();
      }
   }

   /**
    * Send message
    *
    * @param message The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void send(final Message message) throws JMSException {
      session.lock();
      try {
         logger.trace("send {} result={}", this, message);

         checkState();

         producer.send(message);

         logger.trace("sent {} result={}", this, message);
      } finally {
         session.unlock();
      }
   }

   /**
    * Get the delivery mode
    *
    * @return The mode
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int getDeliveryMode() throws JMSException {
      logger.trace("getRoutingType()");

      return producer.getDeliveryMode();
   }

   /**
    * Get the destination
    *
    * @return The destination
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Destination getDestination() throws JMSException {
      logger.trace("getDestination()");

      return producer.getDestination();
   }

   /**
    * Disable message id
    *
    * @return True if disable
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public boolean getDisableMessageID() throws JMSException {
      logger.trace("getDisableMessageID()");

      return producer.getDisableMessageID();
   }

   /**
    * Disable message timestamp
    *
    * @return True if disable
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public boolean getDisableMessageTimestamp() throws JMSException {
      logger.trace("getDisableMessageTimestamp()");

      return producer.getDisableMessageTimestamp();
   }

   /**
    * Get the priority
    *
    * @return The priority
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public int getPriority() throws JMSException {
      logger.trace("getPriority()");

      return producer.getPriority();
   }

   /**
    * Get the time to live
    *
    * @return The ttl
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public long getTimeToLive() throws JMSException {
      logger.trace("getTimeToLive()");

      return producer.getTimeToLive();
   }

   /**
    * Set the delivery mode
    *
    * @param deliveryMode The mode
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setDeliveryMode(final int deliveryMode) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("setRoutingType({})", deliveryMode);
      }

      producer.setDeliveryMode(deliveryMode);
   }

   /**
    * Set disable message id
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setDisableMessageID(final boolean value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("setDisableMessageID({})", value);
      }

      producer.setDisableMessageID(value);
   }

   /**
    * Set disable message timestamp
    *
    * @param value The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setDisableMessageTimestamp(final boolean value) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("setDisableMessageTimestamp({})", value);
      }

      producer.setDisableMessageTimestamp(value);
   }

   /**
    * Set the priority
    *
    * @param defaultPriority The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setPriority(final int defaultPriority) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("setPriority({})", defaultPriority);
      }

      producer.setPriority(defaultPriority);
   }

   /**
    * Set the ttl
    *
    * @param timeToLive The value
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setTimeToLive(final long timeToLive) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("setTimeToLive({})", timeToLive);
      }

      producer.setTimeToLive(timeToLive);
   }

   @Override
   public void setDeliveryDelay(long deliveryDelay) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("setDeliveryDelay({})", deliveryDelay);
      }
      producer.setDeliveryDelay(deliveryDelay);
   }

   @Override
   public long getDeliveryDelay() throws JMSException {
      logger.trace("getDeliveryDelay()");

      return producer.getDeliveryDelay();
   }

   @Override
   public void send(Message message, CompletionListener completionListener) throws JMSException {
      logger.trace("send({}, {})", message, completionListener);

      producer.send(message, completionListener);
   }

   @Override
   public void send(Message message,
                    int deliveryMode,
                    int priority,
                    long timeToLive,
                    CompletionListener completionListener) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("send({}, {}, {}, {}, {})", message, deliveryMode, priority, timeToLive, completionListener);
      }
      producer.send(message, deliveryMode, priority, timeToLive, completionListener);
   }

   @Override
   public void send(Destination destination,
                    Message message,
                    CompletionListener completionListener) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("send({}, {}, {})", destination, message, completionListener);
      }
      producer.send(destination, message, completionListener);
   }

   @Override
   public void send(Destination destination,
                    Message message,
                    int deliveryMode,
                    int priority,
                    long timeToLive,
                    CompletionListener completionListener) throws JMSException {
      if (logger.isTraceEnabled()) {
         logger.trace("send({}, {}, {}, {}, {}, {})", destination, message, deliveryMode, priority, timeToLive, completionListener);
      }
      producer.send(destination, message, deliveryMode, priority, timeToLive, completionListener);
   }

   /**
    * Check state
    *
    * @throws JMSException Thrown if an error occurs
    */
   void checkState() throws JMSException {
      session.checkState();
   }

   /**
    * Close producer
    *
    * @throws JMSException Thrown if an error occurs
    */
   void closeProducer() throws JMSException {
      producer.close();
   }
}
