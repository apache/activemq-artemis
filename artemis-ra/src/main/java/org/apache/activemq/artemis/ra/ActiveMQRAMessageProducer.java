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

/**
 * ActiveMQMessageProducer.
 */
public class ActiveMQRAMessageProducer implements MessageProducer {

   /**
    * Whether trace is enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

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

      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("new ActiveMQMessageProducer " + this +
                                          " producer=" +
                                          producer +
                                          " session=" +
                                          session);
      }
   }

   /**
    * Close
    *
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void close() throws JMSException {
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("close " + this);
      }
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
         if (ActiveMQRAMessageProducer.trace) {
            ActiveMQRALogger.LOGGER.trace("send " + this +
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

         if (ActiveMQRAMessageProducer.trace) {
            ActiveMQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
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
         if (ActiveMQRAMessageProducer.trace) {
            ActiveMQRALogger.LOGGER.trace("send " + this + " destination=" + destination + " message=" + message);
         }

         checkState();

         producer.send(destination, message);

         if (ActiveMQRAMessageProducer.trace) {
            ActiveMQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
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
         if (ActiveMQRAMessageProducer.trace) {
            ActiveMQRALogger.LOGGER.trace("send " + this +
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

         producer.send(message, deliveryMode, priority, timeToLive);

         if (ActiveMQRAMessageProducer.trace) {
            ActiveMQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
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
         if (ActiveMQRAMessageProducer.trace) {
            ActiveMQRALogger.LOGGER.trace("send " + this + " message=" + message);
         }

         checkState();

         producer.send(message);

         if (ActiveMQRAMessageProducer.trace) {
            ActiveMQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("getRoutingType()");
      }

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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("getDestination()");
      }

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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("getDisableMessageID()");
      }

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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("getDisableMessageTimestamp()");
      }

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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("getPriority()");
      }

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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("getTimeToLive()");
      }

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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("setRoutingType(" + deliveryMode + ")");
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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("setDisableMessageID(" + value + ")");
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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("setDisableMessageTimestamp(" + value + ")");
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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("setPriority(" + defaultPriority + ")");
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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("setTimeToLive(" + timeToLive + ")");
      }

      producer.setTimeToLive(timeToLive);
   }

   @Override
   public void setDeliveryDelay(long deliveryDelay) throws JMSException {
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("setDeliveryDelay(" + deliveryDelay + ")");
      }
      producer.setDeliveryDelay(deliveryDelay);
   }

   @Override
   public long getDeliveryDelay() throws JMSException {
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("getDeliveryDelay()");
      }
      return producer.getDeliveryDelay();
   }

   @Override
   public void send(Message message, CompletionListener completionListener) throws JMSException {
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("send(" + message + ", " + completionListener + ")");
      }
      producer.send(message, completionListener);
   }

   @Override
   public void send(Message message,
                    int deliveryMode,
                    int priority,
                    long timeToLive,
                    CompletionListener completionListener) throws JMSException {
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("send(" + message + ", " + deliveryMode + ", " + priority + ", " + timeToLive +
                                          ", " + completionListener + ")");
      }
      producer.send(message, deliveryMode, priority, timeToLive, completionListener);
   }

   @Override
   public void send(Destination destination,
                    Message message,
                    CompletionListener completionListener) throws JMSException {
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("send(" + destination + ", " + message + ", " + completionListener + ")");
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
      if (ActiveMQRAMessageProducer.trace) {
         ActiveMQRALogger.LOGGER.trace("send(" + destination + ", " + message + ", " + deliveryMode + ", " + priority +
                                          ", " + timeToLive + ", " + completionListener + ")");
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
