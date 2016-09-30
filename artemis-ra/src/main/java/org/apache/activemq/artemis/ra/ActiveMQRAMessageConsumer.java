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

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

/**
 * A wrapper for a message consumer
 */
public class ActiveMQRAMessageConsumer implements MessageConsumer {

   /**
    * Whether trace is enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /**
    * The wrapped message consumer
    */
   protected MessageConsumer consumer;

   /**
    * The session for this consumer
    */
   protected ActiveMQRASession session;

   /**
    * Create a new wrapper
    *
    * @param consumer the consumer
    * @param session  the session
    */
   public ActiveMQRAMessageConsumer(final MessageConsumer consumer, final ActiveMQRASession session) {
      this.consumer = consumer;
      this.session = session;

      if (ActiveMQRAMessageConsumer.trace) {
         ActiveMQRALogger.LOGGER.trace("new ActiveMQMessageConsumer " + this +
                                          " consumer=" +
                                          consumer +
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
      if (ActiveMQRAMessageConsumer.trace) {
         ActiveMQRALogger.LOGGER.trace("close " + this);
      }
      try {
         closeConsumer();
      } finally {
         session.removeConsumer(this);
      }
   }

   /**
    * Check state
    *
    * @throws JMSException Thrown if an error occurs
    */
   void checkState() throws JMSException {
      if (ActiveMQRAMessageConsumer.trace) {
         ActiveMQRALogger.LOGGER.trace("checkState()");
      }
      session.checkState();
   }

   /**
    * Get message listener
    *
    * @return The listener
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public MessageListener getMessageListener() throws JMSException {
      if (ActiveMQRAMessageConsumer.trace) {
         ActiveMQRALogger.LOGGER.trace("getMessageListener()");
      }

      checkState();
      session.checkStrict();
      return consumer.getMessageListener();
   }

   /**
    * Set message listener
    *
    * @param listener The listener
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setMessageListener(final MessageListener listener) throws JMSException {
      session.lock();
      try {
         checkState();
         session.checkStrict();
         if (listener == null) {
            consumer.setMessageListener(null);
         } else {
            consumer.setMessageListener(wrapMessageListener(listener));
         }
      } finally {
         session.unlock();
      }
   }

   /**
    * Get message selector
    *
    * @return The selector
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public String getMessageSelector() throws JMSException {
      if (ActiveMQRAMessageConsumer.trace) {
         ActiveMQRALogger.LOGGER.trace("getMessageSelector()");
      }

      checkState();
      return consumer.getMessageSelector();
   }

   /**
    * Receive
    *
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Message receive() throws JMSException {
      session.lock();
      try {
         if (ActiveMQRAMessageConsumer.trace) {
            ActiveMQRALogger.LOGGER.trace("receive " + this);
         }

         checkState();
         Message message = consumer.receive();

         if (ActiveMQRAMessageConsumer.trace) {
            ActiveMQRALogger.LOGGER.trace("received " + this + " result=" + message);
         }

         if (message == null) {
            return null;
         } else {
            return wrapMessage(message);
         }
      } finally {
         session.unlock();
      }
   }

   /**
    * Receive
    *
    * @param timeout The timeout value
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Message receive(final long timeout) throws JMSException {
      session.lock();
      try {
         if (ActiveMQRAMessageConsumer.trace) {
            ActiveMQRALogger.LOGGER.trace("receive " + this + " timeout=" + timeout);
         }

         checkState();
         Message message = consumer.receive(timeout);

         if (ActiveMQRAMessageConsumer.trace) {
            ActiveMQRALogger.LOGGER.trace("received " + this + " result=" + message);
         }

         if (message == null) {
            return null;
         } else {
            return wrapMessage(message);
         }
      } finally {
         session.unlock();
      }
   }

   /**
    * Receive
    *
    * @return The message
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Message receiveNoWait() throws JMSException {
      session.lock();
      try {
         if (ActiveMQRAMessageConsumer.trace) {
            ActiveMQRALogger.LOGGER.trace("receiveNoWait " + this);
         }

         checkState();
         Message message = consumer.receiveNoWait();

         if (ActiveMQRAMessageConsumer.trace) {
            ActiveMQRALogger.LOGGER.trace("received " + this + " result=" + message);
         }

         if (message == null) {
            return null;
         } else {
            return wrapMessage(message);
         }
      } finally {
         session.unlock();
      }
   }

   /**
    * Close consumer
    *
    * @throws JMSException Thrown if an error occurs
    */
   void closeConsumer() throws JMSException {
      if (ActiveMQRAMessageConsumer.trace) {
         ActiveMQRALogger.LOGGER.trace("closeConsumer()");
      }

      consumer.close();
   }

   /**
    * Wrap message
    *
    * @param message The message to be wrapped
    * @return The wrapped message
    */
   Message wrapMessage(final Message message) {
      if (ActiveMQRAMessageConsumer.trace) {
         ActiveMQRALogger.LOGGER.trace("wrapMessage(" + message + ")");
      }

      if (message instanceof BytesMessage) {
         return new ActiveMQRABytesMessage((BytesMessage) message, session);
      } else if (message instanceof MapMessage) {
         return new ActiveMQRAMapMessage((MapMessage) message, session);
      } else if (message instanceof ObjectMessage) {
         return new ActiveMQRAObjectMessage((ObjectMessage) message, session);
      } else if (message instanceof StreamMessage) {
         return new ActiveMQRAStreamMessage((StreamMessage) message, session);
      } else if (message instanceof TextMessage) {
         return new ActiveMQRATextMessage((TextMessage) message, session);
      }
      return new ActiveMQRAMessage(message, session);
   }

   /**
    * Wrap message listener
    *
    * @param listener The listener to be wrapped
    * @return The wrapped listener
    */
   MessageListener wrapMessageListener(final MessageListener listener) {
      if (ActiveMQRAMessageConsumer.trace) {
         ActiveMQRALogger.LOGGER.trace("getMessageSelector()");
      }

      return new ActiveMQRAMessageListener(listener, this);
   }
}
