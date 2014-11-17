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
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAMessageConsumer implements MessageConsumer
{
   /** Whether trace is enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /** The wrapped message consumer */
   protected MessageConsumer consumer;

   /** The session for this consumer */
   protected HornetQRASession session;

   /**
    * Create a new wrapper
    * @param consumer the consumer
    * @param session the session
    */
   public HornetQRAMessageConsumer(final MessageConsumer consumer, final HornetQRASession session)
   {
      this.consumer = consumer;
      this.session = session;

      if (HornetQRAMessageConsumer.trace)
      {
         HornetQRALogger.LOGGER.trace("new HornetQMessageConsumer " + this +
                                            " consumer=" +
                                            consumer +
                                            " session=" +
                                            session);
      }
   }

   /**
    * Close
    * @exception JMSException Thrown if an error occurs
    */
   public void close() throws JMSException
   {
      if (HornetQRAMessageConsumer.trace)
      {
         HornetQRALogger.LOGGER.trace("close " + this);
      }
      try
      {
         closeConsumer();
      }
      finally
      {
         session.removeConsumer(this);
      }
   }

   /**
    * Check state
    * @exception JMSException Thrown if an error occurs
    */
   void checkState() throws JMSException
   {
      if (HornetQRAMessageConsumer.trace)
      {
         HornetQRALogger.LOGGER.trace("checkState()");
      }
      session.checkState();
   }

   /**
    * Get message listener
    * @return The listener
    * @exception JMSException Thrown if an error occurs
    */
   public MessageListener getMessageListener() throws JMSException
   {
      if (HornetQRAMessageConsumer.trace)
      {
         HornetQRALogger.LOGGER.trace("getMessageListener()");
      }

      checkState();
      session.checkStrict();
      return consumer.getMessageListener();
   }

   /**
    * Set message listener
    * @param listener The listener
    * @exception JMSException Thrown if an error occurs
    */
   public void setMessageListener(final MessageListener listener) throws JMSException
   {
      session.lock();
      try
      {
         checkState();
         session.checkStrict();
         if (listener == null)
         {
            consumer.setMessageListener(null);
         }
         else
         {
            consumer.setMessageListener(wrapMessageListener(listener));
         }
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Get message selector
    * @return The selector
    * @exception JMSException Thrown if an error occurs
    */
   public String getMessageSelector() throws JMSException
   {
      if (HornetQRAMessageConsumer.trace)
      {
         HornetQRALogger.LOGGER.trace("getMessageSelector()");
      }

      checkState();
      return consumer.getMessageSelector();
   }

   /**
    * Receive
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public Message receive() throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAMessageConsumer.trace)
         {
            HornetQRALogger.LOGGER.trace("receive " + this);
         }

         checkState();
         Message message = consumer.receive();

         if (HornetQRAMessageConsumer.trace)
         {
            HornetQRALogger.LOGGER.trace("received " + this + " result=" + message);
         }

         if (message == null)
         {
            return null;
         }
         else
         {
            return wrapMessage(message);
         }
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Receive
    * @param timeout The timeout value
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public Message receive(final long timeout) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAMessageConsumer.trace)
         {
            HornetQRALogger.LOGGER.trace("receive " + this + " timeout=" + timeout);
         }

         checkState();
         Message message = consumer.receive(timeout);

         if (HornetQRAMessageConsumer.trace)
         {
            HornetQRALogger.LOGGER.trace("received " + this + " result=" + message);
         }

         if (message == null)
         {
            return null;
         }
         else
         {
            return wrapMessage(message);
         }
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Receive
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public Message receiveNoWait() throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAMessageConsumer.trace)
         {
            HornetQRALogger.LOGGER.trace("receiveNoWait " + this);
         }

         checkState();
         Message message = consumer.receiveNoWait();

         if (HornetQRAMessageConsumer.trace)
         {
            HornetQRALogger.LOGGER.trace("received " + this + " result=" + message);
         }

         if (message == null)
         {
            return null;
         }
         else
         {
            return wrapMessage(message);
         }
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Close consumer
    * @exception JMSException Thrown if an error occurs
    */
   void closeConsumer() throws JMSException
   {
      if (HornetQRAMessageConsumer.trace)
      {
         HornetQRALogger.LOGGER.trace("closeConsumer()");
      }

      consumer.close();
   }

   /**
    * Wrap message
    * @param message The message to be wrapped
    * @return The wrapped message
    */
   Message wrapMessage(final Message message)
   {
      if (HornetQRAMessageConsumer.trace)
      {
         HornetQRALogger.LOGGER.trace("wrapMessage(" + message + ")");
      }

      if (message instanceof BytesMessage)
      {
         return new HornetQRABytesMessage((BytesMessage)message, session);
      }
      else if (message instanceof MapMessage)
      {
         return new HornetQRAMapMessage((MapMessage)message, session);
      }
      else if (message instanceof ObjectMessage)
      {
         return new HornetQRAObjectMessage((ObjectMessage)message, session);
      }
      else if (message instanceof StreamMessage)
      {
         return new HornetQRAStreamMessage((StreamMessage)message, session);
      }
      else if (message instanceof TextMessage)
      {
         return new HornetQRATextMessage((TextMessage)message, session);
      }
      return new HornetQRAMessage(message, session);
   }

   /**
    * Wrap message listener
    * @param listener The listener to be wrapped
    * @return The wrapped listener
    */
   MessageListener wrapMessageListener(final MessageListener listener)
   {
      if (HornetQRAMessageConsumer.trace)
      {
         HornetQRALogger.LOGGER.trace("getMessageSelector()");
      }

      return new HornetQRAMessageListener(listener, this);
   }
}
