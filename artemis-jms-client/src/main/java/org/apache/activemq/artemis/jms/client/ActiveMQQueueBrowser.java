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
package org.apache.activemq.artemis.jms.client;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;
import java.util.NoSuchElementException;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.utils.SelectorTranslator;

/**
 * ActiveMQ Artemis implementation of a JMS QueueBrowser.
 */
public final class ActiveMQQueueBrowser implements QueueBrowser {
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final ConnectionFactoryOptions options;

   private final ClientSession session;

   private ClientConsumer consumer;

   private final ActiveMQQueue queue;

   private SimpleString filterString;

   // Constructors ---------------------------------------------------------------------------------

   protected ActiveMQQueueBrowser(final ConnectionFactoryOptions options,
                                  final ActiveMQQueue queue,
                                  final String messageSelector,
                                  final ClientSession session) throws JMSException {
      this.options = options;
      this.session = session;
      this.queue = queue;
      if (messageSelector != null) {
         filterString = new SimpleString(SelectorTranslator.convertToActiveMQFilterString(messageSelector));
      }
   }

   // QueueBrowser implementation -------------------------------------------------------------------

   @Override
   public void close() throws JMSException {
      if (consumer != null) {
         try {
            consumer.close();
         } catch (ActiveMQException e) {
            throw JMSExceptionHelper.convertFromActiveMQException(e);
         }
      }
   }

   @Override
   public Enumeration getEnumeration() throws JMSException {
      try {
         close();

         consumer = session.createConsumer(queue.getSimpleAddress(), filterString, true);

         return new BrowserEnumeration();
      } catch (ActiveMQException e) {
         throw JMSExceptionHelper.convertFromActiveMQException(e);
      }

   }

   @Override
   public String getMessageSelector() throws JMSException {
      return filterString == null ? null : filterString.toString();
   }

   @Override
   public Queue getQueue() throws JMSException {
      return queue;
   }

   // Public ---------------------------------------------------------------------------------------

   @Override
   public String toString() {
      return "ActiveMQQueueBrowser->" + consumer;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private final class BrowserEnumeration implements Enumeration<ActiveMQMessage> {

      ClientMessage current = null;

      @Override
      public boolean hasMoreElements() {
         if (current == null) {
            try {
               current = consumer.receiveImmediate();
            } catch (ActiveMQException e) {
               return false;
            }
         }
         return current != null;
      }

      @Override
      public ActiveMQMessage nextElement() {
         ActiveMQMessage msg;
         if (hasMoreElements()) {
            ClientMessage next = current;
            current = null;
            msg = ActiveMQMessage.createMessage(next, session, options);
            try {
               msg.doBeforeReceive();
            } catch (Exception e) {
               ActiveMQJMSClientLogger.LOGGER.errorCreatingMessage(msg.getCoreMessage().toString(), e);

               return null;
            }
            return msg;
         } else {
            throw new NoSuchElementException();
         }
      }
   }
}
