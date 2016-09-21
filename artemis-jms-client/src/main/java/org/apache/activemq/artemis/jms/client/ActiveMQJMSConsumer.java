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

import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageListener;

public class ActiveMQJMSConsumer implements JMSConsumer {

   private final ActiveMQJMSContext context;
   private final MessageConsumer consumer;
   private Message messageFormatFailure;

   ActiveMQJMSConsumer(ActiveMQJMSContext context, MessageConsumer consumer) {
      this.context = context;
      this.consumer = consumer;
   }

   @Override
   public String getMessageSelector() {
      try {
         return consumer.getMessageSelector();
      }
      catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public MessageListener getMessageListener() throws JMSRuntimeException {
      try {
         return consumer.getMessageListener();
      }
      catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
      try {
         consumer.setMessageListener(new MessageListenerWrapper(listener));
      }
      catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public Message receive() {
      try {
         return context.setLastMessage(this, consumer.receive());
      }
      catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public Message receive(long timeout) {
      try {
         return context.setLastMessage(this, consumer.receive(timeout));
      }
      catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public Message receiveNoWait() {
      try {
         return context.setLastMessage(this, consumer.receiveNoWait());
      }
      catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public void close() {
      try {
         consumer.close();
      }
      catch (JMSException e) {
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }
   }

   @Override
   public <T> T receiveBody(Class<T> c) {
      return internalReceiveBody(c, 0, false);
   }

   @Override
   public <T> T receiveBody(Class<T> c, long timeout) {
      return internalReceiveBody(c, timeout, false);
   }

   @Override
   public <T> T receiveBodyNoWait(Class<T> c) {
      return internalReceiveBody(c, 0, true);
   }

   private <T> T internalReceiveBody(Class<T> c, long timeout, boolean noWait) {
      T result = null;
      Message message = null;
      BodyReceiver bodyReceiver = (BodyReceiver) consumer;
      if (consumer instanceof ActiveMQMessageConsumer) {
         bodyReceiver = (ActiveMQMessageConsumer) consumer;
      }
      try {
         if (messageFormatFailure == null) {
            message = noWait ? bodyReceiver.receiveNoWait(false) : bodyReceiver.receive(timeout, false);
            context.setLastMessage(ActiveMQJMSConsumer.this, message);
         }
         else {
            message = messageFormatFailure;
         }
         if (message != null) {
            result = message.getBody(c);
            messageFormatFailure = null;
            bodyReceiver.acknowledgeCoreMessage((ActiveMQMessage)message);
         }
      }
      catch (JMSException e) {
         if (e instanceof MessageFormatException) {
            messageFormatFailure = message;
         }
         throw JmsExceptionUtils.convertToRuntimeException(e);
      }

      return result;
   }

   final class MessageListenerWrapper implements MessageListener {

      private final MessageListener wrapped;

      MessageListenerWrapper(MessageListener wrapped) {
         this.wrapped = wrapped;
      }

      @Override
      public void onMessage(Message message) {
         context.setLastMessage(ActiveMQJMSConsumer.this, message);

         context.getThreadAwareContext().setCurrentThread(false);
         try {
            wrapped.onMessage(message);
         }
         finally {
            context.getThreadAwareContext().clearCurrentThread(false);
         }
      }
   }
}
