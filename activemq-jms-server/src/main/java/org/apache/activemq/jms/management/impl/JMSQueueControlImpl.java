/**
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
package org.apache.activemq.jms.management.impl;

import javax.management.MBeanInfo;
import javax.management.StandardMBean;
import java.util.Map;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.FilterConstants;
import org.apache.activemq.api.core.management.MessageCounterInfo;
import org.apache.activemq.api.core.management.Operation;
import org.apache.activemq.api.core.management.QueueControl;
import org.apache.activemq.api.jms.management.JMSQueueControl;
import org.apache.activemq.core.management.impl.MBeanInfoHelper;
import org.apache.activemq.core.messagecounter.MessageCounter;
import org.apache.activemq.core.messagecounter.impl.MessageCounterHelper;
import org.apache.activemq.jms.client.ActiveMQDestination;
import org.apache.activemq.jms.client.ActiveMQMessage;
import org.apache.activemq.jms.client.SelectorTranslator;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.utils.json.JSONArray;
import org.apache.activemq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSQueueControlImpl extends StandardMBean implements JMSQueueControl
{
   private final ActiveMQDestination managedQueue;

   private final JMSServerManager jmsServerManager;

   private final QueueControl coreQueueControl;

   private final MessageCounter counter;

   // Static --------------------------------------------------------

   /**
    * Returns null if the string is null or empty
    */
   public static String createFilterFromJMSSelector(final String selectorStr) throws ActiveMQException
   {
      return selectorStr == null || selectorStr.trim().length() == 0 ? null
         : SelectorTranslator.convertToActiveMQFilterString(selectorStr);
   }

   private static String createFilterForJMSMessageID(final String jmsMessageID) throws Exception
   {
      return FilterConstants.ACTIVEMQ_USERID + " = '" + jmsMessageID + "'";
   }

   static String toJSON(final Map<String, Object>[] messages)
   {
      JSONArray array = new JSONArray();
      for (Map<String, Object> message : messages)
      {
         array.put(new JSONObject(message));
      }
      return array.toString();
   }

   // Constructors --------------------------------------------------

   public JMSQueueControlImpl(final ActiveMQDestination managedQueue,
                              final QueueControl coreQueueControl,
                              final JMSServerManager jmsServerManager,
                              final MessageCounter counter) throws Exception
   {
      super(JMSQueueControl.class);
      this.managedQueue = managedQueue;
      this.jmsServerManager = jmsServerManager;
      this.coreQueueControl = coreQueueControl;
      this.counter = counter;
   }

   // Public --------------------------------------------------------

   // ManagedJMSQueueMBean implementation ---------------------------

   public String getName()
   {
      return managedQueue.getName();
   }

   public String getAddress()
   {
      return managedQueue.getAddress();
   }

   public boolean isTemporary()
   {
      return managedQueue.isTemporary();
   }

   public long getMessageCount()
   {
      return coreQueueControl.getMessageCount();
   }

   public long getMessagesAdded()
   {
      return coreQueueControl.getMessagesAdded();
   }

   public int getConsumerCount()
   {
      return coreQueueControl.getConsumerCount();
   }

   public int getDeliveringCount()
   {
      return coreQueueControl.getDeliveringCount();
   }

   public long getScheduledCount()
   {
      return coreQueueControl.getScheduledCount();
   }

   public boolean isDurable()
   {
      return coreQueueControl.isDurable();
   }

   public String getDeadLetterAddress()
   {
      return coreQueueControl.getDeadLetterAddress();
   }

   public String getExpiryAddress()
   {
      return coreQueueControl.getExpiryAddress();
   }

   @Override
   public void addBinding(String binding) throws Exception
   {
      jmsServerManager.addQueueToBindingRegistry(managedQueue.getName(), binding);
   }

   public String[] getRegistryBindings()
   {
      return jmsServerManager.getBindingsOnQueue(managedQueue.getName());
   }

   public boolean removeMessage(final String messageID) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      int removed = coreQueueControl.removeMessages(filter);
      if (removed != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int removeMessages(final String filterStr) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.removeMessages(filter);
   }

   public Map<String, Object>[] listMessages(final String filterStr) throws Exception
   {
      try
      {
         String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
         Map<String, Object>[] coreMessages = coreQueueControl.listMessages(filter);

         Map<String, Object>[] jmsMessages = new Map[coreMessages.length];

         int i = 0;

         for (Map<String, Object> coreMessage : coreMessages)
         {
            Map<String, Object> jmsMessage = ActiveMQMessage.coreMaptoJMSMap(coreMessage);
            jmsMessages[i++] = jmsMessage;
         }
         return jmsMessages;
      }
      catch (ActiveMQException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public String listMessagesAsJSON(final String filter) throws Exception
   {
      return JMSQueueControlImpl.toJSON(listMessages(filter));
   }

   public long countMessages(final String filterStr) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.countMessages(filter);
   }

   public boolean expireMessage(final String messageID) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      int expired = coreQueueControl.expireMessages(filter);
      if (expired != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int expireMessages(final String filterStr) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.expireMessages(filter);
   }

   public boolean sendMessageToDeadLetterAddress(final String messageID) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      int dead = coreQueueControl.sendMessagesToDeadLetterAddress(filter);
      if (dead != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.sendMessagesToDeadLetterAddress(filter);
   }

   public boolean changeMessagePriority(final String messageID, final int newPriority) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      int changed = coreQueueControl.changeMessagesPriority(filter, newPriority);
      if (changed != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int changeMessagesPriority(final String filterStr, final int newPriority) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.changeMessagesPriority(filter, newPriority);
   }

   public boolean moveMessage(final String messageID, final String otherQueueName) throws Exception
   {
      return moveMessage(messageID, otherQueueName, false);
   }

   public boolean moveMessage(final String messageID, final String otherQueueName, final boolean rejectDuplicates) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      ActiveMQDestination otherQueue = ActiveMQDestination.createQueue(otherQueueName);
      int moved = coreQueueControl.moveMessages(filter, otherQueue.getAddress(), rejectDuplicates);
      if (moved != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }

      return true;
   }

   public int moveMessages(final String filterStr, final String otherQueueName, final boolean rejectDuplicates) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      ActiveMQDestination otherQueue = ActiveMQDestination.createQueue(otherQueueName);
      return coreQueueControl.moveMessages(filter, otherQueue.getAddress(), rejectDuplicates);
   }


   public int moveMessages(final String filterStr, final String otherQueueName) throws Exception
   {
      return moveMessages(filterStr, otherQueueName, false);
   }

   @Operation(desc = "List all the existent consumers on the Queue")
   public String listConsumersAsJSON() throws Exception
   {
      return coreQueueControl.listConsumersAsJSON();
   }

   public String listMessageCounter()
   {
      try
      {
         return MessageCounterInfo.toJSon(counter);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void resetMessageCounter() throws Exception
   {
      coreQueueControl.resetMessageCounter();
   }

   public String listMessageCounterAsHTML()
   {
      return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[]{counter});
   }

   public String listMessageCounterHistory() throws Exception
   {
      return MessageCounterHelper.listMessageCounterHistory(counter);
   }

   public String listMessageCounterHistoryAsHTML()
   {
      return MessageCounterHelper.listMessageCounterHistoryAsHTML(new MessageCounter[]{counter});
   }

   public boolean isPaused() throws Exception
   {
      return coreQueueControl.isPaused();
   }

   public void pause() throws Exception
   {
      coreQueueControl.pause();
   }

   public void resume() throws Exception
   {
      coreQueueControl.resume();
   }

   public String getSelector()
   {
      return coreQueueControl.getFilter();
   }

   public void flushExecutor()
   {
      coreQueueControl.flushExecutor();
   }

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(JMSQueueControl.class),
                           info.getNotifications());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
