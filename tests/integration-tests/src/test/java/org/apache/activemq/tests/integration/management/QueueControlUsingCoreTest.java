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
package org.apache.activemq.tests.integration.management;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.management.QueueControl;
import org.apache.activemq.api.core.management.ResourceNames;
import org.junit.Before;

public class QueueControlUsingCoreTest extends QueueControlTest
{

   protected ClientSession session;
   private ServerLocator locator;

   @Override
   protected QueueControl createManagementControl(final SimpleString address, final SimpleString queue) throws Exception
   {
      return new QueueControl()
      {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session, ResourceNames.CORE_QUEUE + queue);

         @Override
         public void flushExecutor()
         {
            try
            {
               proxy.invokeOperation("flushExecutor");
            }
            catch (Exception e)
            {
               throw new RuntimeException(e.getMessage(), e);
            }
         }

         public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception
         {
            return (Boolean) proxy.invokeOperation("changeMessagePriority", messageID, newPriority);
         }

         public int changeMessagesPriority(final String filter, final int newPriority) throws Exception
         {
            return (Integer) proxy.invokeOperation("changeMessagesPriority", filter, newPriority);
         }

         public long countMessages(final String filter) throws Exception
         {
            return ((Number) proxy.invokeOperation("countMessages", filter)).longValue();
         }

         public boolean expireMessage(final long messageID) throws Exception
         {
            return (Boolean) proxy.invokeOperation("expireMessage", messageID);
         }

         public int expireMessages(final String filter) throws Exception
         {
            return (Integer) proxy.invokeOperation("expireMessages", filter);
         }

         public String getAddress()
         {
            return (String) proxy.retrieveAttributeValue("address");
         }

         public int getConsumerCount()
         {
            return (Integer) proxy.retrieveAttributeValue("consumerCount");
         }

         public String getDeadLetterAddress()
         {
            return (String) proxy.retrieveAttributeValue("deadLetterAddress");
         }

         public int getDeliveringCount()
         {
            return (Integer) proxy.retrieveAttributeValue("deliveringCount");
         }

         public String getExpiryAddress()
         {
            return (String) proxy.retrieveAttributeValue("expiryAddress");
         }

         public String getFilter()
         {
            return (String) proxy.retrieveAttributeValue("filter");
         }

         public long getMessageCount()
         {
            return ((Number) proxy.retrieveAttributeValue("messageCount")).longValue();
         }

         public long getMessagesAdded()
         {
            return (Integer) proxy.retrieveAttributeValue("messagesAdded");
         }

         public long getMessagesAcknowledged()
         {
            return (Integer) proxy.retrieveAttributeValue("messagesAcknowledged");
         }

         public void resetMessagesAdded() throws Exception
         {
            proxy.invokeOperation("resetMessagesAdded");
         }

         public void resetMessagesAcknowledged() throws Exception
         {
            proxy.invokeOperation("resetMessagesAcknowledged");
         }

         public String getName()
         {
            return (String) proxy.retrieveAttributeValue("name");
         }

         public long getID()
         {
            return (Long) proxy.retrieveAttributeValue("ID");
         }

         public long getScheduledCount()
         {
            return (Long) proxy.retrieveAttributeValue("scheduledCount", Long.class);
         }

         public boolean isDurable()
         {
            return (Boolean) proxy.retrieveAttributeValue("durable");
         }

         public boolean isTemporary()
         {
            return (Boolean) proxy.retrieveAttributeValue("temporary");
         }

         public String listMessageCounter() throws Exception
         {
            return (String) proxy.invokeOperation("listMessageCounter");
         }

         public String listMessageCounterAsHTML() throws Exception
         {
            return (String) proxy.invokeOperation("listMessageCounterAsHTML");
         }

         public String listMessageCounterHistory() throws Exception
         {
            return (String) proxy.invokeOperation("listMessageCounterHistory");
         }


         /**
          * Returns the first message on the queue as JSON
          */
         public String getFirstMessageAsJSON() throws Exception
         {
            return (String) proxy.invokeOperation("getFirstMessageAsJSON");
         }


         public String listMessageCounterHistoryAsHTML() throws Exception
         {
            return (String) proxy.invokeOperation("listMessageCounterHistoryAsHTML");
         }

         public Map<String, Object>[] listMessages(final String filter) throws Exception
         {
            Object[] res = (Object[]) proxy.invokeOperation("listMessages", filter);
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++)
            {
               results[i] = (Map<String, Object>) res[i];
            }
            return results;
         }

         public String listMessagesAsJSON(final String filter) throws Exception
         {
            return (String) proxy.invokeOperation("listMessagesAsJSON", filter);
         }

         public Map<String, Object>[] listScheduledMessages() throws Exception
         {
            Object[] res = (Object[]) proxy.invokeOperation("listScheduledMessages");
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++)
            {
               results[i] = (Map<String, Object>) res[i];
            }
            return results;
         }

         public String listScheduledMessagesAsJSON() throws Exception
         {
            return (String) proxy.invokeOperation("listScheduledMessagesAsJSON");
         }

         public int moveMessages(final String filter, final String otherQueueName) throws Exception
         {
            return (Integer) proxy.invokeOperation("moveMessages", filter, otherQueueName);
         }


         @Override
         public int moveMessages(
            int flushLimit,
            String filter,
            String otherQueueName,
            boolean rejectDuplicates) throws Exception
         {
            return (Integer) proxy.invokeOperation("moveMessages", flushLimit, filter, otherQueueName, rejectDuplicates);
         }

         public int moveMessages(final String filter, final String otherQueueName, final boolean rejectDuplicates) throws Exception
         {
            return (Integer) proxy.invokeOperation("moveMessages", filter, otherQueueName, rejectDuplicates);
         }

         public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception
         {
            return (Boolean) proxy.invokeOperation("moveMessage", messageID, otherQueueName);
         }

         public boolean moveMessage(final long messageID, final String otherQueueName, final boolean rejectDuplicates) throws Exception
         {
            return (Boolean) proxy.invokeOperation("moveMessage", messageID, otherQueueName, rejectDuplicates);
         }

         public int removeMessages(final String filter) throws Exception
         {
            return (Integer) proxy.invokeOperation("removeMessages", filter);
         }

         public int removeMessages(final int limit, final String filter) throws Exception
         {
            return (Integer) proxy.invokeOperation("removeMessages", limit, filter);
         }

         public boolean removeMessage(final long messageID) throws Exception
         {
            return (Boolean) proxy.invokeOperation("removeMessage", messageID);
         }

         public void resetMessageCounter() throws Exception
         {
            proxy.invokeOperation("resetMessageCounter");
         }

         public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
         {
            return (Boolean) proxy.invokeOperation("sendMessageToDeadLetterAddress", messageID);
         }

         public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception
         {
            return (Integer) proxy.invokeOperation("sendMessagesToDeadLetterAddress", filterStr);
         }

         public void setDeadLetterAddress(final String deadLetterAddress) throws Exception
         {
            proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress);
         }

         public void setExpiryAddress(final String expiryAddress) throws Exception
         {
            proxy.invokeOperation("setExpiryAddress", expiryAddress);
         }

         public void pause() throws Exception
         {
            proxy.invokeOperation("pause");
         }

         public void resume() throws Exception
         {
            proxy.invokeOperation("resume");
         }

         public boolean isPaused() throws Exception
         {
            return (Boolean) proxy.invokeOperation("isPaused");
         }

         public String listConsumersAsJSON() throws Exception
         {
            return (String) proxy.invokeOperation("listConsumersAsJSON");
         }

         public Map<String, Map<String, Object>[]> listDeliveringMessages() throws Exception
         {
            // This map code could be done better,
            // however that's just to convert stuff for the test class, so I
            // am not going to spend a lot more time on it (Clebert)
            @SuppressWarnings("rawtypes")
            Map res = (Map) proxy.invokeOperation("listDeliveringMessages");

            @SuppressWarnings("rawtypes")
            Map response = new HashMap();

            for (Object key : res.keySet())
            {
               Object[] value = (Object[]) res.get(key);


               Map<String, Object>[] results = new Map[value.length];
               for (int i = 0; i < value.length; i++)
               {
                  results[i] = (Map<String, Object>) value[i];
               }

               response.put(key, results);
            }

            return response;
         }

         @Override
         public String listDeliveringMessagesAsJSON() throws Exception
         {
            return (String) proxy.invokeOperation("listDeliveringMessagesAsJSON");
         }
      };
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
      session.start();
   }
}
