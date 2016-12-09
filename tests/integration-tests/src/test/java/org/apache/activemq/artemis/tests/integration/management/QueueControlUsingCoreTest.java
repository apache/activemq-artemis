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
package org.apache.activemq.artemis.tests.integration.management;

import javax.management.openmbean.CompositeData;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;

public class QueueControlUsingCoreTest extends QueueControlTest {

   @Override
   protected QueueControl createManagementControl(final SimpleString address,
                                                  final SimpleString queue) throws Exception {
      return new QueueControl() {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(addServerLocator(createInVMNonHALocator()), ResourceNames.QUEUE + queue);

         @Override
         public void flushExecutor() {
            try {
               proxy.invokeOperation("flushExecutor");
            } catch (Exception e) {
               throw new RuntimeException(e.getMessage(), e);
            }
         }

         @Override
         public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception {
            return (Boolean) proxy.invokeOperation("changeMessagePriority", messageID, newPriority);
         }

         @Override
         public int changeMessagesPriority(final String filter, final int newPriority) throws Exception {
            return (Integer) proxy.invokeOperation("changeMessagesPriority", filter, newPriority);
         }

         @Override
         public long countMessages(final String filter) throws Exception {
            return (Long) proxy.invokeOperation(Long.class, "countMessages", filter);
         }

         @Override
         public boolean expireMessage(final long messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("expireMessage", messageID);
         }

         @Override
         public int expireMessages(final String filter) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "expireMessages", filter);
         }

         @Override
         public String getAddress() {
            return (String) proxy.retrieveAttributeValue("address");
         }

         @Override
         public int getConsumerCount() {
            return (Integer) proxy.retrieveAttributeValue("consumerCount", Integer.class);
         }

         @Override
         public String getDeadLetterAddress() {
            return (String) proxy.retrieveAttributeValue("deadLetterAddress");
         }

         @Override
         public int getDeliveringCount() {
            return (Integer) proxy.retrieveAttributeValue("deliveringCount", Integer.class);
         }

         @Override
         public String getExpiryAddress() {
            return (String) proxy.retrieveAttributeValue("expiryAddress");
         }

         @Override
         public String getFilter() {
            return (String) proxy.retrieveAttributeValue("filter");
         }

         @Override
         public long getMessageCount() {
            return (Long) proxy.retrieveAttributeValue("messageCount", Long.class);
         }

         @Override
         public long getMessagesAdded() {
            return (Integer) proxy.retrieveAttributeValue("messagesAdded", Integer.class);
         }

         @Override
         public long getMessagesAcknowledged() {
            return (Integer) proxy.retrieveAttributeValue("messagesAcknowledged", Integer.class);
         }

         @Override
         public long getMessagesExpired() {
            return (Long) proxy.retrieveAttributeValue("messagesExpired", Long.class);
         }

         @Override
         public long getMessagesKilled() {
            return ((Number) proxy.retrieveAttributeValue("messagesKilled")).longValue();
         }

         @Override
         public void resetMessagesAdded() throws Exception {
            proxy.invokeOperation("resetMessagesAdded");
         }

         @Override
         public void resetMessagesAcknowledged() throws Exception {
            proxy.invokeOperation("resetMessagesAcknowledged");
         }

         @Override
         public void resetMessagesExpired() throws Exception {
            proxy.invokeOperation("resetMessagesExpired");
         }

         @Override
         public void resetMessagesKilled() throws Exception {
            proxy.invokeOperation("resetMessagesKilled");
         }

         @Override
         public String getName() {
            return (String) proxy.retrieveAttributeValue("name");
         }

         @Override
         public long getID() {
            return (Long) proxy.retrieveAttributeValue("ID");
         }

         @Override
         public long getScheduledCount() {
            return (Long) proxy.retrieveAttributeValue("scheduledCount", Long.class);
         }

         @Override
         public boolean isDurable() {
            return (Boolean) proxy.retrieveAttributeValue("durable");
         }

         @Override
         public boolean isTemporary() {
            return (Boolean) proxy.retrieveAttributeValue("temporary");
         }

         @Override
         public String listMessageCounter() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounter");
         }

         @Override
         public String listMessageCounterAsHTML() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterAsHTML");
         }

         @Override
         public String listMessageCounterHistory() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterHistory");
         }

         /**
          * Returns the first message on the queue as JSON
          */
         @Override
         public String getFirstMessageAsJSON() throws Exception {
            return (String) proxy.invokeOperation("getFirstMessageAsJSON");
         }

         /**
          * Returns the timestamp of the first message in milliseconds.
          */
         @Override
         public Long getFirstMessageTimestamp() throws Exception {
            return (Long) proxy.invokeOperation("getFirstMessageTimestamp");
         }

         /**
          * Returns the age of the first message in milliseconds.
          */
         @Override
         public Long getFirstMessageAge() throws Exception {
            return (Long) proxy.invokeOperation(Long.class, "getFirstMessageAge");
         }

         @Override
         public String listMessageCounterHistoryAsHTML() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterHistoryAsHTML");
         }

         @Override
         public Map<String, Object>[] listMessages(final String filter) throws Exception {
            Object[] res = (Object[]) proxy.invokeOperation("listMessages", filter);
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++) {
               results[i] = (Map<String, Object>) res[i];
            }
            return results;
         }

         @Override
         public String listMessagesAsJSON(final String filter) throws Exception {
            return (String) proxy.invokeOperation("listMessagesAsJSON", filter);
         }

         @Override
         public Map<String, Object>[] listScheduledMessages() throws Exception {
            Object[] res = (Object[]) proxy.invokeOperation("listScheduledMessages");
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++) {
               results[i] = (Map<String, Object>) res[i];
            }
            return results;
         }

         @Override
         public String listScheduledMessagesAsJSON() throws Exception {
            return (String) proxy.invokeOperation("listScheduledMessagesAsJSON");
         }

         @Override
         public int moveMessages(final String filter, final String otherQueueName) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "moveMessages", filter, otherQueueName);
         }

         @Override
         public int moveMessages(int flushLimit,
                                 String filter,
                                 String otherQueueName,
                                 boolean rejectDuplicates) throws Exception {
            return (Integer) proxy.invokeOperation("moveMessages", flushLimit, filter, otherQueueName, rejectDuplicates);
         }

         @Override
         public int moveMessages(final String filter,
                                 final String otherQueueName,
                                 final boolean rejectDuplicates) throws Exception {
            return (Integer) proxy.invokeOperation("moveMessages", filter, otherQueueName, rejectDuplicates);
         }

         @Override
         public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception {
            return (Boolean) proxy.invokeOperation("moveMessage", messageID, otherQueueName);
         }

         @Override
         public boolean moveMessage(final long messageID,
                                    final String otherQueueName,
                                    final boolean rejectDuplicates) throws Exception {
            return (Boolean) proxy.invokeOperation("moveMessage", messageID, otherQueueName, rejectDuplicates);
         }

         @Override
         public boolean retryMessage(final long messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("retryMessage", messageID);
         }

         @Override
         public int retryMessages() throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "retryMessages");
         }

         @Override
         public int removeMessages(final String filter) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "removeMessages", filter);
         }

         @Override
         public int removeMessages(final int limit, final String filter) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "removeMessages", limit, filter);
         }

         @Override
         public boolean removeMessage(final long messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("removeMessage", messageID);
         }

         @Override
         public void resetMessageCounter() throws Exception {
            proxy.invokeOperation("resetMessageCounter");
         }

         @Override
         public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("sendMessageToDeadLetterAddress", messageID);
         }

         @Override
         public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception {
            return (Integer) proxy.invokeOperation("sendMessagesToDeadLetterAddress", filterStr);
         }

         @Override
         public String sendMessage(Map<String, String> headers,
                                   int type,
                                   String body,
                                   boolean durable,
                                   String user,
                                   String password) throws Exception {
            return (String) proxy.invokeOperation("sendMessage", headers, type, body, durable, user, password);
         }

         public void setDeadLetterAddress(final String deadLetterAddress) throws Exception {
            proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress);
         }

         public void setExpiryAddress(final String expiryAddress) throws Exception {
            proxy.invokeOperation("setExpiryAddress", expiryAddress);
         }

         @Override
         public void pause() throws Exception {
            proxy.invokeOperation("pause");
         }

         @Override
         public void pause(boolean persist) throws Exception {
            proxy.invokeOperation("pause", persist);
         }

         @Override
         public void resume() throws Exception {
            proxy.invokeOperation("resume");
         }

         @Override
         public boolean isPaused() throws Exception {
            return (Boolean) proxy.invokeOperation("isPaused");
         }

         @Override
         public CompositeData[] browse() throws Exception {
            Map map = (Map) proxy.invokeOperation("browse");
            CompositeData[] compositeDatas = (CompositeData[]) map.get(CompositeData.class.getName());
            if (compositeDatas == null) {
               compositeDatas = new CompositeData[0];
            }
            return compositeDatas;
         }


         @Override
         public CompositeData[] browse(String filter) throws Exception {
            Map map = (Map) proxy.invokeOperation("browse", filter);
            CompositeData[] compositeDatas = (CompositeData[]) map.get(CompositeData.class.getName());
            if (compositeDatas == null) {
               compositeDatas = new CompositeData[0];
            }
            return compositeDatas;
         }

         @Override
         public String listConsumersAsJSON() throws Exception {
            return (String) proxy.invokeOperation("listConsumersAsJSON");
         }

         @Override
         public Map<String, Map<String, Object>[]> listDeliveringMessages() throws Exception {
            // This map code could be done better,
            // however that's just to convert stuff for the test class, so I
            // am not going to spend a lot more time on it (Clebert)
            @SuppressWarnings("rawtypes")
            Map res = (Map) proxy.invokeOperation("listDeliveringMessages");

            @SuppressWarnings("rawtypes")
            Map response = new HashMap();

            for (Object key : res.keySet()) {
               Object[] value = (Object[]) res.get(key);

               Map<String, Object>[] results = new Map[value.length];
               for (int i = 0; i < value.length; i++) {
                  results[i] = (Map<String, Object>) value[i];
               }

               response.put(key, results);
            }

            return response;
         }

         @Override
         public String listDeliveringMessagesAsJSON() throws Exception {
            return (String) proxy.invokeOperation("listDeliveringMessagesAsJSON");
         }
      };
   }
}
