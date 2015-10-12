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
package org.apache.activemq.artemis.core.management.impl;

import javax.management.MBeanOperationInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.MessageCounterInfo;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.messagecounter.MessageCounter;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterHelper;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.LinkedListIterator;
import org.apache.activemq.artemis.utils.json.JSONArray;
import org.apache.activemq.artemis.utils.json.JSONException;
import org.apache.activemq.artemis.utils.json.JSONObject;

public class QueueControlImpl extends AbstractControl implements QueueControl {

   public static final int FLUSH_LIMIT = 500;
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Queue queue;

   private final String address;

   private final PostOffice postOffice;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private MessageCounter counter;

   // Static --------------------------------------------------------

   private static String toJSON(final Map<String, Object>[] messages) {
      JSONArray array = toJSONMsgArray(messages);
      return array.toString();
   }

   private static JSONArray toJSONMsgArray(final Map<String, Object>[] messages) {
      JSONArray array = new JSONArray();
      for (Map<String, Object> message : messages) {
         array.put(new JSONObject(message));
      }
      return array;
   }

   private static String toJSON(final Map<String, Map<String, Object>[]> messages) {
      try {
         JSONArray arrayReturn = new JSONArray();
         for (Map.Entry<String, Map<String, Object>[]> entry : messages.entrySet()) {
            JSONObject objectItem = new JSONObject();
            objectItem.put("consumerName", entry.getKey());
            objectItem.put("elements", toJSONMsgArray(entry.getValue()));
            arrayReturn.put(objectItem);
         }

         return arrayReturn.toString();
      }
      catch (JSONException e) {
         return "Invalid conversion " + e.toString();
      }
   }

   // Constructors --------------------------------------------------

   public QueueControlImpl(final Queue queue,
                           final String address,
                           final PostOffice postOffice,
                           final StorageManager storageManager,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception {
      super(QueueControl.class, storageManager);
      this.queue = queue;
      this.address = address;
      this.postOffice = postOffice;
      this.addressSettingsRepository = addressSettingsRepository;
   }

   // Public --------------------------------------------------------

   public void setMessageCounter(final MessageCounter counter) {
      this.counter = counter;
   }

   // QueueControlMBean implementation ------------------------------

   public String getName() {
      clearIO();
      try {
         return queue.getName().toString();
      }
      finally {
         blockOnIO();
      }
   }

   public String getAddress() {
      checkStarted();

      return address;
   }

   public String getFilter() {
      checkStarted();

      clearIO();
      try {
         Filter filter = queue.getFilter();

         return filter != null ? filter.getFilterString().toString() : null;
      }
      finally {
         blockOnIO();
      }
   }

   public boolean isDurable() {
      checkStarted();

      clearIO();
      try {
         return queue.isDurable();
      }
      finally {
         blockOnIO();
      }
   }

   public boolean isTemporary() {
      checkStarted();

      clearIO();
      try {
         return queue.isTemporary();
      }
      finally {
         blockOnIO();
      }
   }

   public long getMessageCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessageCount();
      }
      finally {
         blockOnIO();
      }
   }

   public int getConsumerCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getConsumerCount();
      }
      finally {
         blockOnIO();
      }
   }

   public int getDeliveringCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getDeliveringCount();
      }
      finally {
         blockOnIO();
      }
   }

   public long getMessagesAdded() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesAdded();
      }
      finally {
         blockOnIO();
      }
   }

   public long getMessagesAcknowledged() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesAcknowledged();
      }
      finally {
         blockOnIO();
      }
   }

   public long getID() {
      checkStarted();

      clearIO();
      try {
         return queue.getID();
      }
      finally {
         blockOnIO();
      }
   }

   public long getScheduledCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getScheduledCount();
      }
      finally {
         blockOnIO();
      }
   }

   public String getDeadLetterAddress() {
      checkStarted();

      clearIO();
      try {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

         if (addressSettings != null && addressSettings.getDeadLetterAddress() != null) {
            return addressSettings.getDeadLetterAddress().toString();
         }
         return null;
      }
      finally {
         blockOnIO();
      }
   }

   public String getExpiryAddress() {
      checkStarted();

      clearIO();
      try {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

         if (addressSettings != null && addressSettings.getExpiryAddress() != null) {
            return addressSettings.getExpiryAddress().toString();
         }
         else {
            return null;
         }
      }
      finally {
         blockOnIO();
      }
   }

   public Map<String, Object>[] listScheduledMessages() throws Exception {
      checkStarted();

      clearIO();
      try {
         List<MessageReference> refs = queue.getScheduledMessages();
         return convertMessagesToMaps(refs);
      }
      finally {
         blockOnIO();
      }
   }

   public String listScheduledMessagesAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listScheduledMessages());
      }
      finally {
         blockOnIO();
      }
   }

   /**
    * @param refs
    * @return
    */
   private Map<String, Object>[] convertMessagesToMaps(List<MessageReference> refs) {
      Map<String, Object>[] messages = new Map[refs.size()];
      int i = 0;
      for (MessageReference ref : refs) {
         Message message = ref.getMessage();
         messages[i++] = message.toMap();
      }
      return messages;
   }

   public Map<String, Map<String, Object>[]> listDeliveringMessages() {
      checkStarted();

      clearIO();
      try {
         Map<String, List<MessageReference>> msgs = queue.getDeliveringMessages();

         Map<String, Map<String, Object>[]> msgRet = new HashMap<String, Map<String, Object>[]>();

         for (Map.Entry<String, List<MessageReference>> entry : msgs.entrySet()) {
            msgRet.put(entry.getKey(), convertMessagesToMaps(entry.getValue()));
         }
         return msgRet;
      }
      finally {
         blockOnIO();
      }

   }

   public String listDeliveringMessagesAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listDeliveringMessages());
      }
      finally {
         blockOnIO();
      }
   }

   public Map<String, Object>[] listMessages(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         List<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();
         queue.flushExecutor();
         LinkedListIterator<MessageReference> iterator = queue.totalIterator();
         try {
            while (iterator.hasNext()) {
               MessageReference ref = iterator.next();
               if (filter == null || filter.match(ref.getMessage())) {
                  Message message = ref.getMessage();
                  messages.add(message.toMap());
               }
            }
            return messages.toArray(new Map[messages.size()]);
         }
         finally {
            iterator.close();
         }
      }
      catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      }
      finally {
         blockOnIO();
      }
   }

   public String listMessagesAsJSON(final String filter) throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listMessages(filter));
      }
      finally {
         blockOnIO();
      }
   }

   protected Map<String, Object>[] getFirstMessage() throws Exception {
      checkStarted();

      clearIO();
      try {
         List<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();
         queue.flushExecutor();
         LinkedListIterator<MessageReference> iterator = queue.totalIterator();
         try {
            // returns just the first, as it's the first only
            if (iterator.hasNext()) {
               MessageReference ref = iterator.next();
               Message message = ref.getMessage();
               messages.add(message.toMap());
            }
            return messages.toArray(new Map[1]);
         }
         finally {
            iterator.close();
         }
      }
      finally {
         blockOnIO();
      }

   }

   public String getFirstMessageAsJSON() throws Exception {
      return toJSON(getFirstMessage()).toString();
   }

   public Long getFirstMessageTimestamp() throws Exception {
      Map<String, Object>[] _message = getFirstMessage();
      if (_message == null || _message.length == 0 || _message[0] == null) {
         return null;
      }
      Map<String, Object> message = _message[0];
      if (!message.containsKey("timestamp")) {
         return null;
      }
      return (Long) message.get("timestamp");
   }

   public Long getFirstMessageAge() throws Exception {
      Long firstMessageTimestamp = getFirstMessageTimestamp();
      if (firstMessageTimestamp == null) {
         return null;
      }
      long now = new Date().getTime();
      return now - firstMessageTimestamp.longValue();
   }

   public long countMessages(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         if (filter == null) {
            return getMessageCount();
         }
         else {
            LinkedListIterator<MessageReference> iterator = queue.totalIterator();
            try {
               int count = 0;
               while (iterator.hasNext()) {
                  MessageReference ref = iterator.next();
                  if (filter.match(ref.getMessage())) {
                     count++;
                  }
               }
               return count;
            }
            finally {
               iterator.close();
            }
         }
      }
      finally {
         blockOnIO();
      }
   }

   public boolean removeMessage(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.deleteReference(messageID);
      }
      catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      }
      finally {
         blockOnIO();
      }
   }

   public int removeMessages(final String filterStr) throws Exception {
      return removeMessages(FLUSH_LIMIT, filterStr);
   }

   public int removeMessages(final int flushLimit, final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.deleteMatchingReferences(flushLimit, filter);
      }
      finally {
         blockOnIO();
      }
   }

   public boolean expireMessage(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.expireReference(messageID);
      }
      finally {
         blockOnIO();
      }
   }

   public int expireMessages(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         return queue.expireReferences(filter);
      }
      catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      }
      finally {
         blockOnIO();
      }
   }

   public boolean retryMessage(final long messageID) throws Exception {

      checkStarted();
      clearIO();

      try {
         Filter singleMessageFilter = new Filter() {
            @Override
            public boolean match(ServerMessage message) {
               return message.getMessageID() == messageID;
            }

            @Override
            public SimpleString getFilterString() {
               return new SimpleString("custom filter for MESSAGEID= messageID");
            }
         };

         queue.retryMessages(singleMessageFilter);
      }
      finally {
         blockOnIO();
      }

      return false;
   }

   public int retryMessages() throws Exception {
      checkStarted();
      clearIO();

      try {
         return queue.retryMessages(null);
      }
      finally {
         blockOnIO();
      }
   }

   public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception {
      return moveMessage(messageID, otherQueueName, false);
   }

   public boolean moveMessage(final long messageID,
                              final String otherQueueName,
                              final boolean rejectDuplicates) throws Exception {
      checkStarted();

      clearIO();
      try {
         Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));

         if (binding == null) {
            throw ActiveMQMessageBundle.BUNDLE.noQueueFound(otherQueueName);
         }

         return queue.moveReference(messageID, binding.getAddress(), rejectDuplicates);
      }
      finally {
         blockOnIO();
      }

   }

   public int moveMessages(final String filterStr, final String otherQueueName) throws Exception {
      return moveMessages(filterStr, otherQueueName, false);
   }

   public int moveMessages(final int flushLimit,
                           final String filterStr,
                           final String otherQueueName,
                           final boolean rejectDuplicates) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));

         if (binding == null) {
            throw ActiveMQMessageBundle.BUNDLE.noQueueFound(otherQueueName);
         }

         int retValue = queue.moveReferences(flushLimit, filter, binding.getAddress(), rejectDuplicates);

         return retValue;
      }
      finally {
         blockOnIO();
      }

   }

   public int moveMessages(final String filterStr,
                           final String otherQueueName,
                           final boolean rejectDuplicates) throws Exception {
      return moveMessages(FLUSH_LIMIT, filterStr, otherQueueName, rejectDuplicates);
   }

   public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.sendMessagesToDeadLetterAddress(filter);
      }
      finally {
         blockOnIO();
      }
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.sendMessageToDeadLetterAddress(messageID);
      }
      finally {
         blockOnIO();
      }
   }

   public int changeMessagesPriority(final String filterStr, final int newPriority) throws Exception {
      checkStarted();

      clearIO();
      try {
         if (newPriority < 0 || newPriority > 9) {
            throw ActiveMQMessageBundle.BUNDLE.invalidNewPriority(newPriority);
         }
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.changeReferencesPriority(filter, (byte) newPriority);
      }
      finally {
         blockOnIO();
      }
   }

   public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception {
      checkStarted();

      clearIO();
      try {
         if (newPriority < 0 || newPriority > 9) {
            throw ActiveMQMessageBundle.BUNDLE.invalidNewPriority(newPriority);
         }
         return queue.changeReferencePriority(messageID, (byte) newPriority);
      }
      finally {
         blockOnIO();
      }
   }

   public String listMessageCounter() {
      checkStarted();

      clearIO();
      try {
         return MessageCounterInfo.toJSon(counter);
      }
      catch (Exception e) {
         throw new IllegalStateException(e);
      }
      finally {
         blockOnIO();
      }
   }

   public void resetMessageCounter() {
      checkStarted();

      clearIO();
      try {
         counter.resetCounter();
      }
      finally {
         blockOnIO();
      }
   }

   public String listMessageCounterAsHTML() {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[]{counter});
      }
      finally {
         blockOnIO();
      }
   }

   public String listMessageCounterHistory() throws Exception {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterHistory(counter);
      }
      finally {
         blockOnIO();
      }
   }

   public String listMessageCounterHistoryAsHTML() {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterHistoryAsHTML(new MessageCounter[]{counter});
      }
      finally {
         blockOnIO();
      }
   }

   public void pause() {
      checkStarted();

      clearIO();
      try {
         queue.pause();
      }
      finally {
         blockOnIO();
      }
   }

   public void resume() {
      checkStarted();

      clearIO();
      try {
         queue.resume();
      }
      finally {
         blockOnIO();
      }
   }

   public boolean isPaused() throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.isPaused();
      }
      finally {
         blockOnIO();
      }
   }

   public void flushExecutor() {
      checkStarted();

      clearIO();
      try {
         queue.flushExecutor();
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   public String listConsumersAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         Collection<Consumer> consumers = queue.getConsumers();

         JSONArray jsonArray = new JSONArray();

         for (Consumer consumer : consumers) {

            if (consumer instanceof ServerConsumer) {
               ServerConsumer serverConsumer = (ServerConsumer) consumer;

               JSONObject obj = new JSONObject();
               obj.put("consumerID", serverConsumer.getID());
               obj.put("connectionID", serverConsumer.getConnectionID().toString());
               obj.put("sessionID", serverConsumer.getSessionID());
               obj.put("browseOnly", serverConsumer.isBrowseOnly());
               obj.put("creationTime", serverConsumer.getCreationTime());

               jsonArray.put(obj);
            }

         }

         return jsonArray.toString();
      }
      finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(QueueControl.class);
   }

   public void resetMessagesAdded() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesAdded();
      }
      finally {
         blockOnIO();
      }

   }

   public void resetMessagesAcknowledged() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesAcknowledged();
      }
      finally {
         blockOnIO();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkStarted() {
      if (!postOffice.isStarted()) {
         throw new IllegalStateException("Broker is not started. Queue can not be managed yet");
      }
   }

   // Inner classes -------------------------------------------------
}
