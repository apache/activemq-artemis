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

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.openmbean.CompositeData;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.MessageCounterInfo;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.management.impl.openmbean.OpenTypeSupport;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.messagecounter.MessageCounter;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterHelper;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;

public class QueueControlImpl extends AbstractControl implements QueueControl {

   public static final int FLUSH_LIMIT = 500;
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Queue queue;

   private final String address;

   private final PostOffice postOffice;

   private final StorageManager storageManager;
   private final SecurityStore securityStore;
   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private MessageCounter counter;

   // Static --------------------------------------------------------

   private static String toJSON(final Map<String, Object>[] messages) {
      JsonArray array = toJSONMsgArray(messages);
      return array.toString();
   }

   private static JsonArray toJSONMsgArray(final Map<String, Object>[] messages) {
      JsonArrayBuilder array = JsonLoader.createArrayBuilder();
      for (Map<String, Object> message : messages) {
         array.add(JsonUtil.toJsonObject(message));
      }
      return array.build();
   }

   private static String toJSON(final Map<String, Map<String, Object>[]> messages) {
      JsonArrayBuilder arrayReturn = JsonLoader.createArrayBuilder();
      for (Map.Entry<String, Map<String, Object>[]> entry : messages.entrySet()) {
         JsonObjectBuilder objectItem = JsonLoader.createObjectBuilder();
         objectItem.add("consumerName", entry.getKey());
         objectItem.add("elements", toJSONMsgArray(entry.getValue()));
         arrayReturn.add(objectItem);
      }

      return arrayReturn.build().toString();
   }

   // Constructors --------------------------------------------------

   public QueueControlImpl(final Queue queue,
                           final String address,
                           final PostOffice postOffice,
                           final StorageManager storageManager,
                           final SecurityStore securityStore,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception {
      super(QueueControl.class, storageManager);
      this.queue = queue;
      this.address = address;
      this.postOffice = postOffice;
      this.storageManager = storageManager;
      this.securityStore = securityStore;
      this.addressSettingsRepository = addressSettingsRepository;
   }

   // Public --------------------------------------------------------

   public void setMessageCounter(final MessageCounter counter) {
      this.counter = counter;
   }

   // QueueControlMBean implementation ------------------------------

   @Override
   public String getName() {
      clearIO();
      try {
         return queue.getName().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getAddress() {
      checkStarted();

      return address;
   }

   @Override
   public String getFilter() {
      checkStarted();

      clearIO();
      try {
         Filter filter = queue.getFilter();

         return filter != null ? filter.getFilterString().toString() : null;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isDurable() {
      checkStarted();

      clearIO();
      try {
         return queue.isDurable();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getUser() {
      checkStarted();

      clearIO();
      try {
         SimpleString user = queue.getUser();
         return user == null ? null : user.toString();
      } finally {
         blockOnIO();
      }
   }


   @Override
   public String getRoutingType() {
      checkStarted();

      clearIO();
      try {
         return queue.getRoutingType().toString();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public boolean isTemporary() {
      checkStarted();

      clearIO();
      try {
         return queue.isTemporary();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessageCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessageCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getPersistentSize() {
      checkStarted();

      clearIO();
      try {
         return queue.getPersistentSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurableMessageCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableMessageCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurablePersistentSize() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurablePersistentSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getConsumerCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getConsumerCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getDeliveringCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getDeliveringCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDeliveringSize() {
      checkStarted();

      clearIO();
      try {
         return queue.getDeliveringSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getDurableDeliveringCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableDeliveringCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurableDeliveringSize() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableDeliveringSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesAdded() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesAdded();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesAcknowledged() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesAcknowledged();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesExpired() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesExpired();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesKilled() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesKilled();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getID() {
      checkStarted();

      clearIO();
      try {
         return queue.getID();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getScheduledCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getScheduledCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getScheduledSize() {
      checkStarted();

      clearIO();
      try {
         return queue.getScheduledSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurableScheduledCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableScheduledCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurableScheduledSize() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableScheduledSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getDeadLetterAddress() {
      checkStarted();

      clearIO();
      try {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

         if (addressSettings != null && addressSettings.getDeadLetterAddress() != null) {
            return addressSettings.getDeadLetterAddress().toString();
         }
         return null;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getExpiryAddress() {
      checkStarted();

      clearIO();
      try {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

         if (addressSettings != null && addressSettings.getExpiryAddress() != null) {
            return addressSettings.getExpiryAddress().toString();
         } else {
            return null;
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getMaxConsumers() {
      checkStarted();

      clearIO();
      try {
         return queue.getMaxConsumers();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPurgeOnNoConsumers() {
      checkStarted();

      clearIO();
      try {
         return queue.isPurgeOnNoConsumers();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isExclusive() {
      checkStarted();

      clearIO();
      try {
         return queue.isExclusive();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isLastValue() {
      checkStarted();

      clearIO();
      try {
         return queue.isLastValue();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object>[] listScheduledMessages() throws Exception {
      checkStarted();

      clearIO();
      try {
         List<MessageReference> refs = queue.getScheduledMessages();
         return convertMessagesToMaps(refs);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listScheduledMessagesAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listScheduledMessages());
      } finally {
         blockOnIO();
      }
   }

   /**
    * @param refs
    * @return
    */
   private Map<String, Object>[] convertMessagesToMaps(List<MessageReference> refs) throws ActiveMQException {
      Map<String, Object>[] messages = new Map[refs.size()];
      int i = 0;
      for (MessageReference ref : refs) {
         Message message = ref.getMessage();
         messages[i++] = message.toMap();
      }
      return messages;
   }

   @Override
   public Map<String, Map<String, Object>[]> listDeliveringMessages() throws ActiveMQException {
      checkStarted();

      clearIO();
      try {
         Map<String, List<MessageReference>> msgs = queue.getDeliveringMessages();

         Map<String, Map<String, Object>[]> msgRet = new HashMap<>();

         for (Map.Entry<String, List<MessageReference>> entry : msgs.entrySet()) {
            msgRet.put(entry.getKey(), convertMessagesToMaps(entry.getValue()));
         }
         return msgRet;
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String listDeliveringMessagesAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listDeliveringMessages());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object>[] listMessages(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         List<Map<String, Object>> messages = new ArrayList<>();
         queue.flushExecutor();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext()) {
                  MessageReference ref = iterator.next();
                  if (filter == null || filter.match(ref.getMessage())) {
                     Message message = ref.getMessage();
                     messages.add(message.toMap());
                  }
               }
            } catch (NoSuchElementException ignored) {
               // this could happen through paging browsing
            }
            return messages.toArray(new Map[messages.size()]);
         }
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessagesAsJSON(final String filter) throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listMessages(filter));
      } finally {
         blockOnIO();
      }
   }

   protected Map<String, Object>[] getFirstMessage() throws Exception {
      checkStarted();

      clearIO();
      try {
         List<Map<String, Object>> messages = new ArrayList<>();
         queue.flushExecutor();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            // returns just the first, as it's the first only
            if (iterator.hasNext()) {
               MessageReference ref = iterator.next();
               Message message = ref.getMessage();
               messages.add(message.toMap());
            }
            return messages.toArray(new Map[1]);
         }
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getFirstMessageAsJSON() throws Exception {
      return toJSON(getFirstMessage());
   }

   @Override
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

   @Override
   public Long getFirstMessageAge() throws Exception {
      Long firstMessageTimestamp = getFirstMessageTimestamp();
      if (firstMessageTimestamp == null) {
         return null;
      }
      long now = new Date().getTime();
      return now - firstMessageTimestamp.longValue();
   }

   @Override
   public long countMessages() throws Exception {
      return countMessages(null);
   }

   @Override
   public long countMessages(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         if (filter == null) {
            return getMessageCount();
         } else {
            try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
               int count = 0;

               try {
                  while (iterator.hasNext()) {
                     MessageReference ref = iterator.next();
                     if (filter.match(ref.getMessage())) {
                        count++;
                     }
                  }
               } catch (NoSuchElementException ignored) {
                  // this could happen through paging browsing
               }
               return count;
            }
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean removeMessage(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.deleteReference(messageID);
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int removeMessages(final String filterStr) throws Exception {
      return removeMessages(FLUSH_LIMIT, filterStr);
   }

   @Override
   public int removeMessages(final int flushLimit, final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.deleteMatchingReferences(flushLimit, filter);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int removeAllMessages() throws Exception {
      return removeMessages(FLUSH_LIMIT, null);
   }

   @Override
   public boolean expireMessage(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.expireReference(messageID);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int expireMessages(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         return queue.expireReferences(filter);
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean retryMessage(final long messageID) throws Exception {

      checkStarted();
      clearIO();

      try {
         Filter singleMessageFilter = new Filter() {
            @Override
            public boolean match(Message message) {
               return message.getMessageID() == messageID;
            }

            @Override
            public SimpleString getFilterString() {
               return new SimpleString("custom filter for MESSAGEID= messageID");
            }
         };

         return queue.retryMessages(singleMessageFilter) > 0;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int retryMessages() throws Exception {
      checkStarted();
      clearIO();

      try {
         return queue.retryMessages(null);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception {
      return moveMessage(messageID, otherQueueName, false);
   }

   @Override
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

         return queue.moveReference(messageID, binding.getAddress(), binding, rejectDuplicates);
      } finally {
         blockOnIO();
      }

   }

   @Override
   public int moveMessages(final String filterStr, final String otherQueueName) throws Exception {
      return moveMessages(filterStr, otherQueueName, false);
   }

   @Override
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

         int retValue = queue.moveReferences(flushLimit, filter, binding.getAddress(), rejectDuplicates, binding);

         return retValue;
      } finally {
         blockOnIO();
      }

   }

   @Override
   public int moveMessages(final String filterStr,
                           final String otherQueueName,
                           final boolean rejectDuplicates) throws Exception {
      return moveMessages(FLUSH_LIMIT, filterStr, otherQueueName, rejectDuplicates);
   }

   @Override
   public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.sendMessagesToDeadLetterAddress(filter);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String sendMessage(final Map<String, String> headers,
                             final int type,
                             final String body,
                             boolean durable,
                             final String user,
                             final String password) throws Exception {
      try {
         securityStore.check(queue.getAddress(), queue.getName(), CheckType.SEND, new SecurityAuth() {
            @Override
            public String getUsername() {
               return user;
            }

            @Override
            public String getPassword() {
               return password;
            }

            @Override
            public RemotingConnection getRemotingConnection() {
               return null;
            }
         });
         CoreMessage message = new CoreMessage(storageManager.generateID(), 50);
         if (headers != null) {
            for (String header : headers.keySet()) {
               message.putStringProperty(new SimpleString(header), new SimpleString(headers.get(header)));
            }
         }
         message.setType((byte) type);
         message.setDurable(durable);
         message.setTimestamp(System.currentTimeMillis());
         if (body != null) {
            if (type == Message.TEXT_TYPE) {
               message.getBodyBuffer().writeNullableSimpleString(new SimpleString(body));
            } else {
               message.getBodyBuffer().writeBytes(Base64.decode(body));
            }
         }
         message.setAddress(queue.getAddress());
         ByteBuffer buffer = ByteBuffer.allocate(8);
         buffer.putLong(queue.getID());
         message.putBytesProperty(Message.HDR_ROUTE_TO_IDS, buffer.array());
         postOffice.route(message, true);
         return "" + message.getMessageID();
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      }
   }

   @Override
   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.sendMessageToDeadLetterAddress(messageID);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int changeMessagesPriority(final String filterStr, final int newPriority) throws Exception {
      checkStarted();

      clearIO();
      try {
         if (newPriority < 0 || newPriority > 9) {
            throw ActiveMQMessageBundle.BUNDLE.invalidNewPriority(newPriority);
         }
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.changeReferencesPriority(filter, (byte) newPriority);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception {
      checkStarted();

      clearIO();
      try {
         if (newPriority < 0 || newPriority > 9) {
            throw ActiveMQMessageBundle.BUNDLE.invalidNewPriority(newPriority);
         }
         return queue.changeReferencePriority(messageID, (byte) newPriority);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessageCounter() {
      checkStarted();

      clearIO();
      try {
         return MessageCounterInfo.toJSon(counter);
      } catch (Exception e) {
         throw new IllegalStateException(e);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetMessageCounter() {
      checkStarted();

      clearIO();
      try {
         counter.resetCounter();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessageCounterAsHTML() {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[]{counter});
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessageCounterHistory() throws Exception {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterHistory(counter);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessageCounterHistoryAsHTML() {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterHistoryAsHTML(new MessageCounter[]{counter});
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void pause() {
      checkStarted();

      clearIO();
      try {
         queue.pause();
      } finally {
         blockOnIO();
      }
   }


   @Override
   public void pause(boolean persist) {
      checkStarted();

      clearIO();
      try {
         queue.pause(persist);
      } finally {
         blockOnIO();
      }
   }
   @Override
   public void resume() {
      checkStarted();

      clearIO();
      try {
         queue.resume();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPaused() throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.isPaused();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public CompositeData[] browse(int page, int pageSize) throws Exception {
      String filter = null;
      checkStarted();

      clearIO();
      try {
         long index = 0;
         long start = (page - 1) * pageSize;
         long end = Math.min(page * pageSize, queue.getMessageCount());

         ArrayList<CompositeData> c = new ArrayList<>();
         Filter thefilter = FilterImpl.createFilter(filter);
         queue.flushExecutor();

         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext() && index < end) {
                  MessageReference ref = iterator.next();
                  if (thefilter == null || thefilter.match(ref.getMessage())) {
                     if (index >= start) {
                        c.add(OpenTypeSupport.convert(ref));
                     }
                  }
                  index++;
               }
            } catch (NoSuchElementException ignored) {
               // this could happen through paging browsing
            }

            CompositeData[] rc = new CompositeData[c.size()];
            c.toArray(rc);
            return rc;
         }
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public CompositeData[] browse() throws Exception {
      return browse(null);
   }
   @Override
   public CompositeData[] browse(String filter) throws Exception {
      checkStarted();

      clearIO();
      try {
         int pageSize = addressSettingsRepository.getMatch(queue.getName().toString()).getManagementBrowsePageSize();
         int currentPageSize = 0;
         ArrayList<CompositeData> c = new ArrayList<>();
         Filter thefilter = FilterImpl.createFilter(filter);
         queue.flushExecutor();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext() && currentPageSize++ < pageSize) {
                  MessageReference ref = iterator.next();
                  if (thefilter == null || thefilter.match(ref.getMessage())) {
                     c.add(OpenTypeSupport.convert(ref));

                  }
               }
            } catch (NoSuchElementException ignored) {
               // this could happen through paging browsing
            }

            CompositeData[] rc = new CompositeData[c.size()];
            c.toArray(rc);
            return rc;
         }
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void flushExecutor() {
      checkStarted();

      clearIO();
      try {
         queue.flushExecutor();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetAllGroups() {
      checkStarted();

      clearIO();
      try {
         queue.resetAllGroups();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetGroup(String groupID) {
      checkStarted();

      clearIO();
      try {
         queue.resetGroup(SimpleString.toSimpleString(groupID));
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getGroupCount() {
      checkStarted();

      clearIO();
      try {
         return queue.getGroupCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listGroupsAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         Map<SimpleString, Consumer> groups = queue.getGroups();

         JsonArrayBuilder jsonArray = JsonLoader.createArrayBuilder();

         for (Map.Entry<SimpleString, Consumer> group : groups.entrySet()) {

            if (group.getValue() instanceof ServerConsumer) {
               ServerConsumer serverConsumer = (ServerConsumer) group.getValue();

               JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("groupID", group.getKey().toString()).add("consumerID", serverConsumer.getID()).add("connectionID", serverConsumer.getConnectionID().toString()).add("sessionID", serverConsumer.getSessionID()).add("browseOnly", serverConsumer.isBrowseOnly()).add("creationTime", serverConsumer.getCreationTime());

               jsonArray.add(obj);
            }

         }

         return jsonArray.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listConsumersAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         Collection<Consumer> consumers = queue.getConsumers();

         JsonArrayBuilder jsonArray = JsonLoader.createArrayBuilder();

         for (Consumer consumer : consumers) {

            if (consumer instanceof ServerConsumer) {
               ServerConsumer serverConsumer = (ServerConsumer) consumer;

               JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("consumerID", serverConsumer.getID()).add("connectionID", serverConsumer.getConnectionID().toString()).add("sessionID", serverConsumer.getSessionID()).add("browseOnly", serverConsumer.isBrowseOnly()).add("creationTime", serverConsumer.getCreationTime());

               jsonArray.add(obj);
            }

         }

         return jsonArray.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(QueueControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(QueueControl.class);
   }

   @Override
   public void resetMessagesAdded() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesAdded();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public void resetMessagesAcknowledged() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesAcknowledged();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public void resetMessagesExpired() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesExpired();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public void resetMessagesKilled() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesKilled();
      } finally {
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
