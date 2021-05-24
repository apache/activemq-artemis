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
import javax.transaction.xa.Xid;
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
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.management.impl.openmbean.OpenTypeSupport;
import org.apache.activemq.artemis.core.messagecounter.MessageCounter;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterHelper;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.impl.RefsOperation;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;
import org.apache.activemq.artemis.selector.filter.Filterable;
import org.apache.activemq.artemis.logs.AuditLogger;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;

public class QueueControlImpl extends AbstractControl implements QueueControl {

   public static final int FLUSH_LIMIT = 500;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Queue queue;

   private final String address;

   private final ActiveMQServer server;

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
                           final ActiveMQServer server,
                           final StorageManager storageManager,
                           final SecurityStore securityStore,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception {
      super(QueueControl.class, storageManager);
      this.queue = queue;
      this.address = address;
      this.server = server;
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getName(queue);
      }
      clearIO();
      try {
         return queue.getName().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getAddress() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getAddress(queue);
      }

      checkStarted();

      return address;
   }

   @Override
   public String getFilter() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getFilter(queue);
      }

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

      if (AuditLogger.isEnabled()) {
         AuditLogger.isDurable(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getUser(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getRoutingType(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.isTemporary(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.isTemporary();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isRetroactiveResource() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isRetroactiveResource(queue);
      }
      checkStarted();

      clearIO();
      try {
         return ResourceNames.isRetroactiveResource(server.getInternalNamingPrefix(), queue.getName());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessageCount() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessageCount(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getPersistentSize(queue);
      }

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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDurableMessageCount(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDurablePersistSize(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getConsumerCount(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDeliveringCount(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDeliveringSize(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDurableDeliveringCount(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDurableDeliveringSize(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesAdded(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesAcknowledged(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesAcknowledged();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getAcknowledgeAttempts() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesAcknowledged(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.getAcknowledgeAttempts();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesExpired() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesExpired(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesKilled(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getID(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getScheduledCount(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getScheduledSize(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDurableScheduledCount(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDurableScheduledSize(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDeadLetterAddress(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getExpiryAddress(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMaxConsumers(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.isPurgeOnNoConsumers(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.isPurgeOnNoConsumers();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void disable() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.disable(queue);
      }
      checkStarted();

      clearIO();
      try {
         server.getPostOffice().updateQueue(queue.getQueueConfiguration().setEnabled(false));
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void enable() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.enable(queue);
      }
      checkStarted();

      clearIO();
      try {
         server.getPostOffice().updateQueue(queue.getQueueConfiguration().setEnabled(true));
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isEnabled() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isEnabled(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.isEnabled();
      } finally {
         blockOnIO();
      }
   }


   @Override
   public boolean isConfigurationManaged() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isConfigurationManaged(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.isConfigurationManaged();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isExclusive() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isExclusive(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.isLastValue(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.isLastValue();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getLastValueKey() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.lastValueKey(queue);
      }
      checkStarted();

      clearIO();
      try {
         if (queue.getLastValueKey() != null) {
            return queue.getLastValueKey().toString();
         } else {
            return null;
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getConsumersBeforeDispatch() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.consumersBeforeDispatch(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.getConsumersBeforeDispatch();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDelayBeforeDispatch() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.delayBeforeDispatch(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.getDelayBeforeDispatch();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object>[] listScheduledMessages() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.listScheduledMessages(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listScheduledMessagesAsJSON(queue);
      }
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
      final int attributeSizeLimit = addressSettingsRepository.getMatch(address).getManagementMessageAttributeSizeLimit();
      Map<String, Object>[] messages = new Map[refs.size()];
      int i = 0;
      for (MessageReference ref : refs) {
         Message message = ref.getMessage();
         messages[i++] = message.toMap(attributeSizeLimit);
      }
      return messages;
   }

   @Override
   public Map<String, Map<String, Object>[]> listDeliveringMessages() throws ActiveMQException {
      if (AuditLogger.isEnabled()) {
         AuditLogger.listDeliveringMessages(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listDeliveringMessagesAsJSON(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listMessages(queue, filterStr);
      }
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         List<Map<String, Object>> messages = new ArrayList<>();
         queue.flushExecutor();
         final AddressSettings addressSettings = addressSettingsRepository.getMatch(address);
         final int attributeSizeLimit = addressSettings.getManagementMessageAttributeSizeLimit();
         final int limit = addressSettings.getManagementBrowsePageSize();
         int count = 0;
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext() && count++ < limit) {
                  MessageReference ref = iterator.next();
                  if (filter == null || filter.match(ref.getMessage())) {
                     Message message = ref.getMessage();
                     messages.add(message.toMap(attributeSizeLimit));
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listMessagesAsJSON(queue);
      }
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listMessages(filter));
      } finally {
         blockOnIO();
      }
   }

   protected Map<String, Object>[] getFirstMessage() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getFirstMessage(queue);
      }
      checkStarted();

      clearIO();
      try {
         List<Map<String, Object>> messages = new ArrayList<>();
         queue.flushExecutor();
         final int attributeSizeLimit = addressSettingsRepository.getMatch(address).getManagementMessageAttributeSizeLimit();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            // returns just the first, as it's the first only
            if (iterator.hasNext()) {
               MessageReference ref = iterator.next();
               Message message = ref.getMessage();
               messages.add(message.toMap(attributeSizeLimit));
            }
            return messages.toArray(new Map[1]);
         }
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getFirstMessageAsJSON() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getFirstMessageAsJSON(queue);
      }
      return toJSON(getFirstMessage());
   }

   @Override
   public Long getFirstMessageTimestamp() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getFirstMessageTimestamp(queue);
      }

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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getFirstMessageAge(queue);
      }

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
      if (AuditLogger.isEnabled()) {
         AuditLogger.countMessages(queue, filterStr);
      }

      Long value = internalCountMessages(filterStr, null).get(null);
      return value == null ? 0 : value;
   }

   @Override
   public String countMessages(final String filterStr, final String groupByProperty) throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.countMessages(queue, filterStr, groupByProperty);
      }

      return JsonUtil.toJsonObject(internalCountMessages(filterStr, groupByProperty)).toString();
   }

   private Map<String, Long> internalCountMessages(final String filterStr, final String groupByPropertyStr) throws Exception {
      checkStarted();

      clearIO();

      Map<String, Long> result = new HashMap<>();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         SimpleString groupByProperty = SimpleString.toSimpleString(groupByPropertyStr);
         if (filter == null && groupByProperty == null) {
            result.put(null, getMessageCount());
         } else {
            final int limit = addressSettingsRepository.getMatch(address).getManagementBrowsePageSize();
            int count = 0;
            try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
               try {
                  while (iterator.hasNext() && count++ < limit) {
                     Message message = iterator.next().getMessage();
                     internalComputeMessage(result, filter, groupByProperty, message);
                  }
               } catch (NoSuchElementException ignored) {
                  // this could happen through paging browsing
               }
            }
         }
         return result;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long countDeliveringMessages(final String filterStr) throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.countDeliveringMessages(queue, filterStr);
      }

      Long value = internalCountDeliveryMessages(filterStr, null).get(null);
      return value == null ? 0 : value;
   }

   @Override
   public String countDeliveringMessages(final String filterStr, final String groupByProperty) throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.countDeliveringMessages(queue, filterStr, groupByProperty);
      }

      return JsonUtil.toJsonObject(internalCountDeliveryMessages(filterStr, groupByProperty)).toString();
   }

   private Map<String, Long> internalCountDeliveryMessages(final String filterStr, final String groupByPropertyStr) throws Exception {
      checkStarted();

      clearIO();

      Map<String, Long> result = new HashMap<>();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         SimpleString groupByProperty = SimpleString.toSimpleString(groupByPropertyStr);
         if (filter == null && groupByProperty == null) {
            result.put(null, Long.valueOf(getDeliveringCount()));
         } else {
            Map<String, List<MessageReference>> deliveringMessages = queue.getDeliveringMessages();
            deliveringMessages.forEach((s, messageReferenceList) ->
                            messageReferenceList.forEach(messageReference ->
                                    internalComputeMessage(result, filter, groupByProperty, messageReference.getMessage())
                            ));
         }
         return result;
      } finally {
         blockOnIO();
      }
   }

   private void internalComputeMessage(Map<String, Long> result, Filter filter, SimpleString groupByProperty, Message message) {
      if (filter == null || filter.match(message)) {
         if (groupByProperty == null) {
            result.compute(null, (k, v) -> v == null ? 1 : ++v);
         } else {
            Object value = message.getObjectProperty(groupByProperty);
            String valueStr = value == null ? null : value.toString();
            result.compute(valueStr, (k, v) -> v == null ? 1 : ++v);
         }
      }
   }


   @Override
   public boolean removeMessage(final long messageID) throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.removeMessage(queue, messageID);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.removeMessages(queue, flushLimit, filterStr);
      }
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         int removed = 0;
         try {
            removed = queue.deleteMatchingReferences(flushLimit, filter);
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.removeMessagesSuccess(removed, queue.getName().toString());
            }
         } catch (Exception e) {
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.removeMessagesFailure(queue.getName().toString());
            }
            throw e;
         }
         return removed;
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.expireMessage(queue, messageID);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.expireMessages(queue, filterStr);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.retryMessage(queue, messageID);
      }

      checkStarted();
      clearIO();

      try {
         Filter singleMessageFilter = new Filter() {
            @Override
            public boolean match(Message message) {
               return message.getMessageID() == messageID;
            }

            @Override
            public boolean match(Map<String, String> map) {
               return false;
            }

            @Override
            public boolean match(Filterable filterable) {
               return false;
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.retryMessages(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.moveMessage(queue, messageID, otherQueueName, rejectDuplicates);
      }
      checkStarted();

      clearIO();
      try {
         Binding binding = server.getPostOffice().getBinding(new SimpleString(otherQueueName));

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
      return moveMessages(flushLimit, filterStr, otherQueueName, rejectDuplicates, -1);
   }

   @Override
   public int moveMessages(final int flushLimit,
                           final String filterStr,
                           final String otherQueueName,
                           final boolean rejectDuplicates,
                           final int messageCount) throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.moveMessages(queue, flushLimit, filterStr, otherQueueName, rejectDuplicates, messageCount);
      }
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         Binding binding = server.getPostOffice().getBinding(new SimpleString(otherQueueName));

         if (binding == null) {
            throw ActiveMQMessageBundle.BUNDLE.noQueueFound(otherQueueName);
         }

         int retValue = queue.moveReferences(flushLimit, filter, binding.getAddress(), rejectDuplicates, messageCount, binding);
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.sendMessagesToDeadLetterAddress(queue, filterStr);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.sendMessage(queue, null, headers, type, body, durable, user, "****");
      }
      try {
         String s = sendMessage(queue.getAddress(), server, headers, type, body, durable, user, password, queue.getID());
         if (AuditLogger.isResourceLoggingEnabled()) {
            AuditLogger.sendMessageSuccess(queue.getName().toString(), user);
         }
         return s;
      } catch (Exception e) {
         if (AuditLogger.isResourceLoggingEnabled()) {
            AuditLogger.sendMessageFailure(queue.getName().toString(), user);
         }
         throw new IllegalStateException(e.getMessage());
      }
   }

   @Override
   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.sendMessageToDeadLetterAddress(queue, messageID);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.changeMessagesPriority(queue, filterStr, newPriority);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.changeMessagePriority(queue, messageID, newPriority);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listMessageCounter(queue);
      }
      checkStarted();

      clearIO();
      try {
         return counter.toJSon();
      } catch (Exception e) {
         throw new IllegalStateException(e);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetMessageCounter() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.resetMessageCounter(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listMessageCounterAsHTML(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listMessageCounterHistory(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listMessageCounterHistoryAsHTML(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.pause(queue);
      }
      checkStarted();

      clearIO();
      try {
         try {
            queue.pause();
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.pauseQueueSuccess(queue.getName().toString());
            }
         } catch (Exception e) {
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.pauseQueueFailure(queue.getName().toString());
            }
         }
      } finally {
         blockOnIO();
      }
   }


   @Override
   public void pause(boolean persist) {
      if (AuditLogger.isEnabled()) {
         AuditLogger.pause(queue, persist);
      }
      checkStarted();

      clearIO();
      try {
         try {
            queue.pause(persist);
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.pauseQueueSuccess(queue.getName().toString());
            }
         } catch (Exception e) {
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.pauseQueueFailure(queue.getName().toString());
            }
         }
      } finally {
         blockOnIO();
      }
   }
   @Override
   public void resume() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.resume(queue);
      }
      checkStarted();

      clearIO();
      try {
         try {
            queue.resume();
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.resumeQueueSuccess(queue.getName().toString());
            }
         } catch (Exception e) {
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.resumeQueueFailure(queue.getName().toString());
            }
            e.printStackTrace();
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPaused() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isPaused(queue);
      }
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
      return browse(page, pageSize, null);
   }

   @Override
   public CompositeData[] browse(int page, int pageSize, String filter) throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.browse(queue, page, pageSize);
      }
      checkStarted();

      clearIO();
      try {
         long index = 0;
         long start = (long) (page - 1) * pageSize;
         long end = Math.min(page * pageSize, queue.getMessageCount());

         ArrayList<CompositeData> c = new ArrayList<>();
         Filter thefilter = FilterImpl.createFilter(filter);
         queue.flushExecutor();

         final int attributeSizeLimit = addressSettingsRepository.getMatch(address).getManagementMessageAttributeSizeLimit();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext() && index < end) {
                  MessageReference ref = iterator.next();
                  if (thefilter == null || thefilter.match(ref.getMessage())) {
                     if (index >= start) {
                        c.add(OpenTypeSupport.convert(ref, attributeSizeLimit));
                     }
                  }
                  index++;
               }
            } catch (NoSuchElementException ignored) {
               // this could happen through paging browsing
            }

            CompositeData[] rc = new CompositeData[c.size()];
            c.toArray(rc);
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.browseMessagesSuccess(queue.getName().toString(), c.size());
            }
            return rc;
         }
      } catch (ActiveMQException e) {
         if (AuditLogger.isResourceLoggingEnabled()) {
            AuditLogger.browseMessagesFailure(queue.getName().toString());
         }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.browse(queue, filter);
      }
      checkStarted();

      clearIO();
      try {
         final AddressSettings addressSettings = addressSettingsRepository.getMatch(address);
         final int attributeSizeLimit = addressSettings.getManagementMessageAttributeSizeLimit();
         final int limit = addressSettings.getManagementBrowsePageSize();
         int currentPageSize = 0;
         ArrayList<CompositeData> c = new ArrayList<>();
         Filter thefilter = FilterImpl.createFilter(filter);
         queue.flushExecutor();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext() && currentPageSize++ < limit) {
                  MessageReference ref = iterator.next();
                  if (thefilter == null || thefilter.match(ref.getMessage())) {
                     c.add(OpenTypeSupport.convert(ref, attributeSizeLimit));

                  }
               }
            } catch (NoSuchElementException ignored) {
               // this could happen through paging browsing
            }

            CompositeData[] rc = new CompositeData[c.size()];
            c.toArray(rc);
            if (AuditLogger.isResourceLoggingEnabled()) {
               AuditLogger.browseMessagesSuccess(queue.getName().toString(), currentPageSize);
            }
            return rc;
         }
      } catch (ActiveMQException e) {
         if (AuditLogger.isResourceLoggingEnabled()) {
            AuditLogger.browseMessagesFailure(queue.getName().toString());
         }
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void flushExecutor() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.flushExecutor(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.resetAllGroups(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.resetGroup(queue, groupID);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getGroupCount(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.listGroupsAsJSON(queue);
      }
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
   public long getRingSize() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getRingSize(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.getRingSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listConsumersAsJSON() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.listConsumersAsJSON(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.resetMessagesAdded(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.resetMessagesAcknowledged(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.resetMessagesExpired(queue);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.resetMessagesKilled(queue);
      }
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesKilled();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public boolean isGroupRebalance() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isGroupRebalance(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.isGroupRebalance();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isGroupRebalancePauseDispatch() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isGroupRebalancePauseDispatch(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.isGroupRebalancePauseDispatch();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getGroupBuckets() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getGroupBuckets(queue);
      }
      checkStarted();

      clearIO();
      try {
         return queue.getGroupBuckets();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getGroupFirstKey() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getGroupFirstKey(queue);
      }
      checkStarted();

      clearIO();
      try {
         SimpleString groupFirstKey = queue.getGroupFirstKey();

         return groupFirstKey != null ? groupFirstKey.toString() : null;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getPreparedTransactionMessageCount() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getPreparedTransactionMessageCount(queue);
      }
      checkStarted();

      clearIO();
      try {
         int count = 0;

         ResourceManager resourceManager = server.getResourceManager();

         if (resourceManager != null) {
            List<Xid> preparedTransactions = resourceManager.getPreparedTransactions();

            for (Xid preparedTransaction : preparedTransactions) {
               Transaction transaction = resourceManager.getTransaction(preparedTransaction);

               if (transaction != null) {
                  List<TransactionOperation> allOperations = transaction.getAllOperations();

                  for (TransactionOperation operation : allOperations) {
                     if (operation instanceof RefsOperation) {
                        RefsOperation refsOperation = (RefsOperation) operation;
                        List<MessageReference> references = refsOperation.getReferencesToAcknowledge();
                        for (MessageReference reference : references) {
                           if (reference != null && reference.getQueue().getName().equals(queue.getName())) {
                              count++;
                           }
                        }
                     }
                  }
               }
            }
         }
         return count;
      } finally {
         blockOnIO();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkStarted() {
      if (!server.getPostOffice().isStarted()) {
         throw new IllegalStateException("Broker is not started. Queue can not be managed yet");
      }
   }

   // Inner classes -------------------------------------------------
}
