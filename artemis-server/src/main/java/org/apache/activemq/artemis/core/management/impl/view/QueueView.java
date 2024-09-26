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
package org.apache.activemq.artemis.core.management.impl.view;

import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.management.impl.view.predicate.QueueFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.utils.JsonLoader;

public class QueueView extends ActiveMQAbstractView<QueueControl> {

   private static final String defaultSortField = QueueField.NAME.getName();

   private ActiveMQServer server;

   public QueueView(ActiveMQServer server) {
      super();
      this.predicate = new QueueFilterPredicate(server);
      this.server = server;
   }

   @Override
   public Class getClassT() {
      return QueueControl.class;
   }

   @Override
   public JsonObjectBuilder toJson(QueueControl queue) {
      Queue q = server.locateQueue(SimpleString.of(queue.getName()));
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder()
         .add(QueueField.ID.getName(), toString(queue.getID()))
         .add(QueueField.NAME.getName(), toString(queue.getName()))
         .add(QueueField.ADDRESS.getName(), toString(queue.getAddress()))
         .add(QueueField.FILTER.getName(), toString(queue.getFilter()))
         .add(QueueField.DURABLE.getName(), toString(queue.isDurable()))
         .add(QueueField.PAUSED.getName(), toString(q.isPaused()))
         .add(QueueField.TEMPORARY.getName(), toString(queue.isTemporary()))
         .add(QueueField.PURGE_ON_NO_CONSUMERS.getName(), toString(queue.isPurgeOnNoConsumers()))
         .add(QueueField.CONSUMER_COUNT.getName(), toString(queue.getConsumerCount()))
         .add(QueueField.MAX_CONSUMERS.getName(), toString(queue.getMaxConsumers()))
         .add(QueueField.AUTO_CREATED.getName(), toString(q.isAutoCreated()))
         .add(QueueField.USER.getName(), toString(q.getUser()))
         .add(QueueField.ROUTING_TYPE.getName(), toString(queue.getRoutingType()))
         .add(QueueField.MESSAGES_ADDED.getName(), toString(queue.getMessagesAdded()))
         .add(QueueField.MESSAGE_COUNT.getName(), toString(queue.getMessageCount()))
         .add(QueueField.MESSAGES_ACKED.getName(), toString(queue.getMessagesAcknowledged()))
         .add(QueueField.MESSAGES_EXPIRED.getName(), toString(queue.getMessagesExpired()))
         .add(QueueField.DELIVERING_COUNT.getName(), toString(queue.getDeliveringCount()))
         .add(QueueField.MESSAGES_KILLED.getName(), toString(queue.getMessagesKilled()))
         .add(QueueField.DIRECT_DELIVER.getName(), toString(q.isDirectDeliver()))
         .add(QueueField.EXCLUSIVE.getName(), toString(queue.isExclusive()))
         .add(QueueField.LAST_VALUE.getName(), toString(queue.isLastValue()))
         .add(QueueField.LAST_VALUE_KEY.getName(), toString(queue.getLastValueKey()))
         .add(QueueField.SCHEDULED_COUNT.getName(), toString(queue.getScheduledCount()))
         .add(QueueField.GROUP_REBALANCE.getName(), toString(queue.isGroupRebalance()))
         .add(QueueField.GROUP_REBALANCE_PAUSE_DISPATCH.getName(), toString(queue.isGroupRebalancePauseDispatch()))
         .add(QueueField.GROUP_BUCKETS.getName(), toString(queue.getGroupBuckets()))
         .add(QueueField.GROUP_FIRST_KEY.getName(), toString(queue.getGroupFirstKey()))
         .add(QueueField.ENABLED.getName(), toString(queue.isEnabled()))
         .add(QueueField.RING_SIZE.getName(), toString(queue.getRingSize()))
         .add(QueueField.CONSUMERS_BEFORE_DISPATCH.getName(), toString(queue.getConsumersBeforeDispatch()))
         .add(QueueField.DELAY_BEFORE_DISPATCH.getName(), toString(queue.getDelayBeforeDispatch()))
         .add(QueueField.AUTO_DELETE.getName(), toString(q.isAutoDelete()))
         .add(QueueField.INTERNAL_QUEUE.getName(), toString(q.isInternalQueue()));
      return obj;
   }

   @Override
   public Object getField(QueueControl queue, String fieldName) {
      Queue q = server.locateQueue(SimpleString.of(queue.getName()));

      QueueField field = QueueField.valueOfName(fieldName);

      switch (field) {
         case ID:
            return queue.getID();
         case NAME:
            return queue.getName();
         case ADDRESS:
            return queue.getAddress();
         case FILTER:
            return queue.getFilter();
         case DURABLE:
            return queue.isDurable();
         case PAUSED:
            return q.isPaused();
         case TEMPORARY:
            return queue.isTemporary();
         case PURGE_ON_NO_CONSUMERS:
            return queue.isPurgeOnNoConsumers();
         case CONSUMER_COUNT:
            return queue.getConsumerCount();
         case MAX_CONSUMERS:
            return queue.getMaxConsumers();
         case AUTO_CREATED:
            return q.isAutoCreated();
         case USER:
            return q.getUser();
         case ROUTING_TYPE:
            return queue.getRoutingType();
         case MESSAGES_ADDED:
            return queue.getMessagesAdded();
         case MESSAGE_COUNT:
            return queue.getMessageCount();
         case MESSAGES_ACKED:
            return queue.getMessagesAcknowledged();
         case MESSAGES_EXPIRED:
            return queue.getMessagesExpired();
         case DELIVERING_COUNT:
            return queue.getDeliveringCount();
         case MESSAGES_KILLED:
            return queue.getMessagesKilled();
         case DIRECT_DELIVER:
            return q.isDirectDeliver();
         case EXCLUSIVE:
            return q.isExclusive();
         case LAST_VALUE:
            return q.isLastValue();
         case LAST_VALUE_KEY:
            return q.getLastValueKey();
         case SCHEDULED_COUNT:
            return q.getScheduledCount();
         case GROUP_REBALANCE:
            return queue.isGroupRebalance();
         case GROUP_REBALANCE_PAUSE_DISPATCH:
            return queue.isGroupRebalancePauseDispatch();
         case GROUP_BUCKETS:
            return queue.getGroupBuckets();
         case GROUP_FIRST_KEY:
            return queue.getGroupFirstKey();
         case ENABLED:
            return q.isEnabled();
         case RING_SIZE:
            return q.getRingSize();
         case CONSUMERS_BEFORE_DISPATCH:
            return q.getConsumersBeforeDispatch();
         case DELAY_BEFORE_DISPATCH:
            return q.getDelayBeforeDispatch();
         case INTERNAL_QUEUE:
            return q.isInternalQueue();
         default:
            throw new IllegalArgumentException("Unsupported field, " + fieldName);
      }
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortField;
   }
}
