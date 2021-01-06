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

import javax.json.JsonObjectBuilder;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.management.impl.view.predicate.QueueFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.utils.JsonLoader;

public class QueueView extends ActiveMQAbstractView<QueueControl> {

   private static final String defaultSortColumn = "name";

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
      Queue q = server.locateQueue(new SimpleString(queue.getName()));
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("id", toString(queue.getID()))
         .add("name", toString(queue.getName())).add("address", toString(queue.getAddress()))
         .add("filter", toString(queue.getFilter())).add("rate", toString(q.getRate()))
         .add("durable", toString(queue.isDurable())).add("paused", toString(q.isPaused()))
         .add("temporary", toString(queue.isTemporary()))
         .add("purgeOnNoConsumers", toString(queue.isPurgeOnNoConsumers()))
         .add("consumerCount", toString(queue.getConsumerCount()))
         .add("maxConsumers", toString(queue.getMaxConsumers()))
         .add("autoCreated", toString(q.isAutoCreated()))
         .add("user", toString(q.getUser()))
         .add("routingType", toString(queue.getRoutingType()))
         .add("messagesAdded", toString(queue.getMessagesAdded()))
         .add("messageCount", toString(queue.getMessageCount()))
         .add("messagesAcked", toString(queue.getMessagesAcknowledged()))
         .add("deliveringCount", toString(queue.getDeliveringCount()))
         .add("messagesKilled", toString(queue.getMessagesKilled()))
         .add("directDeliver", toString(q.isDirectDeliver()))
         .add("exclusive", toString(queue.isExclusive()))
         .add("lastValue", toString(queue.isLastValue()))
         .add("lastValueKey", toString(queue.getLastValueKey()))
         .add("scheduledCount", toString(queue.getScheduledCount()))
         .add("groupRebalance", toString(queue.isGroupRebalance()))
         .add("groupRebalancePauseDispatch", toString(queue.isGroupRebalancePauseDispatch()))
         .add("groupBuckets", toString(queue.getGroupBuckets()))
         .add("groupFirstKey", toString(queue.getGroupFirstKey()))
         .add("enabled", toString(queue.isEnabled()))
         .add("ringSize", toString(queue.getRingSize()))
         .add("consumersBeforeDispatch", toString(queue.getConsumersBeforeDispatch()))
         .add("delayBeforeDispatch", toString(queue.getDelayBeforeDispatch()));
      return obj;
   }

   @Override
   public Object getField(QueueControl queue, String fieldName) {
      Queue q = server.locateQueue(new SimpleString(queue.getName()));
      switch (fieldName) {
         case "id":
            return queue.getID();
         case "name":
            return queue.getName();
         case "address":
            return queue.getAddress();
         case "filter":
            return queue.getFilter();
         case "rate":
            return q.getRate();
         case "durable":
            return queue.isDurable();
         case "paused":
            return q.isPaused();
         case "temporary":
            return queue.isTemporary();
         case "purgeOnNoConsumers":
            return queue.isPurgeOnNoConsumers();
         case "consumerCount":
            return queue.getConsumerCount();
         case "maxConsumers":
            return queue.getMaxConsumers();
         case "autoCreated":
            return q.isAutoCreated();
         case "user":
            return q.getUser();
         case "routingType":
            return queue.getRoutingType();
         case "messagesAdded":
            return queue.getMessagesAdded();
         case "messageCount":
            return queue.getMessageCount();
         case "messagesAcked":
            return queue.getMessagesAcknowledged();
         case "deliveringCount":
            return queue.getDeliveringCount();
         case "messagesKilled":
            return queue.getMessagesKilled();
         case "deliverDeliver":
            return q.isDirectDeliver();
         case "exclusive":
            return q.isExclusive();
         case "lastValue":
            return q.isLastValue();
         case "lastValueKey":
            return q.getLastValueKey();
         case "scheduledCount":
            return q.getScheduledCount();
         case "groupRebalance":
            return queue.isGroupRebalance();
         case "groupRebalancePauseDispatch":
            return queue.isGroupRebalancePauseDispatch();
         case "groupBuckets":
            return queue.getGroupBuckets();
         case "groupFirstKey":
            return queue.getGroupFirstKey();
         case "enabled":
            return q.isEnabled();
         case "ringSize":
            return q.getRingSize();
         case "consumersBeforeDispatch":
            return q.getConsumersBeforeDispatch();
         case "delayBeforeDispatch":
            return q.getDelayBeforeDispatch();
         default:
            throw new IllegalArgumentException("Unsupported field, " + fieldName);
      }
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortColumn;
   }
}
