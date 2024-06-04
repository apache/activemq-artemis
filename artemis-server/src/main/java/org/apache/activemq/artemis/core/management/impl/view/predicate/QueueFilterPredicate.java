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
package org.apache.activemq.artemis.core.management.impl.view.predicate;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.management.impl.view.QueueField;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.Queue;

public class QueueFilterPredicate extends ActiveMQFilterPredicate<QueueControl> {

   private QueueField f;

   private ActiveMQServer server;

   public QueueFilterPredicate(ActiveMQServer server) {
      super();
      this.server = server;
   }

   @Override
   public boolean test(QueueControl queue) {
      // Using switch over enum vs string comparison is better for perf.
      try {
         if (f == null)
            return true;
         switch (f) {
            case ID:
               return matches(queue.getID());
            case NAME:
               return matches(queue.getName());
            case CONSUMER_ID:
               Queue q = server.locateQueue(SimpleString.of(queue.getName()));
               for (Consumer consumer : q.getConsumers()) {
                  if (matches(consumer.sequentialID()))
                     return true;
               }
               return false;
            case MAX_CONSUMERS:
               return matches(queue.getMaxConsumers());
            case ADDRESS:
               return matches(queue.getAddress());
            case FILTER:
               return matches(queue.getFilter());
            case MESSAGE_COUNT:
               return matches(queue.getMessageCount());
            case CONSUMER_COUNT:
               return matches(queue.getConsumerCount());
            case DELIVERING_COUNT:
               return matches(queue.getDeliveringCount());
            case MESSAGES_ADDED:
               return matches(queue.getMessagesAdded());
            case MESSAGES_ACKED:
               return matches(queue.getMessagesAcknowledged());
            case MESSAGES_EXPIRED:
               return matches(queue.getMessagesExpired());
            case ROUTING_TYPE:
               return matches(queue.getRoutingType());
            case AUTO_CREATED:
               return matches(server.locateQueue(SimpleString.of(queue.getName())).isAutoCreated());
            case DURABLE:
               return matches(queue.isDurable());
            case PAUSED:
               return matches(queue.isPaused());
            case TEMPORARY:
               return matches(queue.isTemporary());
            case PURGE_ON_NO_CONSUMERS:
               return matches(queue.isPurgeOnNoConsumers());
            case MESSAGES_KILLED:
               return matches(queue.getMessagesKilled());
            case EXCLUSIVE:
               return matches(queue.isExclusive());
            case LAST_VALUE:
               return matches(queue.isLastValue());
            case SCHEDULED_COUNT:
               return matches(queue.getScheduledCount());
            case USER:
               return matches(queue.getUser());
            case INTERNAL_QUEUE:
               return matches(queue.isInternalQueue());
            default:
               return true;
         }
      } catch (Exception e) {
         return true;
      }

   }

   @Override
   public void setField(String field) {
      if (field != null && !field.equals("")) {
         this.f = QueueField.valueOfName(field);

         //for backward compatibility
         if (this.f == null) {
            this.f = QueueField.valueOf(field);
         }
      }
   }
}
