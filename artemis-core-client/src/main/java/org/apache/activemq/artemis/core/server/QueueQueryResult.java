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
package org.apache.activemq.artemis.core.server;

import org.apache.activemq.artemis.api.core.SimpleString;

public class QueueQueryResult {

   private SimpleString name;

   private boolean exists;

   private boolean durable;

   private int consumerCount;

   private long messageCount;

   private SimpleString filterString;

   private SimpleString address;

   private boolean temporary;

   private boolean autoCreateJmsQueues;

   public QueueQueryResult(final SimpleString name,
                           final SimpleString address,
                           final boolean durable,
                           final boolean temporary,
                           final SimpleString filterString,
                           final int consumerCount,
                           final long messageCount,
                           final boolean autoCreateJmsQueues) {
      this(name, address, durable, temporary, filterString, consumerCount, messageCount, autoCreateJmsQueues, true);
   }

   public QueueQueryResult(final SimpleString name,
                           final SimpleString address,
                           final boolean durable,
                           final boolean temporary,
                           final SimpleString filterString,
                           final int consumerCount,
                           final long messageCount,
                           final boolean autoCreateJmsQueues,
                           final boolean exists) {
      this.durable = durable;

      this.temporary = temporary;

      this.consumerCount = consumerCount;

      this.messageCount = messageCount;

      this.filterString = filterString;

      this.address = address;

      this.name = name;

      this.autoCreateJmsQueues = autoCreateJmsQueues;

      this.exists = exists;
   }

   public boolean isExists() {
      return exists;
   }

   public boolean isDurable() {
      return durable;
   }

   public int getConsumerCount() {
      return consumerCount;
   }

   public long getMessageCount() {
      return messageCount;
   }

   public SimpleString getFilterString() {
      return filterString;
   }

   public SimpleString getAddress() {
      return address;
   }

   public SimpleString getName() {
      return name;
   }

   public boolean isTemporary() {
      return temporary;
   }

   public boolean isAutoCreateJmsQueues() {
      return autoCreateJmsQueues;
   }

}
