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

import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;

public class BindingQueryResult {

   private boolean exists;

   private List<SimpleString> queueNames;

   private boolean autoCreateQueues;

   private boolean autoCreateAddresses;

   private boolean defaultDeleteOnNoConsumers;

   private int defaultMaxConsumers;

   public BindingQueryResult(final boolean exists,
                             final List<SimpleString> queueNames,
                             final boolean autoCreateQueues,
                             final boolean autoCreateAddresses,
                             final boolean defaultDeleteOnNoConsumers,
                             final int defaultMaxConsumers) {
      this.exists = exists;

      this.queueNames = queueNames;

      this.autoCreateQueues = autoCreateQueues;

      this.autoCreateAddresses = autoCreateAddresses;

      this.defaultDeleteOnNoConsumers = defaultDeleteOnNoConsumers;

      this.defaultMaxConsumers = defaultMaxConsumers;
   }

   public boolean isExists() {
      return exists;
   }

   public boolean isAutoCreateQueues() {
      return autoCreateQueues;
   }

   public boolean isAutoCreateAddresses() {
      return autoCreateAddresses;
   }

   public List<SimpleString> getQueueNames() {
      return queueNames;
   }

   public boolean isDefaultDeleteOnNoConsumers() {
      return defaultDeleteOnNoConsumers;
   }

   public int getDefaultMaxConsumers() {
      return defaultMaxConsumers;
   }
}
