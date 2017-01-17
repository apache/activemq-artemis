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
package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.AutoCreatedQueueManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.ReferenceCounterUtil;

public class AutoCreatedQueueManagerImpl implements AutoCreatedQueueManager {

   private final SimpleString queueName;

   private final ActiveMQServer server;

   private final Runnable runnable = new Runnable() {
      @Override
      public void run() {
         Queue queue = server.locateQueue(queueName);
         SimpleString address = queue.getAddress();
         AddressSettings settings = server.getAddressSettingsRepository().getMatch(address.toString());
         long consumerCount = queue.getConsumerCount();
         long messageCount = queue.getMessageCount();

         if (((queue.isAutoCreated() && settings.isAutoDeleteQueues()) || queue.isPurgeOnNoConsumers()) && queue.getMessageCount() == 0) {
            if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
               ActiveMQServerLogger.LOGGER.debug("deleting " + (queue.isAutoCreated() ? "auto-created " : "") + "queue \"" + queueName + ".\" consumerCount = " + consumerCount + "; messageCount = " + messageCount + "; isAutoDeleteQueues = " + settings.isAutoDeleteQueues());
            }

            try {
               server.destroyQueue(queueName, null, true, false);
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorRemovingAutoCreatedQueue(e, queueName);
            }
         }
      }
   };

   private final ReferenceCounterUtil referenceCounterUtil = new ReferenceCounterUtil(runnable);

   public AutoCreatedQueueManagerImpl(ActiveMQServer server, SimpleString queueName) {
      this.server = server;
      this.queueName = queueName;
   }

   @Override
   public int increment() {
      return referenceCounterUtil.increment();
   }

   @Override
   public int decrement() {
      return referenceCounterUtil.decrement();
   }

   @Override
   public SimpleString getQueueName() {
      return queueName;
   }
}
