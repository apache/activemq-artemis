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
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueManager;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.ReferenceCounterUtil;

public class QueueManagerImpl extends ReferenceCounterUtil implements QueueManager {

   private final SimpleString queueName;

   private final ActiveMQServer server;

   private void doIt() {
      Queue queue = server.locateQueue(queueName);
      //the queue may already have been deleted and this is a result of that
      if (queue == null) {
         if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
            ActiveMQServerLogger.LOGGER.debug("pno queue to delete \"" + queueName + ".\"");
         }
         return;
      }
      SimpleString address = queue.getAddress();
      AddressSettings settings = server.getAddressSettingsRepository().getMatch(address.toString());
      long consumerCount = queue.getConsumerCount();
      long messageCount = queue.getMessageCount();

      if (queue.isAutoCreated() && settings.isAutoDeleteQueues() && queue.getMessageCount() == 0 && queue.getConsumerCount() == 0) {
         if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
            ActiveMQServerLogger.LOGGER.debug("deleting " + (queue.isAutoCreated() ? "auto-created " : "") + "queue \"" + queueName + ".\" consumerCount = " + consumerCount + "; messageCount = " + messageCount + "; isAutoDeleteQueues = " + settings.isAutoDeleteQueues());
         }

         try {
            server.destroyQueue(queueName, null, true, false);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorRemovingAutoCreatedQueue(e, queueName);
         }
      } else if (queue.isPurgeOnNoConsumers()) {
         if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
            ActiveMQServerLogger.LOGGER.debug("purging queue \"" + queueName + ".\" consumerCount = " + consumerCount + "; messageCount = " + messageCount);
         }
         try {
            queue.deleteMatchingReferences(QueueImpl.DEFAULT_FLUSH_LIMIT, null, AckReason.KILLED);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.failedToPurgeQueue(e, queueName);
         }
      }
   }

   public QueueManagerImpl(ActiveMQServer server, SimpleString queueName) {
      this.server = server;
      this.queueName = queueName;
      this.setTask(this::doIt);
   }

   @Override
   public SimpleString getQueueName() {
      return queueName;
   }
}
