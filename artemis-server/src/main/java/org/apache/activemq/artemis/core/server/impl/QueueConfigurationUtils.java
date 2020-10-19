/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

public class QueueConfigurationUtils {

   public static void applyDynamicQueueDefaults(QueueConfiguration config, AddressSettings as) {
      config.setMaxConsumers(config.getMaxConsumers() == null ? as.getDefaultMaxConsumers() : config.getMaxConsumers());
      config.setExclusive(config.isExclusive() == null ? as.isDefaultExclusiveQueue() : config.isExclusive());
      config.setGroupRebalance(config.isGroupRebalance() == null ? as.isDefaultGroupRebalance() : config.isGroupRebalance());
      config.setGroupRebalancePauseDispatch(config.isGroupRebalancePauseDispatch() == null ? as.isDefaultGroupRebalancePauseDispatch() : config.isGroupRebalancePauseDispatch());
      config.setGroupBuckets(config.getGroupBuckets() == null ? as.getDefaultGroupBuckets() : config.getGroupBuckets());
      config.setGroupFirstKey(config.getGroupFirstKey() == null ? as.getDefaultGroupFirstKey() : config.getGroupFirstKey());
      config.setLastValue(config.isLastValue() == null ? as.isDefaultLastValueQueue() : config.isLastValue());
      config.setLastValueKey(config.getLastValueKey() == null ? as.getDefaultLastValueKey() : config.getLastValueKey());
      config.setNonDestructive(config.isNonDestructive() == null ? as.isDefaultNonDestructive() : config.isNonDestructive());
      config.setConsumersBeforeDispatch(config.getConsumersBeforeDispatch() == null ? as.getDefaultConsumersBeforeDispatch() : config.getConsumersBeforeDispatch());
      config.setDelayBeforeDispatch(config.getDelayBeforeDispatch() == null ? as.getDefaultDelayBeforeDispatch() : config.getDelayBeforeDispatch());
      config.setRingSize(config.getRingSize() == null ? as.getDefaultRingSize() : config.getRingSize());
      config.setRoutingType(config.getRoutingType() == null ? as.getDefaultQueueRoutingType() : config.getRoutingType());
      config.setPurgeOnNoConsumers(config.isPurgeOnNoConsumers() == null ? as.isDefaultPurgeOnNoConsumers() : config.isPurgeOnNoConsumers());
      config.setAutoCreateAddress(config.isAutoCreateAddress() == null ? as.isAutoCreateAddresses() : config.isAutoCreateAddress());
      // set the default auto-delete
      config.setAutoDelete(config.isAutoDelete() == null ? !config.isConfigurationManaged() && ((config.isAutoCreated() && as.isAutoDeleteQueues()) || (!config.isAutoCreated() && as.isAutoDeleteCreatedQueues())) : config.isAutoDelete());

      config.setAutoDeleteDelay(config.getAutoDeleteDelay() == null ? as.getAutoDeleteQueuesDelay() : config.getAutoDeleteDelay());
      config.setAutoDeleteMessageCount(config.getAutoDeleteMessageCount() == null ? as.getAutoDeleteQueuesMessageCount() : config.getAutoDeleteMessageCount());

      config.setEnabled(config.isEnabled() == null ? ActiveMQDefaultConfiguration.getDefaultEnabled() : config.isEnabled());
   }
}
