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
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.AutoCreatedQueueManager;
import org.apache.activemq.artemis.core.server.QueueDeleter;
import org.apache.activemq.artemis.utils.ReferenceCounterUtil;

public class AutoCreatedQueueManagerImpl implements AutoCreatedQueueManager {

   private final SimpleString queueName;

   private final QueueDeleter deleter;

   private final Runnable runnable = new Runnable() {
      @Override
      public void run() {
         try {
            if (deleter != null) {
               deleter.delete(queueName);
            }
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorRemovingAutoCreatedQueue(e, queueName);
         }
      }
   };

   private final ReferenceCounterUtil referenceCounterUtil = new ReferenceCounterUtil(runnable);

   public AutoCreatedQueueManagerImpl(QueueDeleter deleter, SimpleString queueName) {
      this.deleter = deleter;
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
