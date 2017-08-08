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
package org.apache.activemq.artemis.tests.unit.core.server.impl.fakes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueConfig;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

public final class FakeQueueFactory implements QueueFactory {

   private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(ActiveMQThreadFactory.defaultThreadFactory());

   private final ExecutorService executor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());

   private PostOffice postOffice;

   @Override
   public Queue createQueueWith(final QueueConfig config) {
      return new QueueImpl(config.id(), config.address(), config.name(), config.filter(), config.pageSubscription(),
                           config.user(), config.isDurable(), config.isTemporary(), config.isAutoCreated(),
                           scheduledExecutor, postOffice, null, null, ArtemisExecutor.delegate(executor), null, this);
   }

   @Deprecated
   @Override
   public Queue createQueue(final long persistenceID,
                            final SimpleString address,
                            final SimpleString name,
                            final Filter filter,
                            final PageSubscription subscription,
                            final SimpleString user,
                            final boolean durable,
                            final boolean temporary,
                            final boolean autoCreated) {
      return new QueueImpl(persistenceID, address, name, filter, subscription, user, durable, temporary, autoCreated,
                           scheduledExecutor, postOffice, null, null, ArtemisExecutor.delegate(executor), null, this);
   }

   @Override
   public void setPostOffice(final PostOffice postOffice) {
      this.postOffice = postOffice;

   }

   public void stop() throws Exception {
      scheduledExecutor.shutdown();

      executor.shutdown();
   }

}
