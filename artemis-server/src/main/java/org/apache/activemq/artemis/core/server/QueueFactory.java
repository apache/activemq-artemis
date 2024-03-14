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

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.postoffice.PostOffice;

/**
 * A QueueFactory
 * <p>
 * Implementations of this class know how to create queues with the correct attribute values
 * based on default and overrides
 */
public interface QueueFactory {

   @Deprecated
   Queue createQueueWith(QueueConfig config) throws Exception;

   Queue createQueueWith(QueueConfiguration config, PagingManager pagingManager, Filter filter) throws Exception;

   /**
    * @deprecated Replaced by {@link #createQueueWith}
    */
   @Deprecated
   Queue createQueue(long persistenceID,
                     SimpleString address,
                     SimpleString name,
                     Filter filter,
                     PageSubscription pageSubscription,
                     SimpleString user,
                     boolean durable,
                     boolean temporary,
                     boolean autoCreated) throws Exception;

   /**
    * This is required for delete-all-reference to work correctly with paging
    *
    * @param postOffice
    */
   void setPostOffice(PostOffice postOffice);

   default void queueRemoved(Queue queue) {

   }
}
