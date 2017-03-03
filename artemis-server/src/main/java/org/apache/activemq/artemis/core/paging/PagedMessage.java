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
package org.apache.activemq.artemis.core.paging;


import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.persistence.StorageManager;

/**
 * A Paged message.
 * <p>
 * We can't just record the ServerMessage as we need other information (such as the TransactionID
 * used during paging)
 */
public interface PagedMessage extends EncodingSupport {

   Message getMessage();

   /**
    * The queues that were routed during paging
    */
   long[] getQueueIDs();

   void initMessage(StorageManager storageManager);

   long getTransactionID();
}
