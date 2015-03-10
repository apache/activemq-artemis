/**
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
package org.apache.activemq.core.paging.cursor;

import org.apache.activemq.core.paging.impl.Page;
import org.apache.activemq.core.transaction.Transaction;

/**
 * A PagingSubscriptionCounterInterface
 */
public interface PageSubscriptionCounter
{

   long getValue();

   void increment(Transaction tx, int add) throws Exception;

   void loadValue(final long recordValueID, final long value);

   void loadInc(final long recordInd, final int add);

   void applyIncrementOnTX(Transaction tx, long recordID, int add);

   /** This will process the reload */
   void processReload();

   /**
    *
    * @param id
    * @param variance
    */
   void addInc(long id, int variance);

   // used when deleting the counter
   void delete() throws Exception;

   void pendingCounter(Page page, int increment) throws Exception;

   // used when leaving page mode, so the counters are deleted in batches
   // for each queue on the address
   void delete(Transaction tx) throws Exception;

   void cleanupNonTXCounters(final long pageID) throws Exception;

}