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

import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;

public interface Consumer {

   /**
    *
    * @see SessionCallback#supportsDirectDelivery()
    */
   default boolean supportsDirectDelivery() {
      return true;
   }

   /**
    * There was a change on semantic during 2.3 here.<br>
    * We now first accept the message, and the actual deliver is done as part of
    * {@link #proceedDeliver(MessageReference)}. This is to avoid holding a lock on the queues while
    * the delivery is being accomplished To avoid a lock on the queue in case of misbehaving
    * consumers.
    * <p>
    * This should return busy if handle is called before proceed deliver is called
    *
    * @param reference
    * @return
    * @throws Exception
    */
   HandleStatus handle(MessageReference reference) throws Exception;

   /**
    * This will proceed with the actual delivery.
    * Notice that handle should hold a readLock and proceedDelivery should release the readLock
    * any lock operation on Consumer should also get a writeLock on the readWriteLock
    * to guarantee there are no pending deliveries
    *
    * @throws Exception
    */
   void proceedDeliver(MessageReference reference) throws Exception;

   Filter getFilter();

   /**
    * @return the list of messages being delivered
    */
   List<MessageReference> getDeliveringMessages();

   String debug();

   /**
    * This method will create a string representation meant for management operations.
    * This is different from the toString method that's meant for debugging and will contain information that regular users won't understand well
    *
    * @return
    */
   String toManagementString();

   /**
    * disconnect the consumer
    */
   void disconnect();

   /** an unique sequential ID for this consumer */
   long sequentialID();
}
