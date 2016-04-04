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

import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 * A ServerConsumer
 */
public interface ServerConsumer extends Consumer {

   void setlowConsumerDetection(SlowConsumerDetectionListener listener);

   SlowConsumerDetectionListener getSlowConsumerDetecion();

   void fireSlowConsumer();

   /**
    * @param protocolContext
    * @see #getProtocolContext()
    */
   void setProtocolContext(Object protocolContext);

   /**
    * An object set by the Protocol implementation.
    * it could be anything pre-determined by the implementation
    */
   Object getProtocolContext();

   long getID();

   Object getConnectionID();

   void close(boolean failed) throws Exception;

   /**
    * This method is just to remove itself from Queues.
    * If for any reason during a close an exception occurred, the exception treatment
    * will call removeItself what should take the consumer out of any queues.
    *
    * @throws Exception
    */
   void removeItself() throws Exception;

   List<MessageReference> cancelRefs(boolean failed, boolean lastConsumedAsDelivered, Transaction tx) throws Exception;

   void setStarted(boolean started);

   void receiveCredits(int credits);

   Queue getQueue();

   MessageReference removeReferenceByID(long messageID) throws Exception;

   void acknowledge(Transaction tx, long messageID) throws Exception;

   void individualAcknowledge(Transaction tx, long messageID) throws Exception;

   void individualCancel(final long messageID, boolean failed) throws Exception;

   void forceDelivery(long sequence);

   void setTransferring(boolean transferring);

   boolean isBrowseOnly();

   long getCreationTime();

   String getSessionID();

   void promptDelivery();
}


