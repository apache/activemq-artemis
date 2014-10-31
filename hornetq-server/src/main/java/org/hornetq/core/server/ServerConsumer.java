/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.server;

import java.util.List;

import org.hornetq.core.transaction.Transaction;

/**
 *
 * A ServerConsumer
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ServerConsumer extends Consumer
{
   long getID();

   Object getConnectionID();

   void close(boolean failed) throws Exception;

   List<MessageReference> cancelRefs(boolean failed, boolean lastConsumedAsDelivered, Transaction tx) throws Exception;

   void setStarted(boolean started);

   void receiveCredits(int credits) throws Exception;

   Queue getQueue();

   MessageReference removeReferenceByID(long messageID) throws Exception;

   void acknowledge(boolean autoCommitAcks, Transaction tx, long messageID) throws Exception;

   void individualAcknowledge(boolean autoCommitAcks, Transaction tx, long messageID) throws Exception;

   void individualCancel(final long messageID, boolean failed) throws Exception;

   void forceDelivery(long sequence);

   void setTransferring(boolean transferring);

   boolean isBrowseOnly();

   long getCreationTime();

   String getSessionID();
}


