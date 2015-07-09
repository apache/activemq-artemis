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
package org.apache.activemq.artemis.core.transaction;

import java.util.List;

import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.RefsOperation;

/**
 * An ActiveMQ Artemis internal transaction
 */
public interface Transaction
{
   static enum State
   {
      ACTIVE, PREPARED, COMMITTED, ROLLEDBACK, SUSPENDED, ROLLBACK_ONLY
   }

   void prepare() throws Exception;

   void commit() throws Exception;

   void commit(boolean onePhase) throws Exception;

   void rollback() throws Exception;

   long getID();

   Xid getXid();

   void suspend();

   void resume();

   State getState();

   void setState(State state);

   void markAsRollbackOnly(ActiveMQException exception);

   long getCreateTime();

   void addOperation(TransactionOperation sync);

   List<TransactionOperation> getAllOperations();

   boolean hasTimedOut(long currentTime, int defaultTimeout);

   void putProperty(int index, Object property);

   Object getProperty(int index);

   boolean isContainsPersistent();

   void setContainsPersistent();

   void setTimeout(int timeout);

   RefsOperation createRefsOperation(Queue queue);
}
