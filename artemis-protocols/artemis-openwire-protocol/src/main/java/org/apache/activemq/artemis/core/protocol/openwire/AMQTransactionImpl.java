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
package org.apache.activemq.artemis.core.protocol.openwire;

import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.RefsOperation;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;

import javax.transaction.xa.Xid;

public class AMQTransactionImpl extends TransactionImpl {

   private boolean rollbackForClose = false;

   public AMQTransactionImpl(StorageManager storageManager, int timeoutSeconds) {
      super(storageManager, timeoutSeconds);
   }

   public AMQTransactionImpl(StorageManager storageManager) {
      super(storageManager);
   }

   public AMQTransactionImpl(Xid xid, StorageManager storageManager, int timeoutSeconds) {
      super(xid, storageManager, timeoutSeconds);
   }

   public AMQTransactionImpl(long id, Xid xid, StorageManager storageManager) {
      super(id, xid, storageManager);
   }

   @Override
   public RefsOperation createRefsOperation(Queue queue) {
      return new AMQrefsOperation(queue, storageManager);
   }

   public class AMQrefsOperation extends RefsOperation {

      public AMQrefsOperation(Queue queue, StorageManager storageManager) {
         super(queue, storageManager);
      }

      @Override
      public void afterRollback(Transaction tx) {
         if (rollbackForClose) {
            super.afterRollback(tx);
         }
      }
   }

   public void setRollbackForClose() {
      this.rollbackForClose = true;
   }
}