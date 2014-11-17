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
package org.apache.activemq.core.transaction.impl;

import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.impl.RefsOperation;

/**
 * A BindingsTransactionImpl
 *
 * @author clebertsuconic
 */
public class BindingsTransactionImpl extends TransactionImpl
{

   public BindingsTransactionImpl(StorageManager storage)
   {
      super(storage, 0);
   }

   /**
    * @throws Exception
    */
   protected void doCommit() throws Exception
   {
      if (isContainsPersistent())
      {
         storageManager.commitBindings(getID());
         setState(State.COMMITTED);
      }
   }

   protected void doRollback() throws Exception
   {
      if (isContainsPersistent())
      {
         storageManager.rollbackBindings(getID());
         setState(State.ROLLEDBACK);
      }
   }

   @Override
   public RefsOperation createRefsOperation(Queue queue)
   {
      return null;
   }
}
