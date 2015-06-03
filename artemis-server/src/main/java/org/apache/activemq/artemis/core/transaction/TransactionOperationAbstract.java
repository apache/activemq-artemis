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

import java.util.Collections;
import java.util.List;

import org.apache.activemq.artemis.core.server.MessageReference;

/**
 * Just a helper, when you don't want to implement all the methods on a transaction operation.
 */
public abstract class TransactionOperationAbstract implements TransactionOperation
{
   public void beforePrepare(Transaction tx) throws Exception
   {

   }

   /**
    * After prepare shouldn't throw any exception.
    * <p>
    * Any verification has to be done on before prepare
    */
   public void afterPrepare(Transaction tx)
   {

   }

   public void beforeCommit(Transaction tx) throws Exception
   {
   }

   /**
    * After commit shouldn't throw any exception.
    * <p>
    * Any verification has to be done on before commit
    */
   public void afterCommit(Transaction tx)
   {
   }

   public void beforeRollback(Transaction tx) throws Exception
   {
   }

   /**
    * After rollback shouldn't throw any exception.
    * <p>
    * Any verification has to be done on before rollback
    */
   public void afterRollback(Transaction tx)
   {
   }

   @Override
   public List<MessageReference> getRelatedMessageReferences()
   {
      return Collections.emptyList();
   }

   /* (non-Javadoc)
    * @see org.apache.activemq.artemis.core.transaction.TransactionOperation#getListOnConsumer(long)
    */
   @Override
   public List<MessageReference> getListOnConsumer(long consumerID)
   {
      return Collections.emptyList();
   }


}
