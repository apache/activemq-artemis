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
package org.apache.activemq.artemis.core.postoffice;

import java.util.List;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.transaction.Transaction;

public interface DuplicateIDCache {

   boolean contains(byte[] duplicateID);

   boolean atomicVerify(byte[] duplID, Transaction tx) throws Exception;

   void addToCache(byte[] duplicateID) throws Exception;

   void addToCache(byte[] duplicateID, Transaction tx) throws Exception;

   /**
    * it will add the data to the cache.
    * If TX == null it won't use a transaction.
    * if instantAdd=true, it won't wait a transaction to add on the cache which is needed on the case of the Bridges
    */
   void addToCache(byte[] duplicateID, Transaction tx, boolean instantAdd) throws Exception;

   void deleteFromCache(byte[] duplicateID) throws Exception;

   void load(List<Pair<byte[], Long>> theIds) throws Exception;

   void load(Transaction tx, byte[] duplID);

   void clear() throws Exception;

   List<Pair<byte[], Long>> getMap();
}
