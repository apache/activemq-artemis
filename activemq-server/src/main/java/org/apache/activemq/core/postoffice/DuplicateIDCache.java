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
package org.apache.activemq.core.postoffice;

import java.util.List;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.core.transaction.Transaction;

/**
 * A DuplicateIDCache
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 8 Dec 2008 16:36:07
 *
 *
 */
public interface DuplicateIDCache
{
   boolean contains(byte[] duplicateID);

   void addToCache(byte[] duplicateID, Transaction tx) throws Exception;

   void deleteFromCache(byte [] duplicateID) throws Exception;

   void load(List<Pair<byte[], Long>> theIds) throws Exception;

   void load(final Transaction tx, final byte[] duplID);

   void clear() throws Exception;

   List<Pair<byte[], Long>> getMap();
}
