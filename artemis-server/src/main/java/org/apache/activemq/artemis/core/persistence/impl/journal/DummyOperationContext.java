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
package org.apache.activemq.artemis.core.persistence.impl.journal;

import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.OperationContext;

final class DummyOperationContext implements OperationContext {

   private static DummyOperationContext instance = new DummyOperationContext();

   public static OperationContext getInstance() {
      return DummyOperationContext.instance;
   }

   public void executeOnCompletion(final IOCallback runnable) {
      // There are no executeOnCompletion calls while using the DummyOperationContext
      // However we keep the code here for correctness
      runnable.done();
   }

   public void replicationDone() {
   }

   public void replicationLineUp() {
   }

   public void storeLineUp() {
   }

   public void done() {
   }

   public void onError(final int errorCode, final String errorMessage) {
   }

   public void waitCompletion() {
   }

   public boolean waitCompletion(final long timeout) {
      return true;
   }

   public void pageSyncLineUp() {
   }

   public void pageSyncDone() {
   }
}
