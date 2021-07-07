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
package org.apache.activemq.artemis.quorum.zookeeper;

import java.util.function.Consumer;

import org.apache.activemq.artemis.quorum.MutableLong;
import org.apache.activemq.artemis.quorum.UnavailableStateException;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;

final class CuratorMutableLong implements MutableLong {

   private final CuratorDistributedPrimitiveManager manager;
   private final String id;
   private final DistributedAtomicLong atomicLong;
   private final Consumer<CuratorMutableLong> onClose;
   private boolean unavailable;
   private boolean closed;

   CuratorMutableLong(CuratorDistributedPrimitiveManager manager,
                      String id,
                      DistributedAtomicLong atomicLong,
                      Consumer<CuratorMutableLong> onClose) {
      this.manager = manager;
      this.id = id;
      this.atomicLong = atomicLong;
      this.onClose = onClose;
      this.closed = false;
      this.unavailable = false;
   }

   void onReconnected() {
      synchronized (manager) {
         if (closed) {
            return;
         }
         unavailable = false;
      }
   }

   void onLost() {
      synchronized (manager) {
         if (closed || unavailable) {
            return;
         }
         unavailable = true;
      }
   }

   void onSuspended() {
   }

   private void checkNotClosed() {
      if (closed) {
         throw new IllegalStateException("This lock is closed");
      }
   }

   @Override
   public String getMutableLongId() {
      return id;
   }

   @Override
   public long get() throws UnavailableStateException {
      synchronized (manager) {
         manager.checkHandlingEvents();
         checkNotClosed();
         if (unavailable) {
            throw new UnavailableStateException(id + " lock state isn't available");
         }
         try {
            AtomicValue<Long> atomicValue = atomicLong.get();
            if (!atomicValue.succeeded()) {
               throw new UnavailableStateException("cannot query long " + id);
            }
            return atomicValue.postValue();
         } catch (UnavailableStateException rethrow) {
            throw rethrow;
         } catch (Throwable e) {
            throw new UnavailableStateException(e);
         }
      }
   }

   @Override
   public void set(long value) throws UnavailableStateException {
      synchronized (manager) {
         manager.checkHandlingEvents();
         checkNotClosed();
         if (unavailable) {
            throw new UnavailableStateException(id + " lock state isn't available");
         }
         try {
            atomicLong.forceSet(value);
         } catch (UnavailableStateException rethrow) {
            throw rethrow;
         } catch (Throwable e) {
            throw new UnavailableStateException(e);
         }
      }
   }

   public void close(boolean useCallback) {
      synchronized (manager) {
         manager.checkHandlingEvents();
         if (closed) {
            return;
         }
         closed = true;
         if (useCallback) {
            onClose.accept(this);
         }
      }
   }

   @Override
   public void close() {
      close(true);
   }
}
