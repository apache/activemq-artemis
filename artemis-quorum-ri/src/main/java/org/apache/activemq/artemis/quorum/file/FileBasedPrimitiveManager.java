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
package org.apache.activemq.artemis.quorum.file;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;

/**
 * This is an implementation suitable to be used just on unit tests and it won't attempt
 * to manage nor purge existing stale locks files. It's part of the tests life-cycle to properly
 * set-up and tear-down the environment.
 */
public class FileBasedPrimitiveManager implements DistributedPrimitiveManager {

   private final File locksFolder;
   private final Map<String, FileDistributedLock> locks;
   private boolean started;

   public FileBasedPrimitiveManager(Map<String, String> args) {
      this(new File(args.get("locks-folder")));
   }

   public FileBasedPrimitiveManager(File locksFolder) {
      Objects.requireNonNull(locksFolder);
      if (!locksFolder.exists()) {
         throw new IllegalStateException(locksFolder + " is supposed to already exists");
      }
      if (!locksFolder.isDirectory()) {
         throw new IllegalStateException(locksFolder + " is supposed to be a directory");
      }
      this.locksFolder = locksFolder;
      this.locks = new HashMap<>();
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public void addUnavailableManagerListener(UnavailableManagerListener listener) {
      // noop
   }

   @Override
   public void removeUnavailableManagerListener(UnavailableManagerListener listener) {
      // noop
   }

   @Override
   public boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
      if (timeout >= 0) {
         Objects.requireNonNull(unit);
      }
      if (started) {
         return true;
      }
      started = true;
      return true;
   }

   @Override
   public void start() throws InterruptedException, ExecutionException {
      start(-1, null);
   }

   @Override
   public void stop() {
      if (!started) {
         return;
      }
      try {
         locks.forEach((lockId, lock) -> {
            try {
               lock.close(false);
            } catch (Throwable t) {
               // TODO no op for now: log would be better!
            }
         });
         locks.clear();
      } finally {
         started = false;
      }
   }

   @Override
   public DistributedLock getDistributedLock(String lockId) throws ExecutionException {
      Objects.requireNonNull(lockId);
      if (!started) {
         throw new IllegalStateException("manager should be started first");
      }
      final FileDistributedLock lock = locks.get(lockId);
      if (lock != null && !lock.isClosed()) {
         return lock;
      }
      try {
         final FileDistributedLock newLock = new FileDistributedLock(locks::remove, locksFolder, lockId);
         locks.put(lockId, newLock);
         return newLock;
      } catch (IOException ioEx) {
         throw new ExecutionException(ioEx);
      }
   }
}
