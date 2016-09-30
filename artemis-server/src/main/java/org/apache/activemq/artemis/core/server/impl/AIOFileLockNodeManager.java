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
package org.apache.activemq.artemis.core.server.impl;

import java.io.File;
import java.nio.channels.FileLock;

import org.apache.activemq.artemis.core.io.aio.ActiveMQFileLock;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jlibaio.LibaioFile;

/**
 * This is using the ActiveMQ Artemis Libaio Native to perform calls to flock on a Linux system. At the
 * current version of RHEL there's a bug on GFS2 and because of that fctl is not functional what
 * will cause issues on Failover over Shared Storage.
 * <p>
 * This will provide an alternative to perform locks through our native module until fctl is fixed
 * on Linux.
 * <p>
 * https://bugzilla.redhat.com/show_bug.cgi?id=678585
 */
public final class AIOFileLockNodeManager extends FileLockNodeManager {

   /**
    * @param directory
    * @param replicatingBackup
    */
   public AIOFileLockNodeManager(final File directory, boolean replicatingBackup) {
      super(directory, replicatingBackup);
   }

   public AIOFileLockNodeManager(final File directory, boolean replicatingBackup, long lockAcquisitionTimeout) {
      super(directory, replicatingBackup);

      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
   }

   @Override
   protected FileLock tryLock(final int lockPos) throws Exception {
      File file = newFileForRegionLock(lockPos);

      LibaioFile fileControl = LibaioContext.openControlFile(file.getAbsolutePath(), false);

      if (!fileControl.lock()) {
         fileControl.close();
         return null;
      }

      FileLock lock = new ActiveMQFileLock(fileControl);

      return lock;

   }

   @Override
   protected FileLock lock(final int liveLockPos) throws Exception {
      long start = System.currentTimeMillis();

      File file = newFileForRegionLock(liveLockPos);

      while (!interrupted) {
         FileLock lockFile = tryLock(liveLockPos);
         if (lockFile != null) {
            return lockFile;
         } else {
            try {
               Thread.sleep(500);
            } catch (InterruptedException e) {
               return null;
            }

            if (lockAcquisitionTimeout != -1 && (System.currentTimeMillis() - start) > lockAcquisitionTimeout) {
               throw new Exception("timed out waiting for lock");
            }
         }
      }

      return null;
   }

   /**
    * @param liveLockPos
    * @return
    */
   protected File newFileForRegionLock(final int liveLockPos) {
      File file = newFile("server." + liveLockPos + ".lock");
      return file;
   }
}
