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

import java.nio.ByteBuffer;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.util.FileIOUtil;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.StorageManager.LargeMessageExtension;
import org.apache.activemq.artemis.core.replication.ReplicatedLargeMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.jboss.logging.Logger;

public final class LargeServerMessageInSync implements ReplicatedLargeMessage {

   private static final Logger logger = Logger.getLogger(LargeServerMessageInSync.class);

   private final LargeServerMessage mainLM;
   private final StorageManager storageManager;
   private SequentialFile appendFile;
   private boolean syncDone;
   private boolean deleted;

   /**
    * @param storageManager
    */
   public LargeServerMessageInSync(StorageManager storageManager) {
      mainLM = storageManager.createLargeMessage();
      this.storageManager = storageManager;
   }

   public synchronized void joinSyncedData(ByteBuffer buffer) throws Exception {
      if (deleted)
         return;
      SequentialFile mainSeqFile = mainLM.getAppendFile();
      if (!mainSeqFile.isOpen()) {
         mainSeqFile.open();
      }

      try {
         if (appendFile != null) {
            if (logger.isTraceEnabled()) {
               logger.trace("joinSyncedData on " + mainLM + ", currentSize on mainMessage=" + mainSeqFile.size() + ", appendFile size = " + appendFile.size());
            }

            FileIOUtil.copyData(appendFile, mainSeqFile, buffer);
            deleteAppendFile();
         } else {
            if (logger.isTraceEnabled()) {
               logger.trace("joinSyncedData, appendFile is null, ignoring joinSyncedData on " + mainLM);
            }
         }
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.errorWhileSyncingData(mainLM.toString(), e);
      }

      if (logger.isTraceEnabled()) {
         logger.trace("joinedSyncData on " + mainLM + " finished with " + mainSeqFile.size());
      }

      syncDone = true;
   }

   public SequentialFile getSyncFile() throws ActiveMQException {
      return mainLM.getAppendFile();
   }

   @Override
   public Message setDurable(boolean durable) {
      mainLM.setDurable(durable);
      return mainLM.toMessage();
   }

   @Override
   public synchronized Message setMessageID(long id) {
      mainLM.setMessageID(id);
      return mainLM.toMessage();
   }

   @Override
   public synchronized void releaseResources(boolean sync, boolean sendEvent) {
      if (logger.isTraceEnabled()) {
         logger.trace("release resources called on " + mainLM, new Exception("trace"));
      }
      mainLM.releaseResources(sync, sendEvent);
      if (appendFile != null && appendFile.isOpen()) {
         try {
            appendFile.close();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.largeMessageErrorReleasingResources(e);
         }
      }
   }

   @Override
   public synchronized void deleteFile() throws Exception {
      deleted = true;
      try {
         mainLM.deleteFile();
      } finally {
         deleteAppendFile();
      }
   }

   /**
    * @throws Exception
    */
   private void deleteAppendFile() throws Exception {
      if (appendFile != null) {
         if (appendFile.isOpen())
            appendFile.close();
         appendFile.delete();
      }
   }

   @Override
   public synchronized void addBytes(byte[] bytes) throws Exception {
      if (deleted)
         return;

      if (syncDone) {
         if (logger.isTraceEnabled()) {
            logger.trace("Adding " + bytes.length + " towards sync message::" + mainLM);
         }
         mainLM.addBytes(bytes);
         return;
      }

      if (logger.isTraceEnabled()) {
         logger.trace("addBytes(bytes.length=" + bytes.length + ") on message=" + mainLM);
      }

      if (appendFile == null) {
         appendFile = storageManager.createFileForLargeMessage(mainLM.getMessageID(), LargeMessageExtension.SYNC);
      }

      if (!appendFile.isOpen()) {
         appendFile.open();
      }
      storageManager.addBytesToLargeMessage(appendFile, mainLM.getMessageID(), bytes);
   }

   @Override
   public void clearPendingRecordID() {
      mainLM.clearPendingRecordID();
   }

   @Override
   public boolean hasPendingRecord() {
      return mainLM.hasPendingRecord();
   }

   @Override
   public void setPendingRecordID(long pendingRecordID) {
      mainLM.setPendingRecordID(pendingRecordID);
   }

   @Override
   public long getPendingRecordID() {
      return mainLM.getPendingRecordID();
   }

}
