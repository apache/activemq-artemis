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
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.StorageManager.LargeMessageExtension;
import org.apache.activemq.artemis.core.replication.ReplicatedLargeMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;

public final class LargeServerMessageInSync implements ReplicatedLargeMessage {

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
      SequentialFile mainSeqFile = mainLM.getFile();
      if (!mainSeqFile.isOpen()) {
         mainSeqFile.open();
      }
      if (appendFile != null) {
         appendFile.close();
         appendFile.open();
         for (;;) {
            buffer.rewind();
            int bytesRead = appendFile.read(buffer);
            if (bytesRead > 0)
               mainSeqFile.writeDirect(buffer, false);
            if (bytesRead < buffer.capacity()) {
               break;
            }
         }
         deleteAppendFile();
      }
      syncDone = true;
   }

   public SequentialFile getSyncFile() throws ActiveMQException {
      return mainLM.getFile();
   }

   @Override
   public Message setDurable(boolean durable) {
      mainLM.setDurable(durable);
      return mainLM;
   }

   @Override
   public synchronized Message setMessageID(long id) {
      mainLM.setMessageID(id);
      return mainLM;
   }

   @Override
   public synchronized void releaseResources() {
      mainLM.releaseResources();
      if (appendFile != null && appendFile.isOpen()) {
         try {
            appendFile.close();
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.largeMessageErrorReleasingResources(e);
         }
      }
   }

   @Override
   public synchronized void deleteFile() throws Exception {
      deleted = true;
      try {
         mainLM.deleteFile();
      }
      finally {
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
         mainLM.addBytes(bytes);
         return;
      }

      if (appendFile == null) {
         appendFile = storageManager.createFileForLargeMessage(mainLM.getMessageID(), LargeMessageExtension.SYNC);
      }

      if (!appendFile.isOpen()) {
         appendFile.open();
      }
      storageManager.addBytesToLargeMessage(appendFile, mainLM.getMessageID(), bytes);
   }

}
