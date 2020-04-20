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

package org.apache.activemq.artemis.protocol.amqp.broker;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.message.LargeBodyReader;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeBody;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.TypeConstructor;

public class AMQPLargeMessage extends AMQPMessage implements LargeServerMessage {

   @Override
   public ICoreMessage toCore(CoreMessageObjectPools coreMessageObjectPools) {
      LargeBodyReader reader = largeBody.getLargeBodyReader();

      try {
         long size = reader.getSize();
         if (size > Integer.MAX_VALUE) {
            throw new RuntimeException("AMQP Large Message Body is too large to be converted into core");
         }
         byte[] buffer = new byte[(int)size];
         ByteBuffer wrapbuffer = ByteBuffer.wrap(buffer);

         reader.open();
         reader.readInto(wrapbuffer);

         AMQPStandardMessage standardMessage = new AMQPStandardMessage(messageFormat, buffer, extraProperties, coreMessageObjectPools);
         standardMessage.setMessageID(messageID);
         return standardMessage.toCore();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         throw new RuntimeException(e.getMessage(), e);
      } finally {
         try {
            reader.close();
         } catch (Exception e) {
            // unexpected to happen, but possible, nothing else we can do beyond logging at this point
            // if we wanted to add anything it would be a critical failure but it would be a heavy refactoring
            // to bring the bits and listeners here for little benefit
            // the possibility of this happening involves losing the storage device which will lead to other errors anyway
            logger.warn(e.getMessage(), e);
         }
      }
   }

   /**
    * AMQPLargeMessagePersister will save the buffer here.
    * */
   private ByteBuf temporaryBuffer;

   private final LargeBody largeBody;
   /**
    * We control durability on a separate property here, as we need to know if it's durable ahead of the header parsing.
    * This will be the case when restarting a server
    */
   private Boolean fileDurable;

   private volatile AmqpReadableBuffer parsingData;

   private StorageManager storageManager;

   public AMQPLargeMessage(long id,
                           long messageFormat,
                           TypedProperties extraProperties,
                           CoreMessageObjectPools coreMessageObjectPools,
                           StorageManager storageManager) {
      super(messageFormat, extraProperties, coreMessageObjectPools);
      this.setMessageID(id);
      largeBody = new LargeBody(this, storageManager);
      this.storageManager = storageManager;
   }

   /**
    * Copy constructor
    */
   private AMQPLargeMessage(final AMQPLargeMessage copy,
                                  final SequentialFile fileCopy,
                                  final long newID) {
      super(copy);
      largeBody = new LargeBody(this, copy.largeBody.getStorageManager(), fileCopy);
      largeBody.setBodySize(copy.largeBody.getStoredBodySize());
      this.storageManager = copy.largeBody.getStorageManager();
      setMessageID(newID);
   }

   public void openLargeMessage() throws Exception {
      this.parsingData = new AmqpReadableBuffer(largeBody.map());
   }

   public void closeLargeMessage() throws Exception {
      largeBody.releaseResources(false);
      parsingData.freeDirectBuffer();
      parsingData = null;
   }

   public void releaseEncodedBuffer() {
      internalReleaseBuffer(1);
   }

   /** {@link #getSavedEncodeBuffer()} will retain two counters from the buffer, one meant for the call,
    * and one that must be released only after encoding.
    *
    * This method is meant to be called when the buffer is actually encoded on the journal, meaning both refs are gone.
    * and the actual buffer can be released.
    */
   public void releaseEncodedBufferAfterWrite() {
      internalReleaseBuffer(2);
   }

   private void internalReleaseBuffer(int releases) {
      synchronized (largeBody) {
         for (int i = 0; i < releases; i++) {
            if (temporaryBuffer != null && temporaryBuffer.release()) {
               temporaryBuffer = null;
            }
         }
      }
   }

   /** This is used on test assertions to make sure the buffers are released corrected */
   public ByteBuf inspectTemporaryBuffer() {
      return temporaryBuffer;
   }

   public ByteBuf getSavedEncodeBuffer() {
      synchronized (largeBody) {
         if (temporaryBuffer == null) {
            temporaryBuffer = PooledByteBufAllocator.DEFAULT.buffer(getEstimateSavedEncode());
            saveEncoding(temporaryBuffer);
         }
         return temporaryBuffer.retain(1);
      }
   }

   @Override
   public void finishParse() throws Exception {
      openLargeMessage();
      try {
         this.ensureMessageDataScanned();
         parsingData.rewind();
         lazyDecodeApplicationProperties();
      } finally {
         closeLargeMessage();
      }
   }

   @Override
   public void validateFile() throws ActiveMQException {
      largeBody.validateFile();
   }

   public void setFileDurable(boolean value) {
      this.fileDurable = value;
   }

   @Override
   public StorageManager getStorageManager() {
      return largeBody.getStorageManager();
   }

   @Override
   public void setStorageManager(StorageManager storageManager) {
      largeBody.setStorageManager(storageManager);
      this.storageManager = storageManager;
   }

   @Override
   public final boolean isDurable() {
      if (fileDurable != null) {
         return fileDurable.booleanValue();
      } else {
         return super.isDurable();
      }
   }

   @Override
   public ReadableBuffer getData() {
      if (parsingData == null) {
         throw new RuntimeException("AMQP Large Message is not open");
      }

      return parsingData;
   }

   public void parseHeader(ReadableBuffer buffer) {

      DecoderImpl decoder = TLSEncode.getDecoder();
      decoder.setBuffer(buffer);

      try {
         int constructorPos = buffer.position();
         TypeConstructor<?> constructor = decoder.readConstructor();
         if (Header.class.equals(constructor.getTypeClass())) {
            header = (Header) constructor.readValue();
            if (header.getTtl() != null) {
               expiration = System.currentTimeMillis() + header.getTtl().intValue();
            }
         }
      } finally {
         decoder.setBuffer(null);
         buffer.rewind();
      }
   }

   public void addBytes(ReadableBuffer data) throws Exception {

      // We need to parse the header on the first add,
      // as it will contain information if the message is durable or not
      if (header == null && largeBody.getStoredBodySize() <= 0) {
         parseHeader(data);
      }

      if (data.hasArray() && data.remaining() == data.array().length) {
         //System.out.println("Received " + data.array().length + "::" + ByteUtil.formatGroup(ByteUtil.bytesToHex(data.array()), 8, 16));
         largeBody.addBytes(data.array());
      } else {
         byte[] bytes = new byte[data.remaining()];
         data.get(bytes);
         //System.out.println("Finishing " + bytes.length + ByteUtil.formatGroup(ByteUtil.bytesToHex(bytes), 8, 16));
         largeBody.addBytes(bytes);
      }
   }

   @Override
   public ReadableBuffer getSendBuffer(int deliveryCount) {
      return getData().rewind();
   }

   @Override
   public Message toMessage() {
      return this;
   }

   @Override
   public void addBytes(byte[] bytes) throws Exception {
      largeBody.addBytes(bytes);
   }

   @Override
   public void addBytes(ActiveMQBuffer bytes) throws Exception {
      largeBody.addBytes(bytes);

   }

   @Override
   public void setPaged() {
      largeBody.setPaged();
   }

   @Override
   public void releaseResources(boolean sync) {
      largeBody.releaseResources(sync);

   }

   @Override
   public void deleteFile() throws Exception {
      largeBody.deleteFile();
   }

   @Override
   public SequentialFile getAppendFile() throws ActiveMQException {
      return largeBody.getAppendFile();
   }

   @Override
   public boolean isLargeMessage() {
      return true;
   }

   @Override
   public LargeBodyReader getLargeBodyReader() {
      return largeBody.getLargeBodyReader();
   }

   @Override
   public LargeBody getLargeBody() {
      return largeBody;
   }

   @Override
   public void clearPendingRecordID() {
      largeBody.clearPendingRecordID();
   }

   @Override
   public boolean hasPendingRecord() {
      return largeBody.hasPendingRecord();
   }

   @Override
   public void setPendingRecordID(long pendingRecordID) {
      largeBody.setPendingRecordID(pendingRecordID);
   }

   @Override
   public long getPendingRecordID() {
      return largeBody.getPendingRecordID();
   }

   @Override
   protected void releaseComplete() {
      largeBody.deleteFile();
   }

   @Override
   public Message copy() {
      SequentialFile newfile = largeBody.createFile();
      AMQPLargeMessage newMessage = new AMQPLargeMessage(this, newfile, messageID);
      newMessage.setParentRef(this);
      newMessage.setFileDurable(this.isDurable());
      return newMessage;
   }

   @Override
   public Message copy(final long newID) {
      try {
         AMQPLargeMessage copy = new AMQPLargeMessage(newID, messageFormat, null, coreMessageObjectPools, storageManager);
         copy.setDurable(this.isDurable());
         largeBody.copyInto(copy);
         copy.finishParse();
         copy.releaseResources(true);
         return copy;

      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.lareMessageErrorCopying(e, this);
         return null;
      }
   }



   @Override
   public void messageChanged() {

   }

   @Override
   public int getEncodeSize() {
      return 0;
   }

   @Override
   public int getMemoryEstimate() {
      return 0;
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {

   }

   @Override
   public int getPersistSize() {
      return 0;
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record, CoreMessageObjectPools pools) {

   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return 0;
   }

   @Override
   public Persister<Message> getPersister() {
      return AMQPLargeMessagePersister.getInstance();
   }

   @Override
   public void reencode() {

   }

   @Override
   protected void ensureDataIsValid() {

   }

   @Override
   protected void encodeMessage() {

   }

   @Override
   public void referenceOriginalMessage(final Message original, String originalQueue) {

      super.referenceOriginalMessage(original, originalQueue);

      if (original instanceof LargeServerMessageImpl) {
         this.largeBody.referenceOriginalMessage(((AMQPLargeMessage) original).largeBody);
      }
   }
}
