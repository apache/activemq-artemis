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

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.MessageImpl;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.MemorySize;
import org.apache.activemq.artemis.utils.TypedProperties;

public class ServerMessageImpl extends MessageImpl implements ServerMessage {

   private final AtomicInteger durableRefCount = new AtomicInteger();

   private final AtomicInteger refCount = new AtomicInteger();

   private PagingStore pagingStore;

   private static final int memoryOffset;

   private boolean persisted = false;

   static {
      // This is an estimate of how much memory a ServerMessageImpl takes up, exclusing body and properties
      // Note, it is only an estimate, it's not possible to be entirely sure with Java
      // This figure is calculated using the test utilities in org.apache.activemq.tests.unit.util.sizeof
      // The value is somewhat higher on 64 bit architectures, probably due to different alignment

      if (MemorySize.is64bitArch()) {
         memoryOffset = 352;
      } else {
         memoryOffset = 232;
      }
   }

   /*
    * Constructor for when reading from network
    */
   public ServerMessageImpl() {
   }

   /*
    * Construct a MessageImpl from storage, or notification, or before routing
    */
   public ServerMessageImpl(final long messageID, final int initialMessageBufferSize) {
      super(initialMessageBufferSize);

      this.messageID = messageID;
   }

   /*
    * Copy constructor
    */
   protected ServerMessageImpl(final ServerMessageImpl other) {
      super(other);
   }

   /*
    * Copy constructor
    */
   protected ServerMessageImpl(final ServerMessageImpl other, TypedProperties properties) {
      super(other, properties);
   }

   @Override
   public boolean isServerMessage() {
      return true;
   }

   @Override
   public ServerMessageImpl setMessageID(final long id) {
      messageID = id;
      return this;
   }

   @Override
   public MessageReference createReference(final Queue queue) {
      MessageReference ref = new MessageReferenceImpl(this, queue);

      return ref;
   }

   @Override
   public boolean hasInternalProperties() {
      return properties.hasInternalProperties();
   }

   @Override
   public int incrementRefCount() throws Exception {
      int count = refCount.incrementAndGet();

      if (pagingStore != null) {
         if (count == 1) {
            pagingStore.addSize(getMemoryEstimate() + MessageReferenceImpl.getMemoryEstimate());
         } else {
            pagingStore.addSize(MessageReferenceImpl.getMemoryEstimate());
         }
      }

      return count;
   }

   @Override
   public int decrementRefCount() throws Exception {
      int count = refCount.decrementAndGet();

      if (count < 0) {
         // this could happen on paged messages since they are not routed and incrementRefCount is never called
         return count;
      }

      if (pagingStore != null) {
         if (count == 0) {
            pagingStore.addSize(-getMemoryEstimate() - MessageReferenceImpl.getMemoryEstimate());

            if (buffer != null) {
               // release the buffer now
               buffer.byteBuf().release();
            }
         } else {
            pagingStore.addSize(-MessageReferenceImpl.getMemoryEstimate());
         }
      }

      return count;
   }

   @Override
   public int incrementDurableRefCount() {
      return durableRefCount.incrementAndGet();
   }

   @Override
   public int decrementDurableRefCount() {
      return durableRefCount.decrementAndGet();
   }

   @Override
   public int getRefCount() {
      return refCount.get();
   }

   @Override
   public boolean isLargeMessage() {
      return false;
   }

   private volatile int memoryEstimate = -1;

   @Override
   public int getMemoryEstimate() {
      if (memoryEstimate == -1) {
         memoryEstimate = ServerMessageImpl.memoryOffset + buffer.capacity() + properties.getMemoryOffset();
      }

      return memoryEstimate;
   }

   @Override
   public ServerMessage copy(final long newID) {
      ServerMessage m = new ServerMessageImpl(this);

      m.setMessageID(newID);

      return m;
   }

   @Override
   public ServerMessage copy() {
      // This is a simple copy, used only to avoid changing original properties
      return new ServerMessageImpl(this);
   }

   public ServerMessage makeCopyForExpiryOrDLA(final long newID,
                                               MessageReference originalReference,
                                               final boolean expiry) throws Exception {
      return makeCopyForExpiryOrDLA(newID, originalReference, expiry, true);
   }

   @Override
   public ServerMessage makeCopyForExpiryOrDLA(final long newID,
                                               MessageReference originalReference,
                                               final boolean expiry,
                                               final boolean copyOriginalHeaders) throws Exception {
      /*
       We copy the message and send that to the dla/expiry queue - this is
       because otherwise we may end up with a ref with the same message id in the
       queue more than once which would barf - this might happen if the same message had been
       expire from multiple subscriptions of a topic for example
       We set headers that hold the original message address, expiry time
       and original message id
      */

      ServerMessage copy = copy(newID);

      if (copyOriginalHeaders) {
         copy.setOriginalHeaders(this, originalReference, expiry);
      }

      return copy;
   }

   @Override
   public void setOriginalHeaders(final ServerMessage other,
                                  final MessageReference originalReference,
                                  final boolean expiry) {
      SimpleString originalQueue = other.getSimpleStringProperty(Message.HDR_ORIGINAL_QUEUE);

      if (originalQueue != null) {
         putStringProperty(Message.HDR_ORIGINAL_QUEUE, originalQueue);
      } else if (originalReference != null) {
         putStringProperty(Message.HDR_ORIGINAL_QUEUE, originalReference.getQueue().getName());
      }

      if (other.containsProperty(Message.HDR_ORIG_MESSAGE_ID)) {
         putStringProperty(Message.HDR_ORIGINAL_ADDRESS, other.getSimpleStringProperty(Message.HDR_ORIGINAL_ADDRESS));

         putLongProperty(Message.HDR_ORIG_MESSAGE_ID, other.getLongProperty(Message.HDR_ORIG_MESSAGE_ID));
      } else {
         putStringProperty(Message.HDR_ORIGINAL_ADDRESS, other.getAddress());

         putLongProperty(Message.HDR_ORIG_MESSAGE_ID, other.getMessageID());
      }

      // reset expiry
      setExpiration(0);

      if (expiry) {
         long actualExpiryTime = System.currentTimeMillis();

         putLongProperty(Message.HDR_ACTUAL_EXPIRY_TIME, actualExpiryTime);
      }

      bufferValid = false;
   }

   @Override
   public void setPagingStore(final PagingStore pagingStore) {
      this.pagingStore = pagingStore;

      // On the server side, we reset the address to point to the instance of address in the paging store
      // Otherwise each message would have its own copy of the address String which would take up more memory
      address = pagingStore.getAddress();
   }

   @Override
   public synchronized void forceAddress(final SimpleString address) {
      this.address = address;
      bufferValid = false;
   }

   @Override
   public PagingStore getPagingStore() {
      return pagingStore;
   }

   @Override
   public boolean storeIsPaging() {
      if (pagingStore != null) {
         return pagingStore.isPaging();
      } else {
         return false;
      }
   }

   @Override
   public String toString() {
      try {
         return "ServerMessage[messageID=" + messageID + ",durable=" + isDurable() + ",userID=" + getUserID() + ",priority=" + this.getPriority() + ", bodySize=" + this.getBodyBufferDuplicate().capacity() +
            ", timestamp=" + toDate(getTimestamp()) + ",expiration=" + toDate(getExpiration()) +
            ", durable=" + durable + ", address=" + getAddress() + ",properties=" + properties.toString() + "]@" + System.identityHashCode(this);
      } catch (Throwable e) {
         return "ServerMessage[messageID=" + messageID + "]";
      }
   }

   private static String toDate(long timestamp) {
      if (timestamp == 0) {
         return "0";
      } else {
         return new java.util.Date(timestamp).toString();
      }

   }

   @Override
   public InputStream getBodyInputStream() {
      return null;
   }

   // Encoding stuff

   @Override
   public void encodeMessageIDToBuffer() {
      // We first set the message id - this needs to be set on the buffer since this buffer will be re-used

      buffer.setLong(buffer.getInt(MessageImpl.BUFFER_HEADER_SPACE) + DataConstants.SIZE_INT, messageID);
   }

   @Override
   public byte[] getDuplicateIDBytes() {
      Object duplicateID = getDuplicateProperty();

      if (duplicateID == null) {
         return null;
      } else {
         if (duplicateID instanceof SimpleString) {
            return ((SimpleString) duplicateID).getData();
         } else {
            return (byte[]) duplicateID;
         }
      }
   }

   @Override
   public Object getDuplicateProperty() {
      return getObjectProperty(Message.HDR_DUPLICATE_DETECTION_ID);
   }
}
