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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.RefCountMessage;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageIdHelper;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport;
import org.apache.activemq.artemis.protocol.amqp.converter.AmqpCoreConverter;
import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.DroppingWritableBuffer;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.TypeConstructor;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

// see https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format
public class AMQPMessage extends RefCountMessage {

   public static final SimpleString ADDRESS_PROPERTY = SimpleString.toSimpleString("_AMQ_AD");

   public static final int DEFAULT_MESSAGE_FORMAT = 0;
   public static final int DEFAULT_MESSAGE_PRIORITY = 4;
   public static final int MAX_MESSAGE_PRIORITY = 9;

   private static final int VALUE_NOT_PRESENT = -1;

   // Buffer and state for the data backing this message.
   private ReadableBuffer data;
   private boolean messageDataScanned;

   // Marks the message as needed to be re-encoded to update the backing buffer
   private boolean modified;

   // Track locations of the message sections for later use and track the size
   // of the header and delivery annotations if present so we can easily exclude
   // the delivery annotations later and perform efficient encodes or copies.
   private int headerPosition = VALUE_NOT_PRESENT;
   private int encodedHeaderSize;
   private int deliveryAnnotationsPosition = VALUE_NOT_PRESENT;
   private int encodedDeliveryAnnotationsSize;
   private int messageAnnotationsPosition = VALUE_NOT_PRESENT;
   private int propertiesPosition = VALUE_NOT_PRESENT;
   private int applicationPropertiesPosition = VALUE_NOT_PRESENT;
   private int remainingBodyPosition = VALUE_NOT_PRESENT;

   // Message level meta data
   private final long messageFormat;
   private long messageID;
   private SimpleString address;
   private volatile int memoryEstimate = -1;
   private long expiration;
   private long scheduledTime = -1;

   // The Proton based AMQP message section that are retained in memory, these are the
   // mutable portions of the Message as the broker sees it, although AMQP defines that
   // the Properties and ApplicationProperties are immutable so care should be taken
   // here when making changes to those Sections.
   private Header header;
   private MessageAnnotations messageAnnotations;
   private Properties properties;
   private ApplicationProperties applicationProperties;

   private String connectionID;
   private final CoreMessageObjectPools coreMessageObjectPools;
   private Set<Object> rejectedConsumers;
   private DeliveryAnnotations deliveryAnnotationsForSendBuffer;

   // These are properties set at the broker level and carried only internally by broker storage.
   private volatile TypedProperties extraProperties;

   /**
    * Creates a new {@link AMQPMessage} instance from binary encoded message data.
    *
    * @param messageFormat
    *       The Message format tag given the in Transfer that carried this message
    * @param data
    *       The encoded AMQP message
    * @param extraProperties
    *       Broker specific extra properties that should be carried with this message
    */
   public AMQPMessage(long messageFormat, byte[] data, TypedProperties extraProperties) {
      this(messageFormat, data, extraProperties, null);
   }

   /**
    * Creates a new {@link AMQPMessage} instance from binary encoded message data.
    *
    * @param messageFormat
    *       The Message format tag given the in Transfer that carried this message
    * @param data
    *       The encoded AMQP message
    * @param extraProperties
    *       Broker specific extra properties that should be carried with this message
    * @param coreMessageObjectPools
    *       Object pool used to accelerate some String operations.
    */
   public AMQPMessage(long messageFormat, byte[] data, TypedProperties extraProperties, CoreMessageObjectPools coreMessageObjectPools) {
      this(messageFormat, ReadableBuffer.ByteBufferReader.wrap(data), extraProperties, coreMessageObjectPools);
   }

   /**
    * Creates a new {@link AMQPMessage} instance from binary encoded message data.
    *
    * @param messageFormat
    *       The Message format tag given the in Transfer that carried this message
    * @param data
    *       The encoded AMQP message in an {@link ReadableBuffer} wrapper.
    * @param extraProperties
    *       Broker specific extra properties that should be carried with this message
    * @param coreMessageObjectPools
    *       Object pool used to accelerate some String operations.
    */
   public AMQPMessage(long messageFormat, ReadableBuffer data, TypedProperties extraProperties, CoreMessageObjectPools coreMessageObjectPools) {
      this.data = data;
      this.messageFormat = messageFormat;
      this.coreMessageObjectPools = coreMessageObjectPools;
      this.extraProperties = extraProperties == null ? null : new TypedProperties(extraProperties);
      ensureMessageDataScanned();
   }

   /**
    * Internal constructor used for persistence reload of the message.
    * <p>
    * The message will not be usable until the persistence mechanism populates the message
    * data and triggers a parse of the message contents to fill in the message state.
    *
    * @param messageFormat
    *       The Message format tag given the in Transfer that carried this message
    */
   AMQPMessage(long messageFormat) {
      this.messageFormat = messageFormat;
      this.modified = true;  // No buffer yet so this indicates invalid state.
      this.coreMessageObjectPools = null;
   }

   // Access to the AMQP message data using safe copies freshly decoded from the current
   // AMQP message data stored in this message wrapper.  Changes to these values cannot
   // be used to influence the underlying AMQP message data, the standard AMQPMessage API
   // must be used to make changes to mutable portions of the message.

   /**
    * Creates and returns a Proton-J MessageImpl wrapper around the message data. Changes to
    * the returned Message are not reflected in this message.
    *
    * @return a MessageImpl that wraps the AMQP message data in this {@link AMQPMessage}
    */
   public MessageImpl getProtonMessage() {
      ensureMessageDataScanned();
      ensureDataIsValid();

      MessageImpl protonMessage = null;
      if (data != null) {
         protonMessage = (MessageImpl) Message.Factory.create();
         data.rewind();
         protonMessage.decode(data.duplicate());
      }

      return protonMessage;
   }

   /**
    * Returns a copy of the message Header if one is present, changes to the returned
    * Header instance do not affect the original Message.
    *
    * @return a copy of the Message Header if one exists or null if none present.
    */
   public Header getHeader() {
      ensureMessageDataScanned();
      ensureDataIsValid();
      return scanForMessageSection(headerPosition, Header.class);
   }

   /**
    * Returns a copy of the MessageAnnotations in the message if present or null.  Changes to the
    * returned DeliveryAnnotations instance do not affect the original Message.
    *
    * @return a copy of the {@link DeliveryAnnotations} present in the message or null if non present.
    */
   public DeliveryAnnotations getDeliveryAnnotations() {
      ensureMessageDataScanned();
      ensureDataIsValid();
      return scanForMessageSection(deliveryAnnotationsPosition, DeliveryAnnotations.class);
   }

   /**
    * Sets the delivery annotations to be included when encoding the message for sending it on the wire.
    *
    * The broker can add additional message annotations as long as the annotations being added follow the
    * rules from the spec. If the user adds something that the remote doesn't understand and it is not
    * prefixed with "x-opt" the remote can just kill the link. See:
    *
    *     http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-annotations
    *
    * @param deliveryAnnotations delivery annotations used in the sendBuffer() method
    */
   public void setDeliveryAnnotationsForSendBuffer(DeliveryAnnotations deliveryAnnotations) {
      this.deliveryAnnotationsForSendBuffer = deliveryAnnotations;
   }

   /**
    * Returns a copy of the DeliveryAnnotations in the message if present or null.  Changes to the
    * returned MessageAnnotations instance do not affect the original Message.
    *
    * @return a copy of the {@link MessageAnnotations} present in the message or null if non present.
    */
   public MessageAnnotations getMessageAnnotations() {
      ensureMessageDataScanned();
      ensureDataIsValid();
      return scanForMessageSection(messageAnnotationsPosition, MessageAnnotations.class);
   }

   /**
    * Returns a copy of the message Properties if one is present, changes to the returned
    * Properties instance do not affect the original Message.
    *
    * @return a copy of the Message Properties if one exists or null if none present.
    */
   public Properties getProperties() {
      ensureMessageDataScanned();
      ensureDataIsValid();
      return scanForMessageSection(propertiesPosition, Properties.class);
   }

   /**
    * Returns a copy of the {@link ApplicationProperties} present in the message if present or null.
    * Changes to the returned MessageAnnotations instance do not affect the original Message.
    *
    * @return a copy of the {@link ApplicationProperties} present in the message or null if non present.
    */
   public ApplicationProperties getApplicationProperties() {
      ensureMessageDataScanned();
      ensureDataIsValid();
      return scanForMessageSection(applicationPropertiesPosition, ApplicationProperties.class);
   }

   /**
    * Retrieves the AMQP Section that composes the body of this message by decoding a
    * fresh copy from the encoded message data.  Changes to the returned value are not
    * reflected in the value encoded in the original message.
    *
    * @return the Section that makes up the body of this message.
    */
   public Section getBody() {
      ensureMessageDataScanned();
      ensureDataIsValid();

      // We only handle Sections of AmqpSequence, AmqpValue and Data types so we filter on those.
      // There could also be a Footer and no body so this will prevent a faulty return type in case
      // of no body or message type we don't handle.
      return scanForMessageSection(Math.max(0, remainingBodyPosition), AmqpSequence.class, AmqpValue.class, Data.class);
   }

   /**
    * Retrieves the AMQP Footer encoded in the data of this message by decoding a
    * fresh copy from the encoded message data.  Changes to the returned value are not
    * reflected in the value encoded in the original message.
    *
    * @return the Footer that was encoded into this AMQP Message.
    */
   public Footer getFooter() {
      ensureMessageDataScanned();
      ensureDataIsValid();
      return scanForMessageSection(Math.max(0, remainingBodyPosition), Footer.class);
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   private <T> T scanForMessageSection(int scanStartPosition, Class...targetTypes) {
      ensureMessageDataScanned();

      // In cases where we parsed out enough to know the value is not encoded in the message
      // we can exit without doing any reads or buffer hopping.
      if (scanStartPosition == VALUE_NOT_PRESENT) {
         return null;
      }

      ReadableBuffer buffer = data.duplicate().position(0);
      final DecoderImpl decoder = TLSEncode.getDecoder();

      buffer.position(scanStartPosition);

      T section = null;

      decoder.setBuffer(buffer);
      try {
         while (buffer.hasRemaining()) {
            TypeConstructor<?> constructor = decoder.readConstructor();
            for (Class<?> type : targetTypes) {
               if (type.equals(constructor.getTypeClass())) {
                  section = (T) constructor.readValue();
                  return section;
               }
            }

            constructor.skipValue();
         }
      } finally {
         decoder.setBuffer(null);
      }

      return section;
   }

   private ApplicationProperties lazyDecodeApplicationProperties() {
      if (applicationProperties == null && applicationPropertiesPosition != VALUE_NOT_PRESENT) {
         applicationProperties = scanForMessageSection(applicationPropertiesPosition, ApplicationProperties.class);
      }

      return applicationProperties;
   }

   @SuppressWarnings("unchecked")
   private Map<String, Object> getApplicationPropertiesMap(boolean createIfAbsent) {
      ApplicationProperties appMap = lazyDecodeApplicationProperties();
      Map<String, Object> map = null;

      if (appMap != null) {
         map = appMap.getValue();
      }

      if (map == null) {
         if (createIfAbsent) {
            map = new HashMap<>();
            this.applicationProperties = new ApplicationProperties(map);
         } else {
            map = Collections.EMPTY_MAP;
         }
      }

      return map;
   }

   @SuppressWarnings("unchecked")
   private Map<Symbol, Object> getMessageAnnotationsMap(boolean createIfAbsent) {
      Map<Symbol, Object> map = null;

      if (messageAnnotations != null) {
         map = messageAnnotations.getValue();
      }

      if (map == null) {
         if (createIfAbsent) {
            map = new HashMap<>();
            this.messageAnnotations = new MessageAnnotations(map);
         } else {
            map = Collections.EMPTY_MAP;
         }
      }

      return map;
   }

   private Object getMessageAnnotation(String annotation) {
      return getMessageAnnotation(Symbol.getSymbol(annotation));
   }

   private Object getMessageAnnotation(Symbol annotation) {
      return getMessageAnnotationsMap(false).get(annotation);
   }

   private Object removeMessageAnnotation(Symbol annotation) {
      return getMessageAnnotationsMap(false).remove(annotation);
   }

   private void setMessageAnnotation(String annotation, Object value) {
      setMessageAnnotation(Symbol.getSymbol(annotation), value);
   }

   private void setMessageAnnotation(Symbol annotation, Object value) {
      getMessageAnnotationsMap(true).put(annotation, value);
   }

   // Message decoding and copying methods.  Care must be taken here to ensure the buffer and the
   // state tracking information is kept up to data.  When the message is manually changed a forced
   // re-encode should be done to update the backing data with the in memory elements.

   private synchronized void ensureMessageDataScanned() {
      if (!messageDataScanned) {
         scanMessageData();
         messageDataScanned = true;
      }
   }

   private synchronized void scanMessageData() {
      DecoderImpl decoder = TLSEncode.getDecoder();
      decoder.setBuffer(data.rewind());

      header = null;
      messageAnnotations = null;
      properties = null;
      applicationProperties = null;
      expiration = 0;
      encodedHeaderSize = 0;
      memoryEstimate = -1;
      scheduledTime = -1;
      encodedDeliveryAnnotationsSize = 0;
      headerPosition = VALUE_NOT_PRESENT;
      deliveryAnnotationsPosition = VALUE_NOT_PRESENT;
      propertiesPosition = VALUE_NOT_PRESENT;
      applicationPropertiesPosition = VALUE_NOT_PRESENT;
      remainingBodyPosition = VALUE_NOT_PRESENT;

      try {
         while (data.hasRemaining()) {
            int constructorPos = data.position();
            TypeConstructor<?> constructor = decoder.readConstructor();
            if (Header.class.equals(constructor.getTypeClass())) {
               header = (Header) constructor.readValue();
               headerPosition = constructorPos;
               encodedHeaderSize = data.position();
               if (header.getTtl() != null) {
                  expiration = System.currentTimeMillis() + header.getTtl().intValue();
               }
            } else if (DeliveryAnnotations.class.equals(constructor.getTypeClass())) {
               // Don't decode these as they are not used by the broker at all and are
               // discarded on send, mark for lazy decode if ever needed.
               constructor.skipValue();
               deliveryAnnotationsPosition = constructorPos;
               encodedDeliveryAnnotationsSize = data.position() - constructorPos;
            } else if (MessageAnnotations.class.equals(constructor.getTypeClass())) {
               messageAnnotationsPosition = constructorPos;
               messageAnnotations = (MessageAnnotations) constructor.readValue();
            } else if (Properties.class.equals(constructor.getTypeClass())) {
               propertiesPosition = constructorPos;
               properties = (Properties) constructor.readValue();

               if (properties.getAbsoluteExpiryTime() != null && properties.getAbsoluteExpiryTime().getTime() > 0) {
                  expiration = properties.getAbsoluteExpiryTime().getTime();
               }
            } else if (ApplicationProperties.class.equals(constructor.getTypeClass())) {
               // Lazy decoding will start at the TypeConstructor of these ApplicationProperties
               // but we scan past it to grab the location of the possible body and footer section.
               applicationPropertiesPosition = constructorPos;
               constructor.skipValue();
               remainingBodyPosition = data.hasRemaining() ? data.position() : VALUE_NOT_PRESENT;
               break;
            } else {
               // This will be either the body or a Footer section which will be treated as an immutable
               // and be copied as is when re-encoding the message.
               remainingBodyPosition = constructorPos;
               break;
            }
         }
      } finally {
         decoder.setByteBuffer(null);
         data.rewind();
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message copy() {
      ensureDataIsValid();

      ReadableBuffer view = data.duplicate().rewind();
      byte[] newData = new byte[view.remaining()];

      // Copy the full message contents with delivery annotations as they will
      // be trimmed on send and may become useful on the broker at a later time.
      view.get(newData);

      AMQPMessage newEncode = new AMQPMessage(this.messageFormat, newData, extraProperties, coreMessageObjectPools);
      newEncode.setMessageID(this.getMessageID());
      return newEncode;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message copy(long newID) {
      return copy().setMessageID(newID);
   }

   // Core Message APIs for persisting and encoding of message data along with
   // utilities for checking memory usage and encoded size characteristics.

   /**
    * Would be called by the Artemis Core components to encode the message into
    * the provided send buffer.  Because of how Proton message data handling works
    * this method is not currently used by the AMQP protocol head and will not be
    * called for out-bound sends.
    *
    * @see #getSendBuffer(int) for the actual method used for message sends.
    */
   @Override
   public void sendBuffer(ByteBuf buffer, int deliveryCount) {
      ensureDataIsValid();
      NettyWritable writable = new NettyWritable(buffer);
      writable.put(getSendBuffer(deliveryCount));
   }

   /**
    * Gets a ByteBuf from the Message that contains the encoded bytes to be sent on the wire.
    * <p>
    * When possible this method will present the bytes to the caller without copying them into
    * a new buffer copy.  If copying is needed a new Netty buffer is created and returned. The
    * caller should ensure that the reference count on the returned buffer is always decremented
    * to avoid a leak in the case of a copied buffer being returned.
    *
    * @param deliveryCount
    *       The new delivery count for this message.
    *
    * @return a Netty ByteBuf containing the encoded bytes of this Message instance.
    */
   public ReadableBuffer getSendBuffer(int deliveryCount) {
      ensureDataIsValid();

      if (deliveryCount > 1) {
         return createCopyWithNewDeliveryCount(deliveryCount);
      } else if (deliveryAnnotationsPosition != VALUE_NOT_PRESENT
         || (deliveryAnnotationsForSendBuffer != null && !deliveryAnnotationsForSendBuffer.getValue().isEmpty())) {
         return createCopyWithSkippedOrExplicitlySetDeliveryAnnotations();
      } else {
         // Common case message has no delivery annotations, no delivery annotations for the send buffer were set
         // and this is the first delivery so no re-encoding or section skipping needed.
         return data.duplicate();
      }
   }

   private ReadableBuffer createCopyWithSkippedOrExplicitlySetDeliveryAnnotations() {
      // The original message had delivery annotations, or delivery annotations for the send buffer are set.
      // That means we must copy into a new buffer skipping the original delivery annotations section
      // (not meant to survive beyond this hop) and including the delivery annotations for the send buffer if set.
      ReadableBuffer duplicate = data.duplicate();

      final ByteBuf result = PooledByteBufAllocator.DEFAULT.heapBuffer(getEncodeSize());
      result.writeBytes(duplicate.limit(encodedHeaderSize).byteBuffer());
      writeDeliveryAnnotationsForSendBuffer(result);
      duplicate.clear();
      // skip existing delivery annotations of the original message
      duplicate.position(encodedHeaderSize + encodedDeliveryAnnotationsSize);
      result.writeBytes(duplicate.byteBuffer());

      return new NettyReadable(result);
   }

   private ReadableBuffer createCopyWithNewDeliveryCount(int deliveryCount) {
      assert deliveryCount > 1;

      final int amqpDeliveryCount = deliveryCount - 1;

      final ByteBuf result = PooledByteBufAllocator.DEFAULT.heapBuffer(getEncodeSize());

      // If this is re-delivering the message then the header must be re-encoded
      // otherwise we want to write the original header if present.  When a
      // Header is present we need to copy it as we are updating the re-delivered
      // message and not the stored version which we don't want to invalidate here.
      Header header = this.header;
      if (header == null) {
         header = new Header();
      } else {
         header = new Header(header);
      }

      header.setDeliveryCount(UnsignedInteger.valueOf(amqpDeliveryCount));
      TLSEncode.getEncoder().setByteBuffer(new NettyWritable(result));
      TLSEncode.getEncoder().writeObject(header);
      TLSEncode.getEncoder().setByteBuffer((WritableBuffer) null);

      writeDeliveryAnnotationsForSendBuffer(result);
      // skip existing delivery annotations of the original message
      data.position(encodedHeaderSize + encodedDeliveryAnnotationsSize);
      result.writeBytes(data.byteBuffer());
      data.position(0);

      return new NettyReadable(result);
   }

   private void writeDeliveryAnnotationsForSendBuffer(ByteBuf result) {
      if (deliveryAnnotationsForSendBuffer != null && !deliveryAnnotationsForSendBuffer.getValue().isEmpty()) {
         TLSEncode.getEncoder().setByteBuffer(new NettyWritable(result));
         TLSEncode.getEncoder().writeObject(deliveryAnnotationsForSendBuffer);
         TLSEncode.getEncoder().setByteBuffer((WritableBuffer) null);
      }
   }

   private int getDeliveryAnnotationsForSendBufferSize() {
      if (deliveryAnnotationsForSendBuffer == null || deliveryAnnotationsForSendBuffer.getValue().isEmpty()) {
         return 0;
      }
      DroppingWritableBuffer droppingWritableBuffer = new DroppingWritableBuffer();
      TLSEncode.getEncoder().setByteBuffer(droppingWritableBuffer);
      TLSEncode.getEncoder().writeObject(deliveryAnnotationsForSendBuffer);
      TLSEncode.getEncoder().setByteBuffer((WritableBuffer) null);
      return droppingWritableBuffer.position() + 1;
   }

   @Override
   public void messageChanged() {
      modified = true;
   }

   @Override
   public ByteBuf getBuffer() {
      if (data == null) {
         return null;
      } else {
         if (data instanceof NettyReadable) {
            return ((NettyReadable) data).getByteBuf();
         } else {
            return Unpooled.wrappedBuffer(data.byteBuffer());
         }
      }
   }

   @Override
   public AMQPMessage setBuffer(ByteBuf buffer) {
      // If this is ever called we would be in a highly unfortunate state
      this.data = null;
      return this;
   }

   @Override
   public int getEncodeSize() {
      ensureDataIsValid();
      // The encoded size will exclude any delivery annotations that are present as we will clip them.
      return data.remaining() - encodedDeliveryAnnotationsSize + getDeliveryAnnotationsForSendBufferSize();
   }

   @Override
   public void receiveBuffer(ByteBuf buffer) {
      // Not used for AMQP messages.
   }

   @Override
   public int getMemoryEstimate() {
      if (memoryEstimate == -1) {
         memoryEstimate = memoryOffset + (data != null ? data.capacity() : 0);
      }

      return memoryEstimate;
   }

   @Override
   public ICoreMessage toCore(CoreMessageObjectPools coreMessageObjectPools) {
      try {
         return AmqpCoreConverter.toCore(
            this, coreMessageObjectPools, header, messageAnnotations, properties, lazyDecodeApplicationProperties(), getBody(), getFooter());
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   public ICoreMessage toCore() {
      return toCore(coreMessageObjectPools);
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      ensureDataIsValid();
      targetRecord.writeInt(internalPersistSize());
      if (data.hasArray()) {
         targetRecord.writeBytes(data.array(), data.arrayOffset(), data.remaining());
      } else {
         targetRecord.writeBytes(data.byteBuffer());
      }
   }

   @Override
   public int getPersistSize() {
      ensureDataIsValid();
      return DataConstants.SIZE_INT + internalPersistSize();
   }

   private int internalPersistSize() {
      return data.remaining();
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record) {
      int size = record.readInt();
      byte[] recordArray = new byte[size];
      record.readBytes(recordArray);
      data = ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.wrap(recordArray));

      // Message state is now that the underlying buffer is loaded but the contents
      // not yet scanned, once done the message is fully populated and ready for dispatch.
      // Force a scan now and tidy the state variables to reflect where we are following
      // this reload from the store.
      scanMessageData();
      messageDataScanned = true;
      modified = false;

      // Message state should reflect that is came from persistent storage which
      // can happen when moved to a durable location.  We must re-encode here to
      // avoid a subsequent redelivery from suddenly appearing with a durable header
      // tag when the initial delivery did not.
      if (!isDurable()) {
         setDurable(true);
         reencode();
      }
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return getEncodeSize();
   }

   @Override
   public Persister<org.apache.activemq.artemis.api.core.Message> getPersister() {
      return AMQPMessagePersisterV2.getInstance();
   }

   @Override
   public void reencode() {
      ensureMessageDataScanned();

      // The address was updated on a message with Properties so we update them
      // for cases where there are no properties we aren't adding a properties
      // section which seems wrong but this preserves previous behavior.
      if (properties != null && address != null) {
         properties.setTo(address.toString());
      }

      encodeMessage();
      scanMessageData();

      messageDataScanned = true;
      modified = false;
   }

   private synchronized void ensureDataIsValid() {
      assert data != null;

      if (modified) {
         encodeMessage();
         modified = false;
      }
   }

   private synchronized void encodeMessage() {
      int estimated = Math.max(1500, data != null ? data.capacity() + 1000 : 0);
      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(estimated);
      EncoderImpl encoder = TLSEncode.getEncoder();

      try {
         NettyWritable writable = new NettyWritable(buffer);

         encoder.setByteBuffer(writable);
         if (header != null) {
            encoder.writeObject(header);
         }

         // We currently do not encode any delivery annotations but it is conceivable
         // that at some point they may need to be preserved, this is where that needs
         // to happen.

         if (messageAnnotations != null) {
            encoder.writeObject(messageAnnotations);
         }
         if (properties != null) {
            encoder.writeObject(properties);
         }

         // Whenever possible avoid encoding sections we don't need to by
         // checking if application properties where loaded or added and
         // encoding only in that case.
         if (applicationProperties != null) {
            encoder.writeObject(applicationProperties);

            // Now raw write the remainder body and footer if present.
            if (remainingBodyPosition != VALUE_NOT_PRESENT) {
               writable.put(data.position(remainingBodyPosition));
            }
         } else if (applicationPropertiesPosition != VALUE_NOT_PRESENT) {
            // Writes out ApplicationProperties, Body and Footer in one go if present.
            writable.put(data.position(applicationPropertiesPosition));
         } else if (remainingBodyPosition != VALUE_NOT_PRESENT) {
            // No Application properties at all so raw write Body and Footer sections
            writable.put(data.position(remainingBodyPosition));
         }

         byte[] bytes = new byte[buffer.writerIndex()];

         buffer.readBytes(bytes);
         data = ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.wrap(bytes));
      } finally {
         encoder.setByteBuffer((WritableBuffer) null);
         buffer.release();
      }
   }

   // These methods interact with the Extra Properties that can accompany the message but
   // because these are not sent on the wire, update to these does not force a re-encode on
   // send of the message.

   public TypedProperties createExtraProperties() {
      if (extraProperties == null) {
         extraProperties = new TypedProperties();
      }
      return extraProperties;
   }

   public TypedProperties getExtraProperties() {
      return extraProperties;
   }

   public AMQPMessage setExtraProperties(TypedProperties extraProperties) {
      this.extraProperties = extraProperties;
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putExtraBytesProperty(SimpleString key, byte[] value) {
      createExtraProperties().putBytesProperty(key, value);
      return this;
   }

   @Override
   public byte[] getExtraBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      if (extraProperties == null) {
         return null;
      } else {
         return extraProperties.getBytesProperty(key);
      }
   }

   @Override
   public byte[] removeExtraBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      if (extraProperties == null) {
         return null;
      } else {
         return (byte[])extraProperties.removeProperty(key);
      }
   }

   // Message meta data access for Core and AMQP usage.

   @Override
   public org.apache.activemq.artemis.api.core.Message setConnectionID(String connectionID) {
      this.connectionID = connectionID;
      return this;
   }

   @Override
   public String getConnectionID() {
      return connectionID;
   }

   public long getMessageFormat() {
      return messageFormat;
   }

   @Override
   public long getMessageID() {
      return messageID;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setMessageID(long id) {
      this.messageID = id;
      return this;
   }

   @Override
   public long getExpiration() {
      return expiration;
   }

   @Override
   public AMQPMessage setExpiration(long expiration) {
      if (properties != null) {
         if (expiration <= 0) {
            properties.setAbsoluteExpiryTime(null);
         } else {
            properties.setAbsoluteExpiryTime(new Date(expiration));
         }
      } else if (expiration > 0) {
         properties = new Properties();
         properties.setAbsoluteExpiryTime(new Date(expiration));
      }

      // We are overriding expiration with an Absolute expiration time so any
      // previous Header based TTL also needs to be removed.
      if (header != null) {
         header.setTtl(null);
      }

      this.expiration = Math.max(0, expiration);

      return this;
   }

   @Override
   public Object getUserID() {
      // User ID in Artemis API means Message ID
      if (properties != null && properties.getMessageId() != null) {
         return properties.getMessageId();
      } else {
         return null;
      }
   }

   /**
    * Before we added AMQP into Artemis the name getUserID was already taken by JMSMessageID.
    * We cannot simply change the names now as it would break the API for existing clients.
    *
    * This is to return and read the proper AMQP userID.
    *
    * @return the UserID value in the AMQP Properties if one is present.
    */
   public Object getAMQPUserID() {
      if (properties != null && properties.getUserId() != null) {
         Binary binary = properties.getUserId();
         return new String(binary.getArray(), binary.getArrayOffset(), binary.getLength(), StandardCharsets.UTF_8);
      } else {
         return null;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setUserID(Object userID) {
      return this;
   }

   @Override
   public Object getDuplicateProperty() {
      return null;
   }

   @Override
   public boolean isDurable() {
      if (header != null && header.getDurable() != null) {
         return header.getDurable();
      } else {
         return false;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setDurable(boolean durable) {
      if (header == null) {
         header = new Header();
      }

      header.setDurable(durable);  // Message needs to be re-encoded following this action.

      return this;
   }

   @Override
   public String getAddress() {
      SimpleString addressSimpleString = getAddressSimpleString();
      return addressSimpleString == null ? null : addressSimpleString.toString();
   }

   @Override
   public AMQPMessage setAddress(String address) {
      setAddress(cachedAddressSimpleString(address));
      return this;
   }

   @Override
   public AMQPMessage setAddress(SimpleString address) {
      this.address = address;
      createExtraProperties().putSimpleStringProperty(ADDRESS_PROPERTY, address);
      return this;
   }

   @Override
   public SimpleString getAddressSimpleString() {
      if (address == null) {
         TypedProperties extraProperties = getExtraProperties();

         // we first check if extraProperties is not null, no need to create it just to check it here
         if (extraProperties != null) {
            address = extraProperties.getSimpleStringProperty(ADDRESS_PROPERTY);
         }

         if (address == null) {
            // if it still null, it will look for the address on the getTo();
            if (properties != null && properties.getTo() != null) {
               address = cachedAddressSimpleString(properties.getTo());
            }
         }
      }
      return address;
   }

   private SimpleString cachedAddressSimpleString(String address) {
      return CoreMessageObjectPools.cachedAddressSimpleString(address, coreMessageObjectPools);
   }

   @Override
   public long getTimestamp() {
      if (properties != null && properties.getCreationTime() != null) {
         return properties.getCreationTime().getTime();
      } else {
         return 0L;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setTimestamp(long timestamp) {
      if (properties == null) {
         properties = new Properties();
      }
      properties.setCreationTime(new Date(timestamp));
      return this;
   }

   @Override
   public byte getPriority() {
      if (header != null && header.getPriority() != null) {
         return (byte) Math.min(header.getPriority().intValue(), MAX_MESSAGE_PRIORITY);
      } else {
         return DEFAULT_MESSAGE_PRIORITY;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setPriority(byte priority) {
      if (header == null) {
         header = new Header();
      }
      header.setPriority(UnsignedByte.valueOf(priority));
      return this;
   }

   @Override
   public SimpleString getReplyTo() {
      if (properties != null) {
         return SimpleString.toSimpleString(properties.getReplyTo());
      } else {
         return null;
      }
   }

   @Override
   public AMQPMessage setReplyTo(SimpleString address) {
      if (properties == null) {
         properties = new Properties();
      }

      properties.setReplyTo(address != null ? address.toString() : null);
      return this;
   }

   @Override
   public RoutingType getRoutingType() {
      Object routingType = getMessageAnnotation(AMQPMessageSupport.ROUTING_TYPE);

      if (routingType != null) {
         return RoutingType.getType((byte) routingType);
      } else {
         routingType = getMessageAnnotation(AMQPMessageSupport.JMS_DEST_TYPE_MSG_ANNOTATION);
         if (routingType != null) {
            if (AMQPMessageSupport.QUEUE_TYPE == (byte) routingType || AMQPMessageSupport.TEMP_QUEUE_TYPE == (byte) routingType) {
               return RoutingType.ANYCAST;
            } else if (AMQPMessageSupport.TOPIC_TYPE == (byte) routingType || AMQPMessageSupport.TEMP_TOPIC_TYPE == (byte) routingType) {
               return RoutingType.MULTICAST;
            }
         } else {
            return null;
         }

         return null;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setRoutingType(RoutingType routingType) {
      if (routingType == null) {
         removeMessageAnnotation(AMQPMessageSupport.ROUTING_TYPE);
      } else {
         setMessageAnnotation(AMQPMessageSupport.ROUTING_TYPE, routingType.getType());
      }
      return this;
   }

   @Override
   public SimpleString getGroupID() {
      ensureMessageDataScanned();

      if (properties != null && properties.getGroupId() != null) {
         return SimpleString.toSimpleString(properties.getGroupId(),
            coreMessageObjectPools == null ? null : coreMessageObjectPools.getGroupIdStringSimpleStringPool());
      } else {
         return null;
      }
   }

   @Override
   public int getGroupSequence() {
      ensureMessageDataScanned();

      if (properties != null && properties.getGroupSequence() != null) {
         return properties.getGroupSequence().intValue();
      } else {
         return 0;
      }
   }

   @Override
   public Long getScheduledDeliveryTime() {
      if (scheduledTime < 0) {
         Object objscheduledTime = getMessageAnnotation(AMQPMessageSupport.SCHEDULED_DELIVERY_TIME);
         Object objdelay = getMessageAnnotation(AMQPMessageSupport.SCHEDULED_DELIVERY_DELAY);

         if (objscheduledTime != null && objscheduledTime instanceof Number) {
            this.scheduledTime = ((Number) objscheduledTime).longValue();
         } else if (objdelay != null && objdelay instanceof Number) {
            this.scheduledTime = System.currentTimeMillis() + ((Number) objdelay).longValue();
         } else {
            this.scheduledTime = 0;
         }
      }

      return scheduledTime;
   }

   @Override
   public AMQPMessage setScheduledDeliveryTime(Long time) {
      if (time != null && time.longValue() > 0) {
         setMessageAnnotation(AMQPMessageSupport.SCHEDULED_DELIVERY_TIME, time);
         removeMessageAnnotation(AMQPMessageSupport.SCHEDULED_DELIVERY_DELAY);
         scheduledTime = time;
      } else {
         removeMessageAnnotation(AMQPMessageSupport.SCHEDULED_DELIVERY_TIME);
         removeMessageAnnotation(AMQPMessageSupport.SCHEDULED_DELIVERY_DELAY);
         scheduledTime = 0;
      }

      return this;
   }

   @Override
   public Object removeAnnotation(SimpleString key) {
      return removeMessageAnnotation(Symbol.getSymbol(key.toString()));
   }

   @Override
   public Object getAnnotation(SimpleString key) {
      return getMessageAnnotation(key.toString());
   }

   @Override
   public AMQPMessage setAnnotation(SimpleString key, Object value) {
      setMessageAnnotation(key.toString(), value);
      return this;
   }

   // JMS Style property access methods.  These can result in additional decode of AMQP message
   // data from Application properties.  Updates to application properties puts the message in a
   // dirty state and requires a re-encode of the data to update all buffer state data otherwise
   // the next send of the Message will not include changes made here.

   @Override
   public Object removeProperty(SimpleString key) {
      return removeProperty(key.toString());
   }

   @Override
   public Object removeProperty(String key) {
      return getApplicationPropertiesMap(false).remove(key);
   }

   @Override
   public boolean containsProperty(SimpleString key) {
      return containsProperty(key.toString());
   }

   @Override
   public boolean containsProperty(String key) {
      return getApplicationPropertiesMap(false).containsKey(key);
   }

   @Override
   public Boolean getBooleanProperty(String key) throws ActiveMQPropertyConversionException {
      return (Boolean) getApplicationPropertiesMap(false).get(key);
   }

   @Override
   public Byte getByteProperty(String key) throws ActiveMQPropertyConversionException {
      return (Byte) getApplicationPropertiesMap(false).get(key);
   }

   @Override
   public Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException {
      return (Double) getApplicationPropertiesMap(false).get(key);
   }

   @Override
   public Integer getIntProperty(String key) throws ActiveMQPropertyConversionException {
      return (Integer) getApplicationPropertiesMap(false).get(key);
   }

   @Override
   public Long getLongProperty(String key) throws ActiveMQPropertyConversionException {
      return (Long) getApplicationPropertiesMap(false).get(key);
   }

   @Override
   public Object getObjectProperty(String key) {
      if (key.equals(MessageUtil.TYPE_HEADER_NAME.toString())) {
         if (properties != null) {
            return properties.getSubject();
         }
      } else if (key.equals(MessageUtil.CONNECTION_ID_PROPERTY_NAME.toString())) {
         return getConnectionID();
      } else if (key.equals(MessageUtil.JMSXGROUPID)) {
         return getGroupID();
      } else if (key.equals(MessageUtil.JMSXGROUPSEQ)) {
         return getGroupSequence();
      } else if (key.equals(MessageUtil.JMSXUSERID)) {
         return getAMQPUserID();
      } else if (key.equals(MessageUtil.CORRELATIONID_HEADER_NAME.toString())) {
         if (properties != null && properties.getCorrelationId() != null) {
            return AMQPMessageIdHelper.INSTANCE.toCorrelationIdString(properties.getCorrelationId());
         }
      } else {
         Object value = getApplicationPropertiesMap(false).get(key);
         if (value instanceof UnsignedInteger ||
             value instanceof UnsignedByte ||
             value instanceof UnsignedLong ||
             value instanceof UnsignedShort) {
            return ((Number)value).longValue();
         } else {
            return value;
         }
      }

      return null;
   }

   @Override
   public Short getShortProperty(String key) throws ActiveMQPropertyConversionException {
      return (Short) getApplicationPropertiesMap(false).get(key);
   }

   @Override
   public Float getFloatProperty(String key) throws ActiveMQPropertyConversionException {
      return (Float) getApplicationPropertiesMap(false).get(key);
   }

   @Override
   public String getStringProperty(String key) throws ActiveMQPropertyConversionException {
      if (key.equals(MessageUtil.TYPE_HEADER_NAME.toString())) {
         return properties.getSubject();
      } else if (key.equals(MessageUtil.CONNECTION_ID_PROPERTY_NAME.toString())) {
         return getConnectionID();
      } else {
         return (String) getApplicationPropertiesMap(false).get(key);
      }
   }

   @Override
   public Set<SimpleString> getPropertyNames() {
      HashSet<SimpleString> values = new HashSet<>();
      for (Object k : getApplicationPropertiesMap(false).keySet()) {
         values.add(SimpleString.toSimpleString(k.toString(), getPropertyKeysPool()));
      }
      return values;
   }

   @Override
   public Boolean getBooleanProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getBooleanProperty(key.toString());
   }

   @Override
   public Byte getByteProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getByteProperty(key.toString());
   }

   @Override
   public byte[] getBytesProperty(String key) throws ActiveMQPropertyConversionException {
      return (byte[]) getApplicationPropertiesMap(false).get(key);
   }

   @Override
   public Double getDoubleProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getDoubleProperty(key.toString());
   }

   @Override
   public Integer getIntProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getIntProperty(key.toString());
   }

   @Override
   public Long getLongProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getLongProperty(key.toString());
   }

   @Override
   public Object getObjectProperty(SimpleString key) {
      return getObjectProperty(key.toString());
   }

   @Override
   public Short getShortProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getShortProperty(key.toString());
   }

   @Override
   public Float getFloatProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getFloatProperty(key.toString());
   }

   @Override
   public String getStringProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getStringProperty(key.toString());
   }

   @Override
   public SimpleString getSimpleStringProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getSimpleStringProperty(key.toString());
   }

   @Override
   public byte[] getBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getBytesProperty(key.toString());
   }
   @Override
   public SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException {
      return SimpleString.toSimpleString((String) getApplicationPropertiesMap(false).get(key), getPropertyValuesPool());
   }

   // Core Message Application Property update methods, calling these puts the message in a dirty
   // state and requires a re-encode of the data to update all buffer state data.  If no re-encode
   // is done prior to the next dispatch the old view of the message will be sent.

   @Override
   public org.apache.activemq.artemis.api.core.Message putBooleanProperty(String key, boolean value) {
      getApplicationPropertiesMap(true).put(key, Boolean.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putByteProperty(String key, byte value) {
      getApplicationPropertiesMap(true).put(key, Byte.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putBytesProperty(String key, byte[] value) {
      getApplicationPropertiesMap(true).put(key, value);
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putShortProperty(String key, short value) {
      getApplicationPropertiesMap(true).put(key, Short.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putCharProperty(String key, char value) {
      getApplicationPropertiesMap(true).put(key, Character.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putIntProperty(String key, int value) {
      getApplicationPropertiesMap(true).put(key, Integer.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putLongProperty(String key, long value) {
      getApplicationPropertiesMap(true).put(key, Long.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putFloatProperty(String key, float value) {
      getApplicationPropertiesMap(true).put(key, Float.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putDoubleProperty(String key, double value) {
      getApplicationPropertiesMap(true).put(key, Double.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putBooleanProperty(SimpleString key, boolean value) {
      getApplicationPropertiesMap(true).put(key.toString(), Boolean.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putByteProperty(SimpleString key, byte value) {
      return putByteProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putBytesProperty(SimpleString key, byte[] value) {
      return putBytesProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putShortProperty(SimpleString key, short value) {
      return putShortProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putCharProperty(SimpleString key, char value) {
      return putCharProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putIntProperty(SimpleString key, int value) {
      return putIntProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putLongProperty(SimpleString key, long value) {
      return putLongProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putFloatProperty(SimpleString key, float value) {
      return putFloatProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putDoubleProperty(SimpleString key, double value) {
      return putDoubleProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putStringProperty(String key, String value) {
      getApplicationPropertiesMap(true).put(key, value);
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putObjectProperty(String key, Object value) throws ActiveMQPropertyConversionException {
      getApplicationPropertiesMap(true).put(key, value);
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putObjectProperty(SimpleString key, Object value) throws ActiveMQPropertyConversionException {
      return putObjectProperty(key.toString(), value);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putStringProperty(SimpleString key, SimpleString value) {
      return putStringProperty(key.toString(), value.toString());
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putStringProperty(SimpleString key, String value) {
      return putStringProperty(key.toString(), value);
   }

   @Override
   public SimpleString getLastValueProperty() {
      return getSimpleStringProperty(HDR_LAST_VALUE_NAME);
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setLastValueProperty(SimpleString lastValueName) {
      return putStringProperty(HDR_LAST_VALUE_NAME, lastValueName);
   }

   @Override
   public String toString() {
      return "AMQPMessage [durable=" + isDurable() +
         ", messageID=" + getMessageID() +
         ", address=" + getAddress() +
         ", size=" + getEncodeSize() +
         ", applicationProperties=" + applicationProperties +
         ", properties=" + properties +
         ", extraProperties = " + getExtraProperties() +
         "]";
   }

   @Override
   public synchronized boolean acceptsConsumer(long consumer) {
      if (rejectedConsumers == null) {
         return true;
      } else {
         return !rejectedConsumers.contains(consumer);
      }
   }

   @Override
   public synchronized void rejectConsumer(long consumer) {
      if (rejectedConsumers == null) {
         rejectedConsumers = new HashSet<>();
      }

      rejectedConsumers.add(consumer);
   }

   private SimpleString.StringSimpleStringPool getPropertyKeysPool() {
      return coreMessageObjectPools == null ? null : coreMessageObjectPools.getPropertiesStringSimpleStringPools().getPropertyKeysPool();
   }

   private SimpleString.StringSimpleStringPool getPropertyValuesPool() {
      return coreMessageObjectPools == null ? null : coreMessageObjectPools.getPropertiesStringSimpleStringPools().getPropertyValuesPool();
   }
}