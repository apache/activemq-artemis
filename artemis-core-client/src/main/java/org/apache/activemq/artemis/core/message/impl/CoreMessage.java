/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.message.impl;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RefCountMessage;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.buffers.impl.ResetLimitWrappedActiveMQBuffer;
import org.apache.activemq.artemis.core.message.LargeBodyEncoder;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.jboss.logging.Logger;

/** Note: you shouldn't change properties using multi-threads. Change your properties before you can send it to multiple
 *  consumers */
public class CoreMessage extends RefCountMessage implements ICoreMessage {

   public static final int BUFFER_HEADER_SPACE = PacketImpl.PACKET_HEADERS_SIZE;

   private volatile int memoryEstimate = -1;

   private static final Logger logger = Logger.getLogger(CoreMessage.class);

   // There's an integer with the number of bytes for the body
   public static final int BODY_OFFSET = DataConstants.SIZE_INT;

   /** That is the encode for the whole message, including properties..
       it does not include the buffer for the Packet send and receive header on core protocol */
   protected ByteBuf buffer;

   private volatile boolean validBuffer = false;

   protected volatile ResetLimitWrappedActiveMQBuffer writableBuffer;

   Object body;

   protected int endOfBodyPosition = -1;

   protected int messageIDPosition = -1;

   protected long messageID;

   protected SimpleString address;

   protected byte type;

   protected boolean durable;

   /**
    * GMT milliseconds at which this message expires. 0 means never expires *
    */
   private long expiration;

   protected long timestamp;

   protected byte priority;

   private UUID userID;

   private int propertiesLocation = -1;

   protected volatile TypedProperties properties;

   private final CoreMessageObjectPools coreMessageObjectPools;

   public CoreMessage(final CoreMessageObjectPools coreMessageObjectPools) {
      this.coreMessageObjectPools = coreMessageObjectPools;
   }

   public CoreMessage() {
      this.coreMessageObjectPools = null;
   }

   /** On core there's no delivery annotation */
   @Override
   public Object getAnnotation(SimpleString key) {
      return getObjectProperty(key);
   }

   /** On core there's no delivery annotation */
   @Override
   public Object removeAnnotation(SimpleString key) {
      return removeProperty(key);
   }

   @Override
   public void cleanupInternalProperties() {
      if (properties.hasInternalProperties()) {
         LinkedList<SimpleString> valuesToRemove = null;

         for (SimpleString name : getPropertyNames()) {
            // We use properties to establish routing context on clustering.
            // However if the client resends the message after receiving, it needs to be removed
            if ((name.startsWith(Message.HDR_ROUTE_TO_IDS) && !name.equals(Message.HDR_ROUTE_TO_IDS)) || (name.startsWith(Message.HDR_ROUTE_TO_ACK_IDS) && !name.equals(Message.HDR_ROUTE_TO_ACK_IDS))) {
               if (valuesToRemove == null) {
                  valuesToRemove = new LinkedList<>();
               }
               valuesToRemove.add(name);
            }
         }

         if (valuesToRemove != null) {
            for (SimpleString removal : valuesToRemove) {
               this.removeProperty(removal);
            }
         }
      }
   }

   @Override
   public Persister<Message> getPersister() {
      return CoreMessagePersister.getInstance();
   }

   public CoreMessage initBuffer(final int initialMessageBufferSize) {
      buffer = ActiveMQBuffers.dynamicBuffer(initialMessageBufferSize).byteBuf();

      // There's a bug in netty which means a dynamic buffer won't resize until you write a byte
      buffer.writeByte((byte) 0);

      buffer.setIndex(BODY_OFFSET, BODY_OFFSET);

      return this;
   }

   @Override
   public SimpleString getReplyTo() {
      return getSimpleStringProperty(MessageUtil.REPLYTO_HEADER_NAME);
   }

   @Override
   public RoutingType getRoutingType() {
      if (containsProperty(Message.HDR_ROUTING_TYPE)) {
         return RoutingType.getType(getByteProperty(Message.HDR_ROUTING_TYPE));
      }
      return null;
   }

   @Override
   public Message setRoutingType(RoutingType routingType) {
      if (routingType == null) {
         removeProperty(Message.HDR_ROUTING_TYPE);
      } else {
         putByteProperty(Message.HDR_ROUTING_TYPE, routingType.getType());
      }
      return this;
   }

   @Override
   public CoreMessage setReplyTo(SimpleString address) {

      if (address == null) {
         checkProperties();
         properties.removeProperty(MessageUtil.REPLYTO_HEADER_NAME);
      } else {
         putStringProperty(MessageUtil.REPLYTO_HEADER_NAME, address);
      }
      return this;
   }

   @Override
   public void receiveBuffer(ByteBuf buffer) {
      this.buffer = buffer;
      this.buffer.retain();
      decode(false);
   }

   /** This will fix the incoming body of 1.x messages */
   @Override
   public void receiveBuffer_1X(ByteBuf buffer) {
      this.buffer = buffer;
      this.buffer.retain();
      decode(true);
      validBuffer = false;
   }

   @Override
   public ActiveMQBuffer getReadOnlyBodyBuffer() {
      checkEncode();
      internalWritableBuffer();
      return new ChannelBufferWrapper(buffer.slice(BODY_OFFSET, endOfBodyPosition - BUFFER_HEADER_SPACE).setIndex(0, endOfBodyPosition - BUFFER_HEADER_SPACE).asReadOnly());
   }

   @Override
   public SimpleString getGroupID() {
      return this.getSimpleStringProperty(Message.HDR_GROUP_ID);
   }

   /**
    * @param sendBuffer
    * @param deliveryCount Some protocols (AMQP) will have this as part of the message. ignored on core
    */
   @Override
   public void sendBuffer(ByteBuf sendBuffer, int deliveryCount) {
      checkEncode();
      sendBuffer.writeBytes(buffer, 0, buffer.writerIndex());
   }

   /**
    * Recast the message as an 1.4 message
    */
   @Override
   public void sendBuffer_1X(ByteBuf sendBuffer) {
      checkEncode();
      ByteBuf tmpBuffer = buffer.duplicate();
      sendBuffer.writeInt(endOfBodyPosition + DataConstants.SIZE_INT);
      tmpBuffer.readerIndex(DataConstants.SIZE_INT);
      tmpBuffer.readBytes(sendBuffer, endOfBodyPosition - BUFFER_HEADER_SPACE);
      sendBuffer.writeInt(tmpBuffer.writerIndex() + DataConstants.SIZE_INT + BUFFER_HEADER_SPACE);
      tmpBuffer.readBytes(sendBuffer, tmpBuffer.readableBytes());
      sendBuffer.readerIndex(0);
   }

   private synchronized void checkEncode() {
      if (!validBuffer) {
         encode();
      }
   }

   @Override
   public Long getScheduledDeliveryTime() {
      checkProperties();
      Object property = getObjectProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);

      if (property != null && property instanceof Number) {
         return ((Number) property).longValue();
      }

      return 0L;
   }

   @Override
   public CoreMessage setScheduledDeliveryTime(Long time) {
      checkProperties();
      if (time == null || time == 0) {
         removeProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
      } else {
         putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, time);
      }
      return this;
   }

   @Override
   public InputStream getBodyInputStream() {
      return null;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public ActiveMQBuffer getBodyBuffer() {
      // if using the writable buffer, we must parse properties
      checkProperties();

      internalWritableBuffer();

      return writableBuffer;
   }

   private void internalWritableBuffer() {
      if (writableBuffer == null) {
         writableBuffer = new ResetLimitWrappedActiveMQBuffer(BODY_OFFSET, buffer.duplicate(), this);
         if (endOfBodyPosition > 0) {
            writableBuffer.byteBuf().setIndex(BODY_OFFSET, endOfBodyPosition - BUFFER_HEADER_SPACE + BODY_OFFSET);
            writableBuffer.resetReaderIndex();
         }
      }
   }

   @Override
   public int getEndOfBodyPosition() {
      if (endOfBodyPosition < 0) {
         endOfBodyPosition = getBodyBuffer().writerIndex();
      }
      return endOfBodyPosition;
   }

   public TypedProperties getTypedProperties() {
      return checkProperties();
   }

   @Override
   public void messageChanged() {
      validBuffer = false;
   }

   protected CoreMessage(CoreMessage other) {
      this(other, other.properties);
   }

   public CoreMessage(long id, int bufferSize) {
      this(id, bufferSize, null);
   }

   public CoreMessage(long id, int bufferSize, CoreMessageObjectPools coreMessageObjectPools) {
      this.initBuffer(bufferSize);
      this.setMessageID(id);
      this.coreMessageObjectPools = coreMessageObjectPools;
   }

   protected CoreMessage(CoreMessage other, TypedProperties copyProperties) {
      this.body = other.body;
      this.endOfBodyPosition = other.endOfBodyPosition;
      this.messageID = other.messageID;
      this.address = other.address;
      this.type = other.type;
      this.durable = other.durable;
      this.expiration = other.expiration;
      this.timestamp = other.timestamp;
      this.priority = other.priority;
      this.userID = other.userID;
      this.coreMessageObjectPools = other.coreMessageObjectPools;
      if (copyProperties != null) {
         this.properties = new TypedProperties(copyProperties);
      }
      if (other.buffer != null) {
         this.buffer = other.buffer.copy();
      }
   }

   @Override
   public void copyHeadersAndProperties(final Message msg) {
      messageID = msg.getMessageID();
      address = msg.getAddressSimpleString();
      userID = (UUID) msg.getUserID();
      type = msg.toCore().getType();
      durable = msg.isDurable();
      expiration = msg.getExpiration();
      timestamp = msg.getTimestamp();
      priority = msg.getPriority();

      if (msg instanceof CoreMessage) {
         properties = ((CoreMessage) msg).getTypedProperties();
      }
   }

   @Override
   public Message copy() {
      checkProperties();
      checkEncode();
      return new CoreMessage(this);
   }

   @Override
   public Message copy(long newID) {
      return copy().setMessageID(newID);
   }

   @Override
   public long getExpiration() {
      return expiration;
   }

   @Override
   public long getTimestamp() {
      return timestamp;
   }

   @Override
   public CoreMessage setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
   }

   @Override
   public long getMessageID() {
      return messageID;
   }

   @Override
   public byte getPriority() {
      return priority;
   }

   @Override
   public UUID getUserID() {
      return userID;
   }

   @Override
   public CoreMessage setUserID(Object uuid) {
      this.userID = (UUID) uuid;
      return this;
   }

   @Override
   public String getValidatedUserID() {
      return getStringProperty(Message.HDR_VALIDATED_USER);
   }

   @Override
   public CoreMessage setValidatedUserID(String validatedUserID) {
      putStringProperty(Message.HDR_VALIDATED_USER, SimpleString.toSimpleString(validatedUserID, getPropertyValuesPool()));
      return this;
   }

   @Override
   public CoreMessage setMessageID(long messageID) {
      this.messageID = messageID;
      if (messageIDPosition >= 0 && validBuffer) {
         buffer.setLong(messageIDPosition, messageID);
      }
      return this;
   }

   @Override
   public CoreMessage setAddress(SimpleString address) {
      if (address == null && this.address == null) {
         // no-op so just return
         return this;
      }
      if (validBuffer && (address == null || !address.equals(this.address))) {
         messageChanged();
      }
      this.address = address;
      return this;
   }

   @Override
   public SimpleString getAddressSimpleString() {
      return address;
   }

   @Override
   public CoreMessage setExpiration(long expiration) {
      this.expiration = expiration;
      messageChanged();
      return this;
   }

   @Override
   public CoreMessage setPriority(byte priority) {
      this.priority = priority;
      messageChanged();
      return this;
   }

   public CoreMessage setUserID(UUID userID) {
      this.userID = userID;
      messageChanged();
      return this;
   }

   /**
    * I am keeping this synchronized as the decode of the Properties is lazy
    */
   protected TypedProperties checkProperties() {
      if (properties == null) {
         TypedProperties properties = new TypedProperties();
         if (buffer != null && propertiesLocation >= 0) {
            final ByteBuf byteBuf = buffer.duplicate().readerIndex(propertiesLocation);
            properties.decode(byteBuf, coreMessageObjectPools == null ? null : coreMessageObjectPools.getPropertiesDecoderPools());
         }
         this.properties = properties;
      }

      return this.properties;
   }

   @Override
   public int getMemoryEstimate() {
      if (memoryEstimate == -1) {
         memoryEstimate = memoryOffset +
            (buffer != null ? buffer.capacity() : 0) +
            (properties != null ? properties.getMemoryOffset() : 0);
      }

      return memoryEstimate;
   }

   @Override
   public boolean isServerMessage() {
      // even though CoreMessage is used both on server and client
      // callers are interested in knowing if this is a server large message
      // as it will be used to send the body from the files.
      //
      // this may need further refactoring when we improve large messages
      // and expose that functionality to other protocols.
      return false;
   }

   @Override
   public byte getType() {
      return type;
   }

   @Override
   public CoreMessage setType(byte type) {
      this.type = type;
      return this;
   }

   private void decode(boolean beforeAddress) {
      endOfBodyPosition = buffer.readInt();

      buffer.skipBytes(endOfBodyPosition - BUFFER_HEADER_SPACE);

      decodeHeadersAndProperties(buffer, true);
      buffer.readerIndex(0);
      validBuffer = true;

      if (beforeAddress) {
         endOfBodyPosition = endOfBodyPosition - DataConstants.SIZE_INT;
      }

      internalWritableBuffer();
   }

   public void decodeHeadersAndProperties(final ByteBuf buffer) {
      decodeHeadersAndProperties(buffer, false);
   }

   private void decodeHeadersAndProperties(final ByteBuf buffer, boolean lazyProperties) {
      messageIDPosition = buffer.readerIndex();
      messageID = buffer.readLong();

      address = SimpleString.readNullableSimpleString(buffer, coreMessageObjectPools == null ? null : coreMessageObjectPools.getAddressDecoderPool());
      if (buffer.readByte() == DataConstants.NOT_NULL) {
         byte[] bytes = new byte[16];
         buffer.readBytes(bytes);
         userID = new UUID(UUID.TYPE_TIME_BASED, bytes);
      } else {
         userID = null;
      }
      type = buffer.readByte();
      durable = buffer.readBoolean();
      expiration = buffer.readLong();
      timestamp = buffer.readLong();
      priority = buffer.readByte();
      if (lazyProperties) {
         properties = null;
         propertiesLocation = buffer.readerIndex();
      } else {
         properties = new TypedProperties();
         properties.decode(buffer, coreMessageObjectPools == null ? null : coreMessageObjectPools.getPropertiesDecoderPools());
      }
   }

   public synchronized CoreMessage encode() {

      checkProperties();

      if (writableBuffer != null) {
         // The message encode takes into consideration the PacketImpl which is not part of this encoding
         // so we always need to take the BUFFER_HEADER_SPACE from packet impl into consideration
         endOfBodyPosition = writableBuffer.writerIndex() + BUFFER_HEADER_SPACE - 4;
      } else if (endOfBodyPosition <= 0) {
         endOfBodyPosition = BUFFER_HEADER_SPACE + DataConstants.SIZE_INT;
      }

      buffer.setIndex(0, 0);
      buffer.writeInt(endOfBodyPosition);

      // The end of body position
      buffer.writerIndex(endOfBodyPosition - BUFFER_HEADER_SPACE + DataConstants.SIZE_INT);

      encodeHeadersAndProperties(buffer);

      validBuffer = true;

      return this;
   }

   public void encodeHeadersAndProperties(final ByteBuf buffer) {
      checkProperties();
      messageIDPosition = buffer.writerIndex();
      buffer.writeLong(messageID);
      SimpleString.writeNullableSimpleString(buffer, address);
      if (userID == null) {
         buffer.writeByte(DataConstants.NULL);
      } else {
         buffer.writeByte(DataConstants.NOT_NULL);
         buffer.writeBytes(userID.asBytes());
      }
      buffer.writeByte(type);
      buffer.writeBoolean(durable);
      buffer.writeLong(expiration);
      buffer.writeLong(timestamp);
      buffer.writeByte(priority);
      properties.encode(buffer);
   }

   @Override
   public int getHeadersAndPropertiesEncodeSize() {
      return DataConstants.SIZE_LONG + // Message ID
         DataConstants.SIZE_BYTE + // user id null?
         (userID == null ? 0 : 16) +
             /* address */SimpleString.sizeofNullableString(address) +
         DataConstants./* Type */SIZE_BYTE +
         DataConstants./* Durable */SIZE_BOOLEAN +
         DataConstants./* Expiration */SIZE_LONG +
         DataConstants./* Timestamp */SIZE_LONG +
         DataConstants./* Priority */SIZE_BYTE +
             /* PropertySize and Properties */checkProperties().getEncodeSize();
   }

   @Override
   public Object getDuplicateProperty() {
      return getObjectProperty(Message.HDR_DUPLICATE_DETECTION_ID);
   }

   @Override
   public SimpleString getLastValueProperty() {
      return getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME);
   }

   @Override
   public Message setLastValueProperty(SimpleString lastValueName) {
      return putStringProperty(Message.HDR_LAST_VALUE_NAME, lastValueName);
   }

   @Override
   public int getEncodeSize() {
      checkEncode();
      return buffer == null ? -1 : buffer.writerIndex();
   }

   @Override
   public boolean isLargeMessage() {
      return false;
   }

   @Override
   public String getAddress() {
      if (address == null) {
         return null;
      } else {
         return address.toString();
      }
   }

   @Override
   public CoreMessage setAddress(String address) {
      messageChanged();
      this.address = SimpleString.toSimpleString(address, coreMessageObjectPools == null ? null : coreMessageObjectPools.getAddressStringSimpleStringPool());
      return this;
   }

   @Override
   public CoreMessage setBuffer(ByteBuf buffer) {
      this.buffer = buffer;

      return this;
   }

   @Override
   public ByteBuf getBuffer() {
      return buffer;
   }

   @Override
   public boolean isDurable() {
      return durable;
   }

   @Override
   public CoreMessage setDurable(boolean durable) {
      messageChanged();
      this.durable = durable;
      return this;
   }

   @Override
   public CoreMessage putBooleanProperty(final String key, final boolean value) {
      messageChanged();
      checkProperties();
      properties.putBooleanProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public CoreMessage putBooleanProperty(final SimpleString key, final boolean value) {
      messageChanged();
      checkProperties();
      properties.putBooleanProperty(key, value);
      return this;
   }

   @Override
   public Boolean getBooleanProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getBooleanProperty(key);
   }

   @Override
   public Boolean getBooleanProperty(final String key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getBooleanProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public CoreMessage putByteProperty(final SimpleString key, final byte value) {
      messageChanged();
      checkProperties();
      properties.putByteProperty(key, value);
      return this;
   }

   @Override
   public CoreMessage putByteProperty(final String key, final byte value) {
      messageChanged();
      checkProperties();
      properties.putByteProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);

      return this;
   }

   @Override
   public Byte getByteProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getByteProperty(key);
   }

   @Override
   public Byte getByteProperty(final String key) throws ActiveMQPropertyConversionException {
      return getByteProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public CoreMessage putBytesProperty(final SimpleString key, final byte[] value) {
      messageChanged();
      checkProperties();
      properties.putBytesProperty(key, value);

      return this;
   }

   @Override
   public CoreMessage putBytesProperty(final String key, final byte[] value) {
      messageChanged();
      checkProperties();
      properties.putBytesProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public byte[] getBytesProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getBytesProperty(key);
   }

   @Override
   public byte[] getBytesProperty(final String key) throws ActiveMQPropertyConversionException {
      return getBytesProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public CoreMessage putCharProperty(SimpleString key, char value) {
      messageChanged();
      checkProperties();
      properties.putCharProperty(key, value);
      return this;
   }

   @Override
   public CoreMessage putCharProperty(String key, char value) {
      messageChanged();
      checkProperties();
      properties.putCharProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public CoreMessage putShortProperty(final SimpleString key, final short value) {
      messageChanged();
      checkProperties();
      properties.putShortProperty(key, value);
      return this;
   }

   @Override
   public CoreMessage putShortProperty(final String key, final short value) {
      messageChanged();
      checkProperties();
      properties.putShortProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public CoreMessage putIntProperty(final SimpleString key, final int value) {
      messageChanged();
      checkProperties();
      properties.putIntProperty(key, value);
      return this;
   }

   @Override
   public CoreMessage putIntProperty(final String key, final int value) {
      messageChanged();
      checkProperties();
      properties.putIntProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public Integer getIntProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getIntProperty(key);
   }

   @Override
   public Integer getIntProperty(final String key) throws ActiveMQPropertyConversionException {
      return getIntProperty(SimpleString.toSimpleString(key));
   }

   @Override
   public CoreMessage putLongProperty(final SimpleString key, final long value) {
      messageChanged();
      checkProperties();
      properties.putLongProperty(key, value);
      return this;
   }

   @Override
   public CoreMessage putLongProperty(final String key, final long value) {
      messageChanged();
      checkProperties();
      properties.putLongProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public Long getLongProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getLongProperty(key);
   }

   @Override
   public Long getLongProperty(final String key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return getLongProperty(SimpleString.toSimpleString(key));
   }

   @Override
   public CoreMessage putFloatProperty(final SimpleString key, final float value) {
      messageChanged();
      checkProperties();
      properties.putFloatProperty(key, value);
      return this;
   }

   @Override
   public CoreMessage putFloatProperty(final String key, final float value) {
      messageChanged();
      checkProperties();
      properties.putFloatProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public CoreMessage putDoubleProperty(final SimpleString key, final double value) {
      messageChanged();
      checkProperties();
      properties.putDoubleProperty(key, value);
      return this;
   }

   @Override
   public CoreMessage putDoubleProperty(final String key, final double value) {
      messageChanged();
      checkProperties();
      properties.putDoubleProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public Double getDoubleProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      messageChanged();
      checkProperties();
      return properties.getDoubleProperty(key);
   }

   @Override
   public Double getDoubleProperty(final String key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return getDoubleProperty(SimpleString.toSimpleString(key));
   }

   @Override
   public CoreMessage putStringProperty(final SimpleString key, final SimpleString value) {
      messageChanged();
      checkProperties();
      properties.putSimpleStringProperty(key, value);
      return this;
   }

   @Override
   public CoreMessage putStringProperty(final SimpleString key, final String value) {
      messageChanged();
      checkProperties();
      properties.putSimpleStringProperty(key, SimpleString.toSimpleString(value, getPropertyValuesPool()));
      return this;
   }


   @Override
   public CoreMessage putStringProperty(final String key, final String value) {
      messageChanged();
      checkProperties();
      properties.putSimpleStringProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), SimpleString.toSimpleString(value, getPropertyValuesPool()));
      return this;
   }

   @Override
   public CoreMessage putObjectProperty(final SimpleString key,
                                        final Object value) throws ActiveMQPropertyConversionException {
      checkProperties();
      messageChanged();
      TypedProperties.setObjectProperty(key, value, properties);
      return this;
   }

   @Override
   public Object getObjectProperty(final String key) {
      checkProperties();
      return getObjectProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public Object getObjectProperty(final SimpleString key) {
      checkProperties();
      return properties.getProperty(key);
   }

   @Override
   public CoreMessage putObjectProperty(final String key, final Object value) throws ActiveMQPropertyConversionException {
      messageChanged();
      putObjectProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()), value);
      return this;
   }

   @Override
   public Short getShortProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getShortProperty(key);
   }

   @Override
   public Short getShortProperty(final String key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getShortProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public Float getFloatProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getFloatProperty(key);
   }

   @Override
   public Float getFloatProperty(final String key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getFloatProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public String getStringProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      SimpleString str = getSimpleStringProperty(key);

      if (str == null) {
         return null;
      } else {
         return str.toString();
      }
   }

   @Override
   public String getStringProperty(final String key) throws ActiveMQPropertyConversionException {
      return getStringProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public SimpleString getSimpleStringProperty(final SimpleString key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getSimpleStringProperty(key);
   }

   @Override
   public SimpleString getSimpleStringProperty(final String key) throws ActiveMQPropertyConversionException {
      checkProperties();
      return properties.getSimpleStringProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public Object removeProperty(final SimpleString key) {
      checkProperties();
      Object oldValue = properties.removeProperty(key);
      if (oldValue != null) {
         messageChanged();
      }
      return oldValue;
   }

   @Override
   public Object removeProperty(final String key) {
      messageChanged();
      checkProperties();
      Object oldValue = properties.removeProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
      if (oldValue != null) {
         messageChanged();
      }
      return oldValue;
   }

   @Override
   public boolean containsProperty(final SimpleString key) {
      checkProperties();
      return properties.containsProperty(key);
   }

   @Override
   public boolean containsProperty(final String key) {
      checkProperties();
      return properties.containsProperty(SimpleString.toSimpleString(key, getPropertyKeysPool()));
   }

   @Override
   public Set<SimpleString> getPropertyNames() {
      checkProperties();
      return properties.getPropertyNames();
   }

   @Override
   public LargeBodyEncoder getBodyEncoder() throws ActiveMQException {
      return new DecodingContext();
   }

   private final class DecodingContext implements LargeBodyEncoder {

      private int lastPos = 0;

      private DecodingContext() {
      }

      @Override
      public void open() {
      }

      @Override
      public void close() {
      }

      @Override
      public long getLargeBodySize() {
         return buffer.writerIndex();
      }

      @Override
      public int encode(final ByteBuffer bufferRead) throws ActiveMQException {
         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(bufferRead);
         return encode(buffer, bufferRead.capacity());
      }

      @Override
      public int encode(final ActiveMQBuffer bufferOut, final int size) {
         bufferOut.byteBuf().writeBytes(buffer, lastPos, size);
         lastPos += size;
         return size;
      }
   }

   @Override
   public int getPersistSize() {
      checkEncode();
      return buffer.writerIndex() + DataConstants.SIZE_INT;
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      checkEncode();
      targetRecord.writeInt(buffer.writerIndex());
      targetRecord.writeBytes(buffer, 0, buffer.writerIndex());
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record) {
      int size = record.readInt();
      initBuffer(size);
      buffer.setIndex(0, 0).writeBytes(record.byteBuf(), size);
      decode(false);
   }

   @Override
   public CoreMessage toCore() {
      return this;
   }

   @Override
   public CoreMessage toCore(CoreMessageObjectPools coreMessageObjectPools) {
      return this;
   }

   @Override
   public String toString() {
      try {
         checkProperties();
         return "CoreMessage[messageID=" + messageID + ",durable=" + isDurable() + ",userID=" + getUserID() + ",priority=" + this.getPriority()  +
            ", timestamp=" + toDate(getTimestamp()) + ",expiration=" + toDate(getExpiration()) +
            ", durable=" + durable + ", address=" + getAddress() + ",size=" + getPersistentSize() + ",properties=" + properties + "]@" + System.identityHashCode(this);
      } catch (Throwable e) {
         logger.warn("Error creating String for message: ", e);
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

   private SimpleString.StringSimpleStringPool getPropertyKeysPool() {
      return coreMessageObjectPools == null ? null : coreMessageObjectPools.getPropertiesStringSimpleStringPools().getPropertyKeysPool();
   }

   private SimpleString.StringSimpleStringPool getPropertyValuesPool() {
      return coreMessageObjectPools == null ? null : coreMessageObjectPools.getPropertiesStringSimpleStringPools().getPropertyValuesPool();
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return getEncodeSize();
   }
}
