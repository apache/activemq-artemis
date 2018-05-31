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
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPConverter;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageIdHelper;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport;
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
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

// see https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format
public class AMQPMessage extends RefCountMessage {

   public static final SimpleString ADDRESS_PROPERTY = SimpleString.toSimpleString("_AMQ_AD");

   public static final int DEFAULT_MESSAGE_PRIORITY = 4;
   public static final int MAX_MESSAGE_PRIORITY = 9;

   final long messageFormat;
   ReadableBuffer data;
   boolean bufferValid;
   Boolean durable;
   long messageID;
   SimpleString address;
   MessageImpl protonMessage;
   private volatile int memoryEstimate = -1;
   private long expiration = 0;

   // Records where the Header section ends if present.
   private int headerEnds = 0;

   // Records where the message payload starts, ignoring DeliveryAnnotations if present
   private int messagePaylodStart = 0;

   private boolean parsedHeaders = false;
   private Header _header;
   private DeliveryAnnotations _deliveryAnnotations;
   private MessageAnnotations _messageAnnotations;
   private Properties _properties;
   private int appLocation = -1;
   private ApplicationProperties applicationProperties;
   private long scheduledTime = -1;
   private String connectionID;
   private final CoreMessageObjectPools coreMessageObjectPools;

   Set<Object> rejectedConsumers;

   /** These are properties set at the broker level..
    *  these are properties created by the broker only */
   private volatile TypedProperties extraProperties;

   public AMQPMessage(long messageFormat, byte[] data, TypedProperties extraProperties) {
      this(messageFormat, data, extraProperties, null);
   }

   public AMQPMessage(long messageFormat, byte[] data, TypedProperties extraProperties, CoreMessageObjectPools coreMessageObjectPools) {
      this(messageFormat, ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.wrap(data)), extraProperties, coreMessageObjectPools);
   }

   public AMQPMessage(long messageFormat, ReadableBuffer data, TypedProperties extraProperties, CoreMessageObjectPools coreMessageObjectPools) {
      this.data = data;
      this.messageFormat = messageFormat;
      this.bufferValid = true;
      this.coreMessageObjectPools = coreMessageObjectPools;
      this.extraProperties = extraProperties == null ? null : new TypedProperties(extraProperties);
      parseHeaders();
   }

   /** for persistence reload */
   public AMQPMessage(long messageFormat) {
      this.messageFormat = messageFormat;
      this.bufferValid = false;
      this.coreMessageObjectPools = null;
   }

   public AMQPMessage(long messageFormat, Message message) {
      this.messageFormat = messageFormat;
      this.protonMessage = (MessageImpl) message;
      this.bufferValid = false;
      this.coreMessageObjectPools = null;
   }

   public AMQPMessage(Message message) {
      this(0, message);
   }

   public MessageImpl getProtonMessage() {
      if (protonMessage == null) {
         protonMessage = (MessageImpl) Message.Factory.create();

         if (data != null) {
            data.rewind();
            protonMessage.decode(data.duplicate());
            this._header = protonMessage.getHeader();
            protonMessage.setHeader(null);
         }
      }

      return protonMessage;
   }

   private void initalizeObjects() {
      if (protonMessage == null) {
         if (data == null) {
            headerEnds = 0;
            messagePaylodStart = 0;
            _header = new Header();
            _deliveryAnnotations = new DeliveryAnnotations(new HashMap<>());
            _properties = new Properties();
            applicationProperties = new ApplicationProperties(new HashMap<>());
            protonMessage = (MessageImpl) Message.Factory.create();
            protonMessage.setApplicationProperties(applicationProperties);
            protonMessage.setDeliveryAnnotations(_deliveryAnnotations);
         }
      }
   }

   private Map<String, Object> getApplicationPropertiesMap() {
      ApplicationProperties appMap = getApplicationProperties();
      Map<String, Object> map = null;

      if (appMap != null) {
         map = appMap.getValue();
      }

      if (map == null) {
         map = new HashMap<>();
         this.applicationProperties = new ApplicationProperties(map);
      }

      return map;
   }

   private ApplicationProperties getApplicationProperties() {
      parseHeaders();

      if (applicationProperties == null && appLocation >= 0) {
         ReadableBuffer buffer = data.duplicate();
         buffer.position(appLocation);
         TLSEncode.getDecoder().setBuffer(buffer);
         Object section = TLSEncode.getDecoder().readObject();
         if (section instanceof ApplicationProperties) {
            this.applicationProperties = (ApplicationProperties) section;
         }
         this.appLocation = -1;
         TLSEncode.getDecoder().setBuffer(null);
      }

      return applicationProperties;
   }

   private synchronized void parseHeaders() {
      if (!parsedHeaders) {
         if (data == null) {
            initalizeObjects();
         } else {
            partialDecode(data);
         }
         parsedHeaders = true;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setConnectionID(String connectionID) {
      this.connectionID = connectionID;
      return this;
   }

   @Override
   public String getConnectionID() {
      return connectionID;
   }

   public MessageAnnotations getMessageAnnotations() {
      parseHeaders();
      return _messageAnnotations;
   }

   public Header getHeader() {
      parseHeaders();
      return _header;
   }

   public Properties getProperties() {
      parseHeaders();
      return _properties;
   }

   private Object getSymbol(String symbol) {
      return getSymbol(Symbol.getSymbol(symbol));
   }

   private Object getSymbol(Symbol symbol) {
      MessageAnnotations annotations = getMessageAnnotations();
      Map<Symbol, Object> mapAnnotations = annotations != null ? annotations.getValue() : null;
      if (mapAnnotations != null) {
         return mapAnnotations.get(symbol);
      }

      return null;
   }

   private Object removeSymbol(Symbol symbol) {
      MessageAnnotations annotations = getMessageAnnotations();
      Map<Symbol, Object> mapAnnotations = annotations != null ? annotations.getValue() : null;
      if (mapAnnotations != null) {
         return mapAnnotations.remove(symbol);
      }

      return null;
   }

   private void setSymbol(String symbol, Object value) {
      setSymbol(Symbol.getSymbol(symbol), value);
   }

   private void setSymbol(Symbol symbol, Object value) {
      MessageAnnotations annotations = getMessageAnnotations();
      if (annotations == null) {
         _messageAnnotations = new MessageAnnotations(new HashMap<>());
         annotations = _messageAnnotations;
      }
      Map<Symbol, Object> mapAnnotations = annotations != null ? annotations.getValue() : null;
      if (mapAnnotations != null) {
         mapAnnotations.put(symbol, value);
      }
   }

   @Override
   public RoutingType getRoutingType() {
      Object routingType = getSymbol(AMQPMessageSupport.ROUTING_TYPE);

      if (routingType != null) {
         return RoutingType.getType((byte) routingType);
      } else {
         routingType = getSymbol(AMQPMessageSupport.JMS_DEST_TYPE_MSG_ANNOTATION);
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
      parseHeaders();
      if (routingType == null) {
         removeSymbol(AMQPMessageSupport.ROUTING_TYPE);
      } else {
         setSymbol(AMQPMessageSupport.ROUTING_TYPE, routingType.getType());
      }
      return this;
   }

   @Override
   public SimpleString getGroupID() {
      parseHeaders();

      if (_properties != null && _properties.getGroupId() != null) {
         return SimpleString.toSimpleString(_properties.getGroupId(), coreMessageObjectPools == null ? null : coreMessageObjectPools.getGroupIdStringSimpleStringPool());
      } else {
         return null;
      }
   }

   @Override
   public Long getScheduledDeliveryTime() {

      if (scheduledTime < 0) {
         Object objscheduledTime = getSymbol("x-opt-delivery-time");
         Object objdelay = getSymbol("x-opt-delivery-delay");

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
      parseHeaders();
      setSymbol(AMQPMessageSupport.JMS_DELIVERY_TIME, time);
      return this;
   }

   @Override
   public Persister<org.apache.activemq.artemis.api.core.Message> getPersister() {
      return AMQPMessagePersisterV2.getInstance();
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

   private synchronized void partialDecode(ReadableBuffer buffer) {
      DecoderImpl decoder = TLSEncode.getDecoder();
      decoder.setBuffer(buffer.rewind());

      _header = null;
      _deliveryAnnotations = null;
      _messageAnnotations = null;
      _properties = null;
      applicationProperties = null;
      Section section = null;

      try {
         if (buffer.hasRemaining()) {
            section = (Section) decoder.readObject();
         }

         if (section instanceof Header) {
            _header = (Header) section;
            headerEnds = buffer.position();
            messagePaylodStart = headerEnds;
            this.durable = _header.getDurable();

            if (_header.getTtl() != null) {
               this.expiration = System.currentTimeMillis() + _header.getTtl().intValue();
            }

            if (buffer.hasRemaining()) {
               section = (Section) decoder.readObject();
            } else {
               section = null;
            }

         } else {
            // meaning there is no header
            headerEnds = 0;
         }
         if (section instanceof DeliveryAnnotations) {
            _deliveryAnnotations = (DeliveryAnnotations) section;

            // Advance the start beyond the delivery annotations so they are not written
            // out on send of the message.
            messagePaylodStart = buffer.position();

            if (buffer.hasRemaining()) {
               section = (Section) decoder.readObject();
            } else {
               section = null;
            }
         }
         if (section instanceof MessageAnnotations) {
            _messageAnnotations = (MessageAnnotations) section;

            if (buffer.hasRemaining()) {
               section = (Section) decoder.readObject();
            } else {
               section = null;
            }
         }
         if (section instanceof Properties) {
            _properties = (Properties) section;

            if (_properties.getAbsoluteExpiryTime() != null && _properties.getAbsoluteExpiryTime().getTime() > 0) {
               this.expiration = _properties.getAbsoluteExpiryTime().getTime();
            }

            // We don't read the next section on purpose, as we will parse ApplicationProperties
            // lazily
            section = null;
         }

         if (section instanceof ApplicationProperties) {
            applicationProperties = (ApplicationProperties) section;
         } else {
            if (buffer.hasRemaining()) {
               this.appLocation = buffer.position();
            } else {
               this.appLocation = -1;
            }
         }
      } finally {
         decoder.setByteBuffer(null);
         data.position(0);
      }
   }

   public long getMessageFormat() {
      return messageFormat;
   }

   @Override
   public void messageChanged() {
      bufferValid = false;
      this.data = null;
   }

   @Override
   public ByteBuf getBuffer() {
      if (data == null) {
         return null;
      } else {
         return Unpooled.wrappedBuffer(data.byteBuffer());
      }
   }

   @Override
   public AMQPMessage setBuffer(ByteBuf buffer) {
      this.data = null;
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message copy() {
      checkBuffer();

      ReadableBuffer view = data.duplicate();

      byte[] newData = new byte[view.remaining() - (messagePaylodStart - headerEnds)];

      view.position(0).limit(headerEnds);
      view.get(newData, 0, headerEnds);
      view.clear();
      view.position(messagePaylodStart);
      view.get(newData, headerEnds, view.remaining());

      AMQPMessage newEncode = new AMQPMessage(this.messageFormat, newData, extraProperties, coreMessageObjectPools);
      newEncode.setDurable(isDurable()).setMessageID(this.getMessageID());
      return newEncode;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message copy(long newID) {
      checkBuffer();
      return copy().setMessageID(newID);
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

      Properties properties = getProperties();

      if (properties != null) {
         if (expiration <= 0) {
            properties.setAbsoluteExpiryTime(null);
         } else {
            properties.setAbsoluteExpiryTime(new Date(expiration));
         }
      }
      this.expiration = expiration;
      return this;
   }

   @Override
   public Object getUserID() {
      Properties properties = getProperties();
      if (properties != null && properties.getMessageId() != null) {
         return properties.getMessageId();
      } else {
         return null;
      }
   }

   /**
    * Before we added AMQP into Artemis / Hornetq, the name getUserID was already taken by JMSMessageID.
    * We cannot simply change the names now as it would break the API for existing clients.
    *
    * This is to return and read the proper AMQP userID.
    * @return
    */
   public Object getAMQPUserID() {
      Properties properties = getProperties();
      if (properties != null && properties.getUserId() != null) {
         Binary binary = properties.getUserId();
         return new String(binary.getArray(), binary.getArrayOffset(), binary.getLength(), StandardCharsets.UTF_8);
      } else {
         return null;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setUserID(Object userID) {
      return null;
   }

   @Override
   public boolean isDurable() {
      if (durable != null) {
         return durable;
      }

      parseHeaders();

      if (getHeader() != null && getHeader().getDurable() != null) {
         durable = getHeader().getDurable();
         return durable;
      } else {
         return durable != null ? durable : false;
      }
   }

   @Override
   public Object getDuplicateProperty() {
      return null;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setDurable(boolean durable) {
      this.durable = durable;
      return this;
   }

   @Override
   public String getAddress() {
      SimpleString addressSimpleString = getAddressSimpleString();
      return addressSimpleString == null ? null : addressSimpleString.toString();
   }


   public SimpleString cachedAddressSimpleString(String address) {
      return CoreMessageObjectPools.cachedAddressSimpleString(address, coreMessageObjectPools);
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
            Properties properties = getProperties();
            if (properties != null && properties.getTo() != null) {
               address = cachedAddressSimpleString(properties.getTo());
            }
         }
      }
      return address;
   }

   @Override
   public long getTimestamp() {
      if (getProperties() != null && getProperties().getCreationTime() != null) {
         return getProperties().getCreationTime().getTime();
      } else {
         return 0L;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setTimestamp(long timestamp) {
      getProperties().setCreationTime(new Date(timestamp));
      return this;
   }

   @Override
   public byte getPriority() {
      if (getHeader() != null && getHeader().getPriority() != null) {
         return (byte) Math.min(getHeader().getPriority().intValue(), MAX_MESSAGE_PRIORITY);
      } else {
         return DEFAULT_MESSAGE_PRIORITY;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setPriority(byte priority) {
      getHeader().setPriority(UnsignedByte.valueOf(priority));
      return this;
   }

   @Override
   public void receiveBuffer(ByteBuf buffer) {

   }

   private synchronized void checkBuffer() {
      if (!bufferValid) {
         encodeProtonMessage();
      }
   }

   private void encodeProtonMessage() {
      int estimated = Math.max(1500, data != null ? data.capacity() + 1000 : 0);
      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(estimated);
      try {
         getProtonMessage().encode(new NettyWritable(buffer));
         byte[] bytes = new byte[buffer.writerIndex()];
         buffer.readBytes(bytes);
         this.data = ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.wrap(bytes));
      } finally {
         buffer.release();
      }
   }

   @Override
   public int getEncodeSize() {
      checkBuffer();
      // + 20checkBuffer is an estimate for the Header with the deliveryCount
      return data.remaining() - messagePaylodStart + 20;
   }

   @Override
   public void sendBuffer(ByteBuf buffer, int deliveryCount) {
      checkBuffer();

      int amqpDeliveryCount = deliveryCount - 1;

      // If the re-delivering the message then the header must be re-encoded
      // otherwise we want to write the original header if present.
      if (amqpDeliveryCount > 0) {

         Header header = getHeader();
         if (header == null) {
            header = new Header();
            header.setDurable(durable);
         }

         synchronized (header) {
            header.setDeliveryCount(UnsignedInteger.valueOf(amqpDeliveryCount));
            TLSEncode.getEncoder().setByteBuffer(new NettyWritable(buffer));
            TLSEncode.getEncoder().writeObject(header);
            TLSEncode.getEncoder().setByteBuffer((WritableBuffer) null);
         }
      } else if (headerEnds > 0) {
         buffer.writeBytes(data.duplicate().limit(headerEnds).byteBuffer());
      }

      data.position(messagePaylodStart);
      buffer.writeBytes(data.byteBuffer());
      data.position(0);
   }

   /**
    * Gets a ByteBuf from the Message that contains the encoded bytes to be sent on the wire.
    * <p>
    * When possible this method will present the bytes to the caller without copying them into
    * another buffer copy.  If copying is needed a new Netty buffer is created and returned. The
    * caller should ensure that the reference count on the returned buffer is always decremented
    * to avoid a leak in the case of a copied buffer being returned.
    *
    * @param deliveryCount
    *       The new delivery count for this message.
    *
    * @return a Netty ByteBuf containing the encoded bytes of this Message instance.
    */
   public ReadableBuffer getSendBuffer(int deliveryCount) {
      checkBuffer();

      if (deliveryCount > 1) {
         return createCopyWithNewDeliveryCount(deliveryCount);
      } else if (headerEnds != messagePaylodStart) {
         return createCopyWithoutDeliveryAnnotations();
      } else {
         // Common case message has no delivery annotations and this is the first delivery
         // so no re-encoding or section skipping needed.
         return data.duplicate();
      }
   }

   private ReadableBuffer createCopyWithoutDeliveryAnnotations() {
      assert headerEnds != messagePaylodStart;

      // The original message had delivery annotations and so we must copy into a new
      // buffer skipping the delivery annotations section as that is not meant to survive
      // beyond this hop.
      ReadableBuffer duplicate = data.duplicate();

      final ByteBuf result = PooledByteBufAllocator.DEFAULT.heapBuffer(getEncodeSize());
      result.writeBytes(duplicate.limit(headerEnds).byteBuffer());
      duplicate.clear();
      duplicate.position(messagePaylodStart);
      result.writeBytes(duplicate.byteBuffer());

      return new NettyReadable(result);
   }

   private ReadableBuffer createCopyWithNewDeliveryCount(int deliveryCount) {
      assert deliveryCount > 1;

      final int amqpDeliveryCount = deliveryCount - 1;
      // If the re-delivering the message then the header must be re-encoded
      // (or created if not previously present).  Any delivery annotations should
      // be skipped as well in the resulting buffer.

      final ByteBuf result = PooledByteBufAllocator.DEFAULT.heapBuffer(getEncodeSize());

      Header header = getHeader();
      if (header == null) {
         header = new Header();
         header.setDurable(durable);
      }

      synchronized (header) {
         // Updates or adds a Header section with the correct delivery count
         header.setDeliveryCount(UnsignedInteger.valueOf(amqpDeliveryCount));
         TLSEncode.getEncoder().setByteBuffer(new NettyWritable(result));
         TLSEncode.getEncoder().writeObject(header);
         TLSEncode.getEncoder().setByteBuffer((WritableBuffer) null);
      }

      // This will skip any existing delivery annotations that might have been present
      // in the original message.
      data.position(messagePaylodStart);
      result.writeBytes(data.byteBuffer());
      data.position(0);

      return new NettyReadable(result);
   }

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

   @Override
   public org.apache.activemq.artemis.api.core.Message putBooleanProperty(String key, boolean value) {
      getApplicationPropertiesMap().put(key, Boolean.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putByteProperty(String key, byte value) {
      getApplicationPropertiesMap().put(key, Byte.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putBytesProperty(String key, byte[] value) {
      getApplicationPropertiesMap().put(key, value);
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putShortProperty(String key, short value) {
      getApplicationPropertiesMap().put(key, Short.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putCharProperty(String key, char value) {
      getApplicationPropertiesMap().put(key, Character.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putIntProperty(String key, int value) {
      getApplicationPropertiesMap().put(key, Integer.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putLongProperty(String key, long value) {
      getApplicationPropertiesMap().put(key, Long.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putFloatProperty(String key, float value) {
      getApplicationPropertiesMap().put(key, Float.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putDoubleProperty(String key, double value) {
      getApplicationPropertiesMap().put(key, Double.valueOf(value));
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putBooleanProperty(SimpleString key, boolean value) {
      getApplicationPropertiesMap().put(key.toString(), Boolean.valueOf(value));
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
      getApplicationPropertiesMap().put(key, value);
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putObjectProperty(String key,
                                                                         Object value) throws ActiveMQPropertyConversionException {
      getApplicationPropertiesMap().put(key, value);
      return this;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putObjectProperty(SimpleString key,
                                                                         Object value) throws ActiveMQPropertyConversionException {
      return putObjectProperty(key.toString(), value);
   }

   @Override
   public Object removeProperty(String key) {
      return getApplicationPropertiesMap().remove(key);
   }

   @Override
   public boolean containsProperty(String key) {
      return getApplicationPropertiesMap().containsKey(key);
   }

   @Override
   public Boolean getBooleanProperty(String key) throws ActiveMQPropertyConversionException {
      return (Boolean) getApplicationPropertiesMap().get(key);
   }

   @Override
   public Byte getByteProperty(String key) throws ActiveMQPropertyConversionException {
      return (Byte) getApplicationPropertiesMap().get(key);
   }

   @Override
   public Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException {
      return (Double) getApplicationPropertiesMap().get(key);
   }

   @Override
   public Integer getIntProperty(String key) throws ActiveMQPropertyConversionException {
      return (Integer) getApplicationPropertiesMap().get(key);
   }

   @Override
   public Long getLongProperty(String key) throws ActiveMQPropertyConversionException {
      return (Long) getApplicationPropertiesMap().get(key);
   }

   @Override
   public Object getObjectProperty(String key) {
      if (key.equals(MessageUtil.TYPE_HEADER_NAME.toString())) {
         if (getProperties() != null) {
            return getProperties().getSubject();
         }
      } else if (key.equals(MessageUtil.CONNECTION_ID_PROPERTY_NAME.toString())) {
         return getConnectionID();
      } else if (key.equals(MessageUtil.JMSXGROUPID)) {
         return getGroupID();
      } else if (key.equals(MessageUtil.JMSXUSERID)) {
         return getAMQPUserID();
      } else if (key.equals(MessageUtil.CORRELATIONID_HEADER_NAME.toString())) {
         if (getProperties() != null && getProperties().getCorrelationId() != null) {
            return AMQPMessageIdHelper.INSTANCE.toCorrelationIdString(getProperties().getCorrelationId());
         }
      } else {
         Object value = getApplicationPropertiesMap().get(key);
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
      return (Short) getApplicationPropertiesMap().get(key);
   }

   @Override
   public Float getFloatProperty(String key) throws ActiveMQPropertyConversionException {
      return (Float) getApplicationPropertiesMap().get(key);
   }

   @Override
   public String getStringProperty(String key) throws ActiveMQPropertyConversionException {
      if (key.equals(MessageUtil.TYPE_HEADER_NAME.toString())) {
         return getProperties().getSubject();
      } else if (key.equals(MessageUtil.CONNECTION_ID_PROPERTY_NAME.toString())) {
         return getConnectionID();
      } else {
         return (String) getApplicationPropertiesMap().get(key);
      }
   }

   @Override
   public Object removeAnnotation(SimpleString key) {
      return removeSymbol(Symbol.getSymbol(key.toString()));
   }

   @Override
   public Object getAnnotation(SimpleString key) {
      return getSymbol(key.toString());
   }

   @Override
   public AMQPMessage setAnnotation(SimpleString key, Object value) {
      setSymbol(key.toString(), value);
      return this;
   }

   @Override
   public void reencode() {
      parseHeaders();
      getApplicationProperties();
      if (_header != null) getProtonMessage().setHeader(_header);
      if (_deliveryAnnotations != null) getProtonMessage().setDeliveryAnnotations(_deliveryAnnotations);
      if (_messageAnnotations != null) getProtonMessage().setMessageAnnotations(_messageAnnotations);
      if (applicationProperties != null) getProtonMessage().setApplicationProperties(applicationProperties);
      if (_properties != null) {
         if (address != null) {
            _properties.setTo(address.toString());
         }
         getProtonMessage().setProperties(this._properties);
      }
      bufferValid = false;
      checkBuffer();
   }

   @Override
   public SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException {
      return SimpleString.toSimpleString((String) getApplicationPropertiesMap().get(key), getPropertyValuesPool());
   }

   @Override
   public byte[] getBytesProperty(String key) throws ActiveMQPropertyConversionException {
      return (byte[]) getApplicationPropertiesMap().get(key);
   }

   @Override
   public Object removeProperty(SimpleString key) {
      return removeProperty(key.toString());
   }

   @Override
   public boolean containsProperty(SimpleString key) {
      return containsProperty(key.toString());
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
   public org.apache.activemq.artemis.api.core.Message putStringProperty(SimpleString key, SimpleString value) {
      return putStringProperty(key.toString(), value.toString());
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message putStringProperty(SimpleString key, String value) {
      return putStringProperty(key.toString(), value);
   }

   @Override
   public Set<SimpleString> getPropertyNames() {
      HashSet<SimpleString> values = new HashSet<>();
      for (Object k : getApplicationPropertiesMap().keySet()) {
         values.add(SimpleString.toSimpleString(k.toString(), getPropertyKeysPool()));
      }
      return values;
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
         return AMQPConverter.getInstance().toCore(this, coreMessageObjectPools);
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   public ICoreMessage toCore() {
      return toCore(null);
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
   public SimpleString getReplyTo() {
      if (getProperties() != null) {
         return SimpleString.toSimpleString(getProperties().getReplyTo());
      } else {
         return null;
      }
   }

   @Override
   public AMQPMessage setReplyTo(SimpleString address) {
      if (getProperties() != null) {
         getProperties().setReplyTo(address != null ? address.toString() : null);
      }
      return this;
   }

   @Override
   public int getPersistSize() {
      checkBuffer();
      return DataConstants.SIZE_INT + internalPersistSize();
   }

   private int internalPersistSize() {
      return data.remaining();
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      checkBuffer();
      targetRecord.writeInt(internalPersistSize());
      if (data.hasArray()) {
         targetRecord.writeBytes(data.array(), data.arrayOffset(), data.remaining());
      } else {
         targetRecord.writeBytes(data.byteBuffer());
      }
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record) {
      int size = record.readInt();
      byte[] recordArray = new byte[size];
      record.readBytes(recordArray);
      this.messagePaylodStart = 0; // whatever was persisted will be sent
      this.data = ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.wrap(recordArray));
      this.bufferValid = true;
      this.durable = true; // it's coming from the journal, so it's durable
      parseHeaders();
   }

   @Override
   public String toString() {
      return "AMQPMessage [durable=" + isDurable() +
         ", messageID=" + getMessageID() +
         ", address=" + getAddress() +
         ", size=" + getEncodeSize() +
         ", applicationProperties=" + getApplicationProperties() +
         ", properties=" + getProperties() +
         ", extraProperties = " + getExtraProperties() +
         "]";
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
