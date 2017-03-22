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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.RefCountMessage;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPConverter;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.utils.DataConstants;
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
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

// see https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format
public class AMQPMessage extends RefCountMessage {

   final long messageFormat;
   ByteBuf data;
   boolean bufferValid;
   Boolean durable;
   long messageID;
   String address;
   MessageImpl protonMessage;
   private volatile int memoryEstimate = -1;
   private long expiration = 0;
   // this is to store where to start sending bytes, ignoring header and delivery annotations.
   private int sendFrom = 0;
   private boolean parsedHeaders = false;
   private Header _header;
   private DeliveryAnnotations _deliveryAnnotations;
   private MessageAnnotations _messageAnnotations;
   private Properties _properties;
   private int appLocation = -1;
   private ApplicationProperties applicationProperties;
   private long scheduledTime = -1;
   private String connectionID;

   public AMQPMessage(long messageFormat, byte[] data) {
      this.data = Unpooled.wrappedBuffer(data);
      this.messageFormat = messageFormat;
      this.bufferValid = true;
      parseHeaders();

   }

   /** for persistence reload */
   public AMQPMessage(long messageFormat) {
      this.messageFormat = messageFormat;
      this.bufferValid = false;

   }

   public AMQPMessage(long messageFormat, Message message) {
      this.messageFormat = messageFormat;
      this.protonMessage = (MessageImpl) message;
      this.bufferValid = false;

   }

   public AMQPMessage(Message message) {
      this(0, message);
   }

   public MessageImpl getProtonMessage() {
      if (protonMessage == null) {
         protonMessage = (MessageImpl) Message.Factory.create();

         if (data != null) {
            data.readerIndex(0);
            protonMessage.decode(data.nioBuffer());
            this._header = protonMessage.getHeader();
            protonMessage.setHeader(null);
         }
      }

      return protonMessage;
   }

   private void initalizeObjects() {
      if (protonMessage == null) {
         if (data == null) {
            this.sendFrom = 0;
            _header = new Header();
            _deliveryAnnotations = new DeliveryAnnotations(new HashMap<>());
            _properties = new Properties();
            this.applicationProperties = new ApplicationProperties(new HashMap<>());
            this.protonMessage = (MessageImpl) Message.Factory.create();
            this.protonMessage.setApplicationProperties(applicationProperties);
            this.protonMessage.setDeliveryAnnotations(_deliveryAnnotations);
         }
      }
   }

   private Map getApplicationPropertiesMap() {
      ApplicationProperties appMap = getApplicationProperties();
      Map map = null;

      if (appMap != null) {
         map = appMap.getValue();
      }

      if (map == null) {
         return Collections.emptyMap();
      } else {
         return map;
      }
   }

   private ApplicationProperties getApplicationProperties() {
      parseHeaders();

      if (applicationProperties == null && appLocation >= 0) {
         ByteBuffer buffer = getBuffer().nioBuffer();
         buffer.position(appLocation);
         TLSEncode.getDecoder().setByteBuffer(buffer);
         Object section = TLSEncode.getDecoder().readObject();
         if (section instanceof ApplicationProperties) {
            this.applicationProperties = (ApplicationProperties) section;
         }
         this.appLocation = -1;
         TLSEncode.getDecoder().setByteBuffer(null);

      }

      return applicationProperties;
   }

   private void parseHeaders() {
      if (!parsedHeaders) {
         if (data == null) {
            initalizeObjects();
         } else {
            partialDecode(data.nioBuffer());
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
      Map mapAnnotations = annotations != null ? annotations.getValue() : null;
      if (mapAnnotations != null) {
         return mapAnnotations.get(symbol);
      }

      return null;
   }

   private Object removeSymbol(Symbol symbol) {
      MessageAnnotations annotations = getMessageAnnotations();
      Map mapAnnotations = annotations != null ? annotations.getValue() : null;
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
      Map mapAnnotations = annotations != null ? annotations.getValue() : null;
      if (mapAnnotations != null) {
         mapAnnotations.put(symbol, value);
      }
   }

   @Override
   public RoutingType getRouteType() {

      /* TODO-now How to use this properly
      switch (((Byte)type).byteValue()) {
         case AMQPMessageSupport.QUEUE_TYPE:
         case AMQPMessageSupport.TEMP_QUEUE_TYPE:
            return RoutingType.ANYCAST;

         case AMQPMessageSupport.TOPIC_TYPE:
         case AMQPMessageSupport.TEMP_TOPIC_TYPE:
            return RoutingType.MULTICAST;
         default:
            return null;
      } */

      return null;
   }

   @Override
   public SimpleString getGroupID() {
      parseHeaders();

      if (_properties != null && _properties.getGroupId() != null) {
         return SimpleString.toSimpleString(_properties.getGroupId());
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
      return AMQPMessagePersister.getInstance();
   }

   private synchronized void partialDecode(ByteBuffer buffer) {
      DecoderImpl decoder = TLSEncode.getDecoder();
      decoder.setByteBuffer(buffer);
      buffer.position(0);

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
            sendFrom = buffer.position();
            _header = (Header) section;

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
            sendFrom = 0;
         }
         if (section instanceof DeliveryAnnotations) {
            _deliveryAnnotations = (DeliveryAnnotations) section;
            sendFrom = buffer.position();

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

            if (_properties.getAbsoluteExpiryTime() != null) {
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
      }
   }

   public long getMessageFormat() {
      return messageFormat;
   }

   public int getLength() {
      return data.array().length;
   }

   public byte[] getArray() {
      return data.array();
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
         return Unpooled.wrappedBuffer(data);
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

      byte[] origin = data.array();
      byte[] newData = new byte[data.array().length - sendFrom];
      for (int i = 0; i < newData.length; i++) {
         newData[i] = origin[i + sendFrom];
      }
      AMQPMessage newEncode = new AMQPMessage(this.messageFormat, newData);
      newEncode.setDurable(isDurable());
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
         return this;
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

      if (getHeader() != null && getHeader().getDurable() != null) {
         durable =  getHeader().getDurable().booleanValue();
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
      if (address == null) {
         Properties properties = getProtonMessage().getProperties();
         if (properties != null) {
            return properties.getTo();
         } else {
            return null;
         }
      } else {
         return address;
      }
   }

   @Override
   public AMQPMessage setAddress(String address) {
      this.address = address;
      return this;
   }

   @Override
   public AMQPMessage setAddress(SimpleString address) {
      return setAddress(address.toString());
   }

   @Override
   public SimpleString getAddressSimpleString() {
      return SimpleString.toSimpleString(getAddress());
   }

   @Override
   public long getTimestamp() {
      return 0;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setTimestamp(long timestamp) {
      return null;
   }

   @Override
   public byte getPriority() {
      return 0;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setPriority(byte priority) {
      return null;
   }

   @Override
   public void receiveBuffer(ByteBuf buffer) {

   }

   private synchronized void checkBuffer() {
      if (!bufferValid) {
         ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1500);
         try {
            getProtonMessage().encode(new NettyWritable(buffer));
            byte[] bytes = new byte[buffer.writerIndex()];
            buffer.readBytes(bytes);
            this.data = Unpooled.wrappedBuffer(bytes);
         } finally {
            buffer.release();
         }
      }
   }

   @Override
   public int getEncodeSize() {
      checkBuffer();
      // + 20checkBuffer is an estimate for the Header with the deliveryCount
      return data.array().length - sendFrom + 20;
   }

   @Override
   public void sendBuffer(ByteBuf buffer, int deliveryCount) {
      checkBuffer();
      Header header = getHeader();
      if (header == null && deliveryCount > 0) {
         header = new Header();
         header.setDurable(durable);
      }
      if (header != null) {
         synchronized (header) {
            header.setDeliveryCount(UnsignedInteger.valueOf(deliveryCount - 1));
            TLSEncode.getEncoder().setByteBuffer(new NettyWritable(buffer));
            TLSEncode.getEncoder().writeObject(header);
            TLSEncode.getEncoder().setByteBuffer((WritableBuffer) null);
         }
      }
      buffer.writeBytes(data, sendFrom, data.writerIndex() - sendFrom);
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
      getApplicationPropertiesMap().put(key, Boolean.valueOf(value));
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
         return getProperties().getSubject();
      } else if (key.equals(MessageUtil.CONNECTION_ID_PROPERTY_NAME.toString())) {
         return getConnectionID();
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
      if (_deliveryAnnotations != null) getProtonMessage().setDeliveryAnnotations(_deliveryAnnotations);
      if (_messageAnnotations != null) getProtonMessage().setMessageAnnotations(_messageAnnotations);
      if (applicationProperties != null) getProtonMessage().setApplicationProperties(applicationProperties);
      if (_properties != null) getProtonMessage().setProperties(this._properties);
      bufferValid = false;
      checkBuffer();
   }



   @Override
   public SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException {
      return SimpleString.toSimpleString((String) getApplicationPropertiesMap().get(key));
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
   public Set<SimpleString> getPropertyNames() {
      HashSet<SimpleString> values = new HashSet<>();
      for (Object k : getApplicationPropertiesMap().keySet()) {
         values.add(SimpleString.toSimpleString(k.toString()));
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
   public ICoreMessage toCore() {
      try {
         return AMQPConverter.getInstance().toCore(this);
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
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
      return data.array().length - sendFrom;
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      checkBuffer();
      targetRecord.writeInt(internalPersistSize());
      targetRecord.writeBytes(data.array(), sendFrom, data.array().length - sendFrom);
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record) {
      int size = record.readInt();
      byte[] recordArray = new byte[size];
      record.readBytes(recordArray);
      this.sendFrom = 0; // whatever was persisted will be sent
      this.data = Unpooled.wrappedBuffer(recordArray);
      this.bufferValid = true;
      this.durable = true; // it's coming from the journal, so it's durable
      parseHeaders();
   }
}
