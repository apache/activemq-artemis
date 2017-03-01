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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.RefCountMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.encode.BodyType;
import org.apache.activemq.artemis.core.message.LargeBodyEncoder;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;

// see https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format
public class AMQPMessage extends RefCountMessage {

   final long messageFormat;
   ByteBuf data;
   boolean bufferValid;
   byte type;
   long messageID;
   String address;
   MessageImpl protonMessage;
   private volatile int memoryEstimate = -1;
   private ProtonProtocolManager protocolManager;
   private long expiration = 0;
   // this can be used to encode the header again and the rest of the message buffer
   private int headerEnd = -1;
   private Header _header;
   private DeliveryAnnotations _deliveryAnnotations;
   private MessageAnnotations _messageAnnotations;
   private Properties _properties;
   private ApplicationProperties applicationProperties;

   public AMQPMessage(long messageFormat, byte[] data, ProtonProtocolManager protocolManager) {
      this.protocolManager = protocolManager;
      this.data = Unpooled.wrappedBuffer(data);
      this.messageFormat = messageFormat;
      this.bufferValid = true;

   }

   /** for persistence reload */
   public AMQPMessage(long messageFormat) {
      this.messageFormat = messageFormat;
      this.bufferValid = false;

   }

   public AMQPMessage(long messageFormat, Message message, ProtonProtocolManager protocolManager) {
      this.protocolManager = protocolManager;
      this.protonMessage = (MessageImpl)message;
      this.messageFormat = messageFormat;

   }

   public AMQPMessage(Message message, ProtonProtocolManager protocolManager) {
      this(0, message, protocolManager);
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
            this.headerEnd = -1;
            _header = new Header();
            _deliveryAnnotations = new DeliveryAnnotations(new HashMap<>());
            _properties = new Properties();
            this.applicationProperties = new ApplicationProperties(new HashMap<>());
            this.protonMessage = (MessageImpl)Message.Factory.create();
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
      if (applicationProperties == null) {
         if (data != null) {
            partialDecode(data.nioBuffer(), true);
         } else {
            initalizeObjects();
         }
      }

      return applicationProperties;
   }

   public Header getHeader() {
      if (_header == null) {
         if (data == null) {
            initalizeObjects();
         } else {
            partialDecode(this.data.nioBuffer(), false);
         }
      }

      return _header;
   }

   public Properties getProperties() {
      if (_properties == null) {
         if (data == null) {
            initalizeObjects();
         } else {
            partialDecode(this.data.nioBuffer(), true);
         }
      }

      return _properties;
   }

   @Override
   public Persister<org.apache.activemq.artemis.api.core.Message> getPersister() {
      return AMQPMessagePersister.getInstance();
   }

   private synchronized void partialDecode(ByteBuffer buffer, boolean readApplicationProperties) {
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
            headerEnd = buffer.position();
            _header = (Header) section;

            if (_header.getTtl() != null) {
               this.expiration = System.currentTimeMillis() + _header.getTtl().intValue();
            }

            if (!readApplicationProperties) {
               return;
            }

            if (buffer.hasRemaining() && readApplicationProperties) {
               section = (Section) decoder.readObject();
            } else {
               section = null;
            }
         } else {
            // meaning there is no header
            headerEnd = 0;
         }

         if (!readApplicationProperties) {
            return;
         }
         if (section instanceof DeliveryAnnotations) {
            _deliveryAnnotations = (DeliveryAnnotations) section;

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

            if (buffer.hasRemaining()) {
               section = (Section) decoder.readObject();
            } else {
               section = null;
            }

         }
         if (section instanceof ApplicationProperties) {
            applicationProperties = (ApplicationProperties) section;
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
   public ActiveMQBuffer getBodyBuffer() {
      // NO-IMPL
      return null;
   }

   @Override
   public ActiveMQBuffer getReadOnlyBodyBuffer() {
      // NO-IMPL
      return null;
   }

   @Override
   public LargeBodyEncoder getBodyEncoder() throws ActiveMQException {
      // NO-IMPL
      return null;
   }

   @Override
   public byte getType() {
      return type;
   }

   @Override
   public AMQPMessage setType(byte type) {
      this.type = type;
      return this;
   }

   @Override
   public boolean isLargeMessage() {
      return false;
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
      AMQPMessage newEncode = new AMQPMessage(this.messageFormat, data.array(), protocolManager);
      return newEncode;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message copy(long newID) {
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
      this.expiration = expiration;
      return this;
   }

   @Override
   public Object getUserID() {
      return getProperties().getMessageId();
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setUserID(Object userID) {
      return null;
   }

   @Override
   public void copyHeadersAndProperties(org.apache.activemq.artemis.api.core.Message msg) {

   }

   @Override
   public boolean isDurable() {
      if (getHeader() != null) {
         return getHeader().getDurable().booleanValue();
      } else {
         return false;
      }
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setDurable(boolean durable) {
      return null;
   }

   @Override
   public Object getProtocol() {
      return protocolManager;
   }

   @Override
   public AMQPMessage setProtocol(Object protocol) {
      this.protocolManager = (ProtonProtocolManager)protocol;
      return this;
   }

   @Override
   public Object getBody() {
      return null;
   }

   @Override
   public BodyType getBodyType() {
      return null;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message setBody(BodyType type, Object body) {
      return null;
   }

   @Override
   public String getAddress() {
      if (address == null) {
         Properties properties = getProtonMessage().getProperties();
         if (properties != null) {
            return  properties.getTo();
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
   public void sendBuffer(ByteBuf buffer, int deliveryCount) {
      checkBuffer();
      Header header = getHeader();
      if (header != null) {
         synchronized (header) {
            if (header.getDeliveryCount() != null) {
               header.setDeliveryCount(UnsignedInteger.valueOf(header.getDeliveryCount().intValue() + 1));
            } else {
               header.setDeliveryCount(UnsignedInteger.valueOf(1));
            }
            TLSEncode.getEncoder().setByteBuffer(new NettyWritable(buffer));
            TLSEncode.getEncoder().writeObject(header);
         }
      }
      buffer.writeBytes(data, headerEnd, data.writerIndex() - headerEnd);
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
      return (Boolean)getApplicationPropertiesMap().get(key);
   }

   @Override
   public Byte getByteProperty(String key) throws ActiveMQPropertyConversionException {
      return (Byte)getApplicationPropertiesMap().get(key);
   }

   @Override
   public Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException {
      return (Double)getApplicationPropertiesMap().get(key);
   }

   @Override
   public Integer getIntProperty(String key) throws ActiveMQPropertyConversionException {
      return (Integer)getApplicationPropertiesMap().get(key);
   }

   @Override
   public Long getLongProperty(String key) throws ActiveMQPropertyConversionException {
      return (Long)getApplicationPropertiesMap().get(key);
   }

   @Override
   public Object getObjectProperty(String key) {
      return getApplicationPropertiesMap().get(key);
   }

   @Override
   public Short getShortProperty(String key) throws ActiveMQPropertyConversionException {
      return (Short)getApplicationPropertiesMap().get(key);
   }

   @Override
   public Float getFloatProperty(String key) throws ActiveMQPropertyConversionException {
      return (Float)getApplicationPropertiesMap().get(key);
   }

   @Override
   public String getStringProperty(String key) throws ActiveMQPropertyConversionException {
      return (String)getApplicationPropertiesMap().get(key);
   }

   @Override
   public SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException {
      return SimpleString.toSimpleString((String)getApplicationPropertiesMap().get(key));
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
   public int getEncodeSize() {
      return 0;
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
         memoryEstimate = memoryOffset +
            (data != null ? data.capacity() : 0);
      }

      return memoryEstimate;
   }

   @Override
   public org.apache.activemq.artemis.api.core.Message toCore() {
      MessageImpl protonMessage = getProtonMessage();
      return null;
   }

   @Override
   public int getPersistSize() {
      checkBuffer();
      return data.array().length + DataConstants.SIZE_INT;
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      checkBuffer();
      targetRecord.writeInt(data.array().length);
      targetRecord.writeBytes(data.array());
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record) {
      int size = record.readInt();
      byte[] recordArray = new byte[size];
      record.readBytes(recordArray);
      this.data = Unpooled.wrappedBuffer(recordArray);
      this.bufferValid = true;
   }
}
