/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.openwire;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RefCountMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.SimpleString.StringSimpleStringPool;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.message.impl.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

public class OpenWireMessage extends RefCountMessage {

   private org.apache.activemq.command.ActiveMQMessage message;
   private WireFormat wireFormat;

   public OpenWireMessage(org.apache.activemq.command.ActiveMQMessage message, WireFormat wireFormat) {
      this.message = message;
      this.wireFormat = wireFormat;
   }

   public OpenWireMessage() {
      if (wireFormat == null) {
         wireFormat = new OpenWireFormat();
      }
   }

   public org.apache.activemq.command.ActiveMQMessage getAMQMessage() {
      return message;
   }

   public void setMarsheller(WireFormat wireFormat) {
      this.wireFormat = wireFormat;
   }

   @Override
   public void messageChanged() {
   }

   @Override
   public Long getScheduledDeliveryTime() {
      return message.getArrival();
   }

   @Override
   public SimpleString getReplyTo() {
      return SimpleString.toSimpleString(message.getReplyTo().getPhysicalName());
   }

   @Override
   public Message setReplyTo(SimpleString address) {
      message.setReplyTo(ActiveMQDestination.createDestination(address.toString(), ActiveMQDestination.QUEUE_TYPE));
      return this;
   }

   @Override
   public Message setBuffer(ByteBuf buffer) {
      try {
         message = (ActiveMQMessage) wireFormat.unmarshal(new ChannelBufferWrapper(buffer));
      } catch (IOException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public ByteBuf getBuffer() {
      try {
         ByteSequence byteSequence = wireFormat.marshal(message);
         return Unpooled.copiedBuffer(byteSequence.getData(), byteSequence.getOffset(), byteSequence.getLength());
      } catch (IOException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Message copy() {
      return new OpenWireMessage((ActiveMQMessage) message.copy(), wireFormat);
   }

   @Override
   public Message copy(long newID) {
      OpenWireMessage copy = new OpenWireMessage((ActiveMQMessage) message.copy(), wireFormat);
      copy.setMessageID(newID);
      return copy;
   }

   @Override
   public long getMessageID() {
      return message.getMessageId().getBrokerSequenceId();
   }

   @Override
   public Message setMessageID(long id) {
      message.getMessageId().setBrokerSequenceId(id);
      return this;
   }

   @Override
   public long getExpiration() {
      return message.getExpiration();
   }

   @Override
   public Message setExpiration(long expiration) {
      message.setExpiration(expiration);
      return this;
   }

   @Override
   public Object getUserID() {
      return message.getUserID();
   }

   @Override
   public Message setUserID(Object userID) {
      message.setUserID(userID.toString());
      return this;
   }

   @Override
   public boolean isDurable() {
      return message.isPersistent();
   }

   @Override
   public Message setDurable(boolean durable) {
      message.setPersistent(durable);
      return this;
   }

   @Override
   public Persister<Message> getPersister() {
      return OpenWireMessagePersister.INSTANCE;
   }

   @Override
   public String getAddress() {
      return message.getDestination().getPhysicalName();
   }

   @Override
   public Message setAddress(String address) {
      message.setDestination(ActiveMQDestination.createDestination(address, ActiveMQDestination.QUEUE_TYPE));
      return this;
   }

   @Override
   public SimpleString getAddressSimpleString() {
      return new StringSimpleStringPool().getOrCreate(message.getDestination().getPhysicalName());
   }

   @Override
   public Message setAddress(SimpleString address) {
      message.setDestination(ActiveMQDestination.createDestination(address.toString(), ActiveMQDestination.QUEUE_TYPE));
      return this;
   }

   @Override
   public long getTimestamp() {
      return message.getTimestamp();
   }

   @Override
   public Message setTimestamp(long timestamp) {
      message.setTimestamp(timestamp);
      return this;
   }

   @Override
   public byte getPriority() {
      return message.getPriority();
   }

   @Override
   public Message setPriority(byte priority) {
      message.setPriority(priority);
      return this;
   }

   @Override
   public void receiveBuffer(ByteBuf buffer) {

   }

   @Override
   public void sendBuffer(ByteBuf buffer, int deliveryCount) {

   }

   @Override
   public int getPersistSize() {
      return DataConstants.SIZE_INT + getBuffer().array().length;
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      targetRecord.writeInt(getBuffer().array().length);
      targetRecord.writeBytes(getBuffer().array(), 0, getBuffer().writerIndex() );
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record) {
      int size = record.readInt();
      byte[] recordArray = new byte[size];
      record.readBytes(recordArray);
      setBuffer(Unpooled.wrappedBuffer(recordArray));
   }

   @Override
   public Message putBooleanProperty(String key, boolean value) {
      try {
         message.setBooleanProperty(key, value);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putByteProperty(String key, byte value) {
      try {
         message.setByteProperty(key, value);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putBytesProperty(String key, byte[] value) {
      return putObjectProperty(key, value);
   }

   @Override
   public Message putShortProperty(String key, short value) {
      try {
         message.setShortProperty(key, value);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putCharProperty(String key, char value) {
      return putStringProperty(key, Character.toString(value));
   }

   @Override
   public Message putIntProperty(String key, int value) {
      try {
         message.setIntProperty(key, value);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putLongProperty(String key, long value) {
      try {
         message.setLongProperty(key, value);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putFloatProperty(String key, float value) {
      try {
         message.setFloatProperty(key, value);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putDoubleProperty(String key, double value) {
      try {
         message.setDoubleProperty(key, value);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putBooleanProperty(SimpleString key, boolean value) {
      return putBooleanProperty(key.toString(), value);
   }

   @Override
   public Message putByteProperty(SimpleString key, byte value) {
      return putByteProperty(key.toString(), value);
   }

   @Override
   public Message putBytesProperty(SimpleString key, byte[] value) {
      return putBytesProperty(key.toString(), value);
   }

   @Override
   public Message putShortProperty(SimpleString key, short value) {
      return putShortProperty(key.toString(), value);
   }

   @Override
   public Message putCharProperty(SimpleString key, char value) {
      return putCharProperty(key.toString(), value);
   }

   @Override
   public Message putIntProperty(SimpleString key, int value) {
      return putIntProperty(key.toString(), value);
   }

   @Override
   public Message putLongProperty(SimpleString key, long value) {
      return putLongProperty(key.toString(), value);
   }

   @Override
   public Message putFloatProperty(SimpleString key, float value) {
      return putFloatProperty(key.toString(), value);

   }

   @Override
   public Message putDoubleProperty(SimpleString key, double value) {
      return putDoubleProperty(key.toString(), value);
   }

   @Override
   public Message putStringProperty(String key, String value) {
      try {
         message.setStringProperty(key, value);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putObjectProperty(String key, Object value) throws ActiveMQPropertyConversionException {
      try {
         final Object v;
         if (value instanceof SimpleString) {
            v = value.toString();
         } else {
            v = value;
         }
         message.setProperty(key, value);
      } catch (IOException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
      return this;
   }

   @Override
   public Message putObjectProperty(SimpleString key, Object value) throws ActiveMQPropertyConversionException {
      return putObjectProperty(key.toString(), value);
   }

   @Override
   public Object removeProperty(String key) {
      try {
         Object o = message.getProperty(key);
         if (o != null) {
            message.removeProperty(key);
         }
         return o;
      } catch (IOException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }

   }

   @Override
   public boolean containsProperty(String key) {
      try {
         return message.getProperty(key) != null;
      } catch (IOException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Boolean getBooleanProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return message.getBooleanProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Byte getByteProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return message.getByteProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return message.getDoubleProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Integer getIntProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return message.getIntProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Long getLongProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return message.getLongProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Object getObjectProperty(String key) {
      try {
         return message.getObjectProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Short getShortProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return message.getShortProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public Float getFloatProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return message.getFloatProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public String getStringProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return message.getStringProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return SimpleString.toSimpleString(message.getStringProperty(key));
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public byte[] getBytesProperty(String key) throws ActiveMQPropertyConversionException {
      try {
         return (byte[]) message.getObjectProperty(key);
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
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
   public Object getAnnotation(SimpleString key) {
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
      return SimpleString.toSimpleString(getStringProperty(key));
   }

   @Override
   public byte[] getBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return getBytesProperty(key.toString());
   }

   @Override
   public Message putStringProperty(SimpleString key, SimpleString value) {
      return putStringProperty(key.toString(), value.toString());
   }

   @Override
   public Message putStringProperty(SimpleString key, String value) {
      return putStringProperty(key.toString(), value);
   }

   @Override
   public int getEncodeSize() {
      return getBuffer() == null ? -1 : getBuffer().writerIndex();
   }

   @Override
   public Set<SimpleString> getPropertyNames() {
      try {
         Set<SimpleString> simpleStrings = new HashSet<>();
         Enumeration propertyNames = message.getPropertyNames();
         while (propertyNames.hasMoreElements()) {
            simpleStrings.add(SimpleString.toSimpleString((String) propertyNames.nextElement()));
         }
         return simpleStrings;
      } catch (JMSException e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public ICoreMessage toCore() {
      return toCore(null);
   }

   @Override
   public ICoreMessage toCore(CoreMessageObjectPools coreMessageObjectPools) {
      try {
         return OpenWireMessageConverter.convertToCore(message, wireFormat, coreMessageObjectPools);
      } catch (Exception e) {
         throw new ActiveMQPropertyConversionException(e.getMessage());
      }
   }

   @Override
   public int getMemoryEstimate() {
      return message.getSize();
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return getEncodeSize();
   }
}
