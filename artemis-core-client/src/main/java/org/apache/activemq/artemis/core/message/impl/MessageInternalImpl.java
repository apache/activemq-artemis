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
package org.apache.activemq.artemis.core.message.impl;

import java.io.InputStream;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.BodyEncoder;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.TypedProperties;

@Deprecated
public class MessageInternalImpl implements MessageInternal {

   private CoreMessage message;

   public MessageInternalImpl(ICoreMessage message) {
      this.message = (CoreMessage) message;
   }

   @Override
   public void decodeFromBuffer(ActiveMQBuffer buffer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getEndOfMessagePosition() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getEndOfBodyPosition() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void bodyChanged() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isServerMessage() {
      return true;
   }

   @Override
   public ActiveMQBuffer getEncodedBuffer() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getHeadersAndPropertiesEncodeSize() {
      return message.getHeadersAndPropertiesEncodeSize();
   }

   @Override
   public ActiveMQBuffer getWholeBuffer() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void encodeHeadersAndProperties(ActiveMQBuffer buffer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void decodeHeadersAndProperties(ActiveMQBuffer buffer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public BodyEncoder getBodyEncoder() throws ActiveMQException {
      throw new UnsupportedOperationException();
   }

   @Override
   public InputStream getBodyInputStream() {
      return message.getBodyInputStream();
   }

   @Override
   public void messageChanged() {
      throw new UnsupportedOperationException();
   }

   /**
    * Used to calculate what is the delivery time.
    * Return null if not scheduled.
    */
   @Override
   public Long getScheduledDeliveryTime() {
      return message.getScheduledDeliveryTime();
   }

   @Override
   public SimpleString getReplyTo() {
      return message.getReplyTo();
   }

   @Override
   public Message setReplyTo(SimpleString address) {
      message.setReplyTo(address);
      return this;
   }

   /**
    * The buffer will belong to this message, until release is called.
    *
    * @param buffer
    */
   @Override
   public Message setBuffer(ByteBuf buffer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf getBuffer() {
      return message.getBuffer();
   }

   /**
    * It will generate a new instance of the message encode, being a deep copy, new properties, new everything
    */
   @Override
   public Message copy() {
      return message.copy();
   }

   /**
    * It will generate a new instance of the message encode, being a deep copy, new properties, new everything
    *
    * @param newID
    */
   @Override
   public Message copy(long newID) {
      return message.copy(newID);
   }

   @Override
   public Message copy(long newID, boolean isDLQorExpiry) {
      return message.copy(newID, isDLQorExpiry);
   }

   /**
    * Returns the messageID.
    * <br>
    * The messageID is set when the message is handled by the server.
    */
   @Override
   public long getMessageID() {
      return message.getMessageID();
   }

   @Override
   public Message setMessageID(long id) {
      message.setMessageID(id);
      return this;
   }

   /**
    * Returns the expiration time of this message.
    */
   @Override
   public long getExpiration() {
      return message.getExpiration();
   }

   /**
    * Sets the expiration of this message.
    *
    * @param expiration expiration time
    */
   @Override
   public Message setExpiration(long expiration) {
      message.setExpiration(expiration);
      return this;
   }

   /**
    * This represents historically the JMSMessageID.
    * We had in the past used this for the MessageID that was sent on core messages...
    *
    * later on when we added AMQP this name clashed with AMQPMessage.getUserID();
    *
    * @return the user id
    */
   @Override
   public Object getUserID() {
      return message.getUserID();
   }

   @Override
   public Message setUserID(Object userID) {
      message.setUserID(userID);
      return this;
   }

   /**
    * Returns whether this message is durable or not.
    */
   @Override
   public boolean isDurable() {
      return message.isDurable();
   }

   /**
    * Sets whether this message is durable or not.
    *
    * @param durable {@code true} to flag this message as durable, {@code false} else
    */
   @Override
   public Message setDurable(boolean durable) {
      message.setDurable(durable);
      return message;
   }

   @Override
   public Persister<Message> getPersister() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String getAddress() {
      return message.getAddress();
   }

   @Override
   public Message setAddress(String address) {
      message.setAddress(address);
      return this;
   }

   @Override
   public SimpleString getAddressSimpleString() {
      return message.getAddressSimpleString();
   }

   @Override
   public Message setAddress(SimpleString address) {
      message.setAddress(address);
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

   /**
    * Returns the message priority.
    * <p>
    * Values range from 0 (less priority) to 9 (more priority) inclusive.
    */
   @Override
   public byte getPriority() {
      return message.getPriority();
   }

   /**
    * Sets the message priority.
    * <p>
    * Value must be between 0 and 9 inclusive.
    *
    * @param priority the new message priority
    */
   @Override
   public Message setPriority(byte priority) {
      message.setPriority(priority);
      return this;
   }

   /**
    * Used to receive this message from an encoded medium buffer
    *
    * @param buffer
    */
   @Override
   public void receiveBuffer(ByteBuf buffer) {
      throw new UnsupportedOperationException();
   }

   /**
    * Used to send this message to an encoded medium buffer.
    *
    * @param buffer        the buffer used.
    * @param deliveryCount Some protocols (AMQP) will have this as part of the message.
    */
   @Override
   public void sendBuffer(ByteBuf buffer, int deliveryCount) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getPersistSize() {
      return message.getPersistSize();
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {
      message.persist(targetRecord);
   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record, CoreMessageObjectPools pools) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Message putBooleanProperty(String key, boolean value) {
      message.putBooleanProperty(key, value);
      return this;
   }

   @Override
   public Message putByteProperty(String key, byte value) {
      message.putByteProperty(key, value);
      return this;
   }

   @Override
   public Message putBytesProperty(String key, byte[] value) {
      message.putBytesProperty(key, value);
      return this;
   }

   @Override
   public Message putShortProperty(String key, short value) {
      message.putShortProperty(key, value);
      return this;
   }

   @Override
   public Message putCharProperty(String key, char value) {
      message.putCharProperty(key, value);
      return this;
   }

   @Override
   public Message putIntProperty(String key, int value) {
      message.putIntProperty(key, value);
      return this;
   }

   @Override
   public Message putLongProperty(String key, long value) {
      message.putLongProperty(key, value);
      return this;
   }

   @Override
   public Message putFloatProperty(String key, float value) {
      message.putFloatProperty(key, value);
      return this;
   }

   @Override
   public Message putDoubleProperty(String key, double value) {
      message.putDoubleProperty(key, value);
      return this;
   }

   @Override
   public Message putBooleanProperty(SimpleString key, boolean value) {
      message.putBooleanProperty(key, value);
      return this;
   }

   @Override
   public Message putByteProperty(SimpleString key, byte value) {
      message.putByteProperty(key, value);
      return this;
   }

   @Override
   public Message putBytesProperty(SimpleString key, byte[] value) {
      message.putBytesProperty(key, value);
      return this;
   }

   @Override
   public Message putShortProperty(SimpleString key, short value) {
      message.putShortProperty(key, value);
      return this;
   }

   @Override
   public Message putCharProperty(SimpleString key, char value) {
      message.putCharProperty(key, value);
      return this;
   }

   @Override
   public Message putIntProperty(SimpleString key, int value) {
      message.putIntProperty(key, value);
      return this;
   }

   @Override
   public Message putLongProperty(SimpleString key, long value) {
      message.putLongProperty(key, value);
      return this;
   }

   @Override
   public Message putFloatProperty(SimpleString key, float value) {
      message.putFloatProperty(key, value);
      return this;
   }

   @Override
   public Message putDoubleProperty(SimpleString key, double value) {
      message.putDoubleProperty(key, value);
      return this;
   }

   /**
    * Puts a String property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   @Override
   public Message putStringProperty(String key, String value) {
      message.putStringProperty(key, value);
      return this;
   }

   @Override
   public Message putObjectProperty(String key, Object value) throws ActiveMQPropertyConversionException {
      message.putObjectProperty(key, value);
      return this;
   }

   @Override
   public Message putObjectProperty(SimpleString key, Object value) throws ActiveMQPropertyConversionException {
      message.putObjectProperty(key, value);
      return this;
   }

   @Override
   public Object removeProperty(String key) {
      return message.removeProperty(key);
   }

   @Override
   public boolean containsProperty(String key) {
      return message.containsProperty(key);
   }

   @Override
   public Boolean getBooleanProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getBooleanProperty(key);
   }

   @Override
   public Byte getByteProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getByteProperty(key);
   }

   @Override
   public Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getDoubleProperty(key);
   }

   @Override
   public Integer getIntProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getIntProperty(key);
   }

   @Override
   public Long getLongProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getLongProperty(key);
   }

   @Override
   public Object getObjectProperty(String key) {
      return message.getObjectProperty(key);
   }

   @Override
   public Short getShortProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getShortProperty(key);
   }

   @Override
   public Float getFloatProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getFloatProperty(key);
   }

   @Override
   public String getStringProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getStringProperty(key);
   }

   @Override
   public SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getSimpleStringProperty(key);
   }

   @Override
   public byte[] getBytesProperty(String key) throws ActiveMQPropertyConversionException {
      return message.getBytesProperty(key);
   }

   @Override
   public Object removeProperty(SimpleString key) {
      return message.removeProperty(key);
   }

   @Override
   public boolean containsProperty(SimpleString key) {
      return message.containsProperty(key);
   }

   @Override
   public Boolean getBooleanProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getBooleanProperty(key);
   }

   @Override
   public Byte getByteProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getByteProperty(key);
   }

   @Override
   public Double getDoubleProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getDoubleProperty(key);
   }

   @Override
   public Integer getIntProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getIntProperty(key);
   }

   @Override
   public Long getLongProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getLongProperty(key);
   }

   @Override
   public Object getObjectProperty(SimpleString key) {
      return message.getObjectProperty(key);
   }

   @Override
   public Object getAnnotation(SimpleString key) {
      return message.getAnnotation(key);
   }

   @Override
   public Short getShortProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getShortProperty(key);
   }

   @Override
   public Float getFloatProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getFloatProperty(key);
   }

   @Override
   public String getStringProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getStringProperty(key);
   }

   @Override
   public SimpleString getSimpleStringProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getSimpleStringProperty(key);
   }

   @Override
   public byte[] getBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return message.getBytesProperty(key);
   }

   @Override
   public Message putStringProperty(SimpleString key, SimpleString value) {
      return message.putStringProperty(key, value);
   }

   @Override
   public Message putStringProperty(SimpleString key, String value) {
      return message.putStringProperty(key, value);
   }

   /**
    * Returns the size of the <em>encoded</em> message.
    */
   @Override
   public int getEncodeSize() {
      return message.getEncodeSize();
   }

   /**
    * Returns all the names of the properties for this message.
    */
   @Override
   public Set<SimpleString> getPropertyNames() {
      return message.getPropertyNames();
   }

   @Override
   public int getRefCount() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getDurableCount() {
      return 0;
   }

   @Override
   public int refUp() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int refDown() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int usageUp() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int usageDown() {
      return 0;
   }

   @Override
   public int getUsage() {
      return 0;
   }

   @Override
   public int durableUp() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int durableDown() {
      throw new UnsupportedOperationException();
   }

   /**
    * This should make you convert your message into Core format.
    */
   @Override
   public ICoreMessage toCore() {
      return message.toCore();
   }

   /**
    * This should make you convert your message into Core format.
    *
    * @param coreMessageObjectPools
    */
   @Override
   public ICoreMessage toCore(CoreMessageObjectPools coreMessageObjectPools) {
      return message.toCore();
   }

   @Override
   public int getMemoryEstimate() {
      return message.getMemoryEstimate();
   }

   @Override
   public void setAddressTransient(SimpleString address) {
      message.setAddress(address);
   }

   @Override
   public TypedProperties getTypedProperties() {
      return new TypedProperties(message.getProperties());
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return message.getPersistentSize();
   }

   @Override
   public Object getOwner() {
      return message.getOwner();
   }

   @Override
   public void setOwner(Object object) {
      message.setOwner(object);
   }

}
