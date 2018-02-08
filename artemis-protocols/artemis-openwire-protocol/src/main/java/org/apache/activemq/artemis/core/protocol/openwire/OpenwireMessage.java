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

package org.apache.activemq.artemis.core.protocol.openwire;

import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RefCountMessageListener;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.Persister;

import io.netty.buffer.ByteBuf;

// TODO: Implement this
public class OpenwireMessage implements Message {

   @Override
   public boolean containsProperty(SimpleString key) {
      return false;
   }

   @Override
   public void messageChanged() {

   }

   @Override
   public SimpleString getReplyTo() {
      return null;
   }

   @Override
   public Message setReplyTo(SimpleString address) {
      return null;
   }

   @Override
   public Object removeAnnotation(SimpleString key) {
      return null;
   }

   @Override
   public Object getAnnotation(SimpleString key) {
      return null;
   }

   @Override
   public Long getScheduledDeliveryTime() {
      return null;
   }

   @Override
   public RefCountMessageListener getContext() {
      return null;
   }

   @Override
   public Message setContext(RefCountMessageListener context) {
      return null;
   }

   @Override
   public Message setBuffer(ByteBuf buffer) {
      return null;
   }

   @Override
   public ByteBuf getBuffer() {
      return null;
   }

   @Override
   public Message copy() {
      return null;
   }

   @Override
   public Message copy(long newID) {
      return null;
   }

   @Override
   public long getMessageID() {
      return 0;
   }

   @Override
   public Message setMessageID(long id) {
      return null;
   }

   @Override
   public long getExpiration() {
      return 0;
   }

   @Override
   public Message setExpiration(long expiration) {
      return null;
   }

   @Override
   public Object getUserID() {
      return null;
   }

   @Override
   public Message setUserID(Object userID) {
      return null;
   }

   @Override
   public boolean isDurable() {
      return false;
   }

   @Override
   public Message setDurable(boolean durable) {
      return null;
   }

   @Override
   public Persister<Message> getPersister() {
      return null;
   }

   @Override
   public String getAddress() {
      return null;
   }

   @Override
   public Message setAddress(String address) {
      return null;
   }

   @Override
   public SimpleString getAddressSimpleString() {
      return null;
   }

   @Override
   public Message setAddress(SimpleString address) {
      return null;
   }

   @Override
   public long getTimestamp() {
      return 0;
   }

   @Override
   public Message setTimestamp(long timestamp) {
      return null;
   }

   @Override
   public byte getPriority() {
      return 0;
   }

   @Override
   public Message setPriority(byte priority) {
      return null;
   }

   @Override
   public void receiveBuffer(ByteBuf buffer) {

   }

   @Override
   public void sendBuffer(ByteBuf buffer, int deliveryCount) {

   }

   @Override
   public int getPersistSize() {
      return 0;
   }

   @Override
   public void persist(ActiveMQBuffer targetRecord) {

   }

   @Override
   public void reloadPersistence(ActiveMQBuffer record) {

   }

   @Override
   public Message putBooleanProperty(String key, boolean value) {
      return null;
   }

   @Override
   public Message putByteProperty(String key, byte value) {
      return null;
   }

   @Override
   public Message putBytesProperty(String key, byte[] value) {
      return null;
   }

   @Override
   public Message putShortProperty(String key, short value) {
      return null;
   }

   @Override
   public Message putCharProperty(String key, char value) {
      return null;
   }

   @Override
   public Message putIntProperty(String key, int value) {
      return null;
   }

   @Override
   public Message putLongProperty(String key, long value) {
      return null;
   }

   @Override
   public Message putFloatProperty(String key, float value) {
      return null;
   }

   @Override
   public Message putDoubleProperty(String key, double value) {
      return null;
   }

   @Override
   public Message putBooleanProperty(SimpleString key, boolean value) {
      return null;
   }

   @Override
   public Message putByteProperty(SimpleString key, byte value) {
      return null;
   }

   @Override
   public Message putBytesProperty(SimpleString key, byte[] value) {
      return null;
   }

   @Override
   public Message putShortProperty(SimpleString key, short value) {
      return null;
   }

   @Override
   public Message putCharProperty(SimpleString key, char value) {
      return null;
   }

   @Override
   public Message putIntProperty(SimpleString key, int value) {
      return null;
   }

   @Override
   public Message putLongProperty(SimpleString key, long value) {
      return null;
   }

   @Override
   public Message putFloatProperty(SimpleString key, float value) {
      return null;
   }

   @Override
   public Message putDoubleProperty(SimpleString key, double value) {
      return null;
   }

   @Override
   public Message putStringProperty(String key, String value) {
      return null;
   }

   @Override
   public Message putObjectProperty(String key, Object value) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Message putObjectProperty(SimpleString key, Object value) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Object removeProperty(String key) {
      return null;
   }

   @Override
   public boolean containsProperty(String key) {
      return false;
   }

   @Override
   public Boolean getBooleanProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Byte getByteProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Integer getIntProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Long getLongProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Object getObjectProperty(String key) {
      return null;
   }

   @Override
   public Short getShortProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Float getFloatProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public String getStringProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public byte[] getBytesProperty(String key) throws ActiveMQPropertyConversionException {
      return new byte[0];
   }

   @Override
   public Object removeProperty(SimpleString key) {
      return null;
   }

   @Override
   public Boolean getBooleanProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Byte getByteProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Double getDoubleProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Integer getIntProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Long getLongProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Object getObjectProperty(SimpleString key) {
      return null;
   }

   @Override
   public Short getShortProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public Float getFloatProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public String getStringProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public SimpleString getSimpleStringProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return null;
   }

   @Override
   public byte[] getBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException {
      return new byte[0];
   }

   @Override
   public Message putStringProperty(SimpleString key, SimpleString value) {
      return null;
   }

   @Override
   public Message putStringProperty(SimpleString key, String value) {
      return null;
   }

   @Override
   public int getEncodeSize() {
      return 0;
   }

   @Override
   public Set<SimpleString> getPropertyNames() {
      return null;
   }

   @Override
   public int getRefCount() {
      return 0;
   }

   @Override
   public int incrementRefCount() throws Exception {
      return 0;
   }

   @Override
   public int decrementRefCount() throws Exception {
      return 0;
   }

   @Override
   public int incrementDurableRefCount() {
      return 0;
   }

   @Override
   public int decrementDurableRefCount() {
      return 0;
   }

   @Override
   public ICoreMessage toCore() {
      return toCore(null);
   }

   @Override
   public ICoreMessage toCore(CoreMessageObjectPools coreMessageObjectPools) {
      return null;
   }

   @Override
   public int getMemoryEstimate() {
      return 0;
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return 0;
   }
}
