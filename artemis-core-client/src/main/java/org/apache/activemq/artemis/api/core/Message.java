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
package org.apache.activemq.artemis.api.core;

import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.utils.UUID;

/**
 * A Message is a routable instance that has a payload.
 * <p>
 * The payload (the "body") is opaque to the messaging system. A Message also has a fixed set of
 * headers (required by the messaging system) and properties (defined by the users) that can be used
 * by the messaging system to route the message (e.g. to ensure it matches a queue filter).
 * <h2>Message Properties</h2>
 * <p>
 * Message can contain properties specified by the users. It is possible to convert from some types
 * to other types as specified by the following table:
 * <pre>
 * |        | boolean byte short int long float double String byte[]
 * |----------------------------------------------------------------
 * |boolean |    X                                      X
 * |byte    |          X    X    X   X                  X
 * |short   |               X    X   X                  X
 * |int     |                    X   X                  X
 * |long    |                        X                  X
 * |float   |                              X     X      X
 * |double  |                                    X      X
 * |String  |    X     X    X    X   X     X     X      X
 * |byte[]  |                                                   X
 * |-----------------------------------------------------------------
 * </pre>
 * <p>
 * If conversion is not allowed (for example calling {@code getFloatProperty} on a property set a
 * {@code boolean}), a {@link ActiveMQPropertyConversionException} will be thrown.
 */
public interface Message {

   /**
    * the actual time the message was expired.
    * * *
    */
   SimpleString HDR_ACTUAL_EXPIRY_TIME = new SimpleString("_AMQ_ACTUAL_EXPIRY");

   /**
    * The original address of a message when a message is transferred through DLQ or expiry
    */
   SimpleString HDR_ORIGINAL_ADDRESS = new SimpleString("_AMQ_ORIG_ADDRESS");

   /**
    * The original address of a message when a message is transferred through DLQ or expiry
    */
   SimpleString HDR_ORIGINAL_QUEUE = new SimpleString("_AMQ_ORIG_QUEUE");

   /**
    * The original message ID before th emessage was transferred.
    */
   SimpleString HDR_ORIG_MESSAGE_ID = new SimpleString("_AMQ_ORIG_MESSAGE_ID");

   /**
    * For the Message Grouping feature.
    */
   SimpleString HDR_GROUP_ID = new SimpleString("_AMQ_GROUP_ID");

   /**
    * to determine if the Large Message was compressed.
    */
   SimpleString HDR_LARGE_COMPRESSED = new SimpleString("_AMQ_LARGE_COMPRESSED");

   /**
    * The body size of a large message before it was compressed.
    */
   SimpleString HDR_LARGE_BODY_SIZE = new SimpleString("_AMQ_LARGE_SIZE");

   /**
    * To be used with Scheduled Delivery.
    */
   SimpleString HDR_SCHEDULED_DELIVERY_TIME = new SimpleString("_AMQ_SCHED_DELIVERY");

   /**
    * To be used with duplicate detection.
    */
   SimpleString HDR_DUPLICATE_DETECTION_ID = new SimpleString("_AMQ_DUPL_ID");

   /**
    * To be used with Last value queues.
    */
   SimpleString HDR_LAST_VALUE_NAME = new SimpleString("_AMQ_LVQ_NAME");

   /**
    * To define the mime-type of body messages. Mainly for stomp but it could be informed on any message for user purposes.
    */
   SimpleString HDR_CONTENT_TYPE = new SimpleString("_AMQ_CONTENT_TYPE");

   /**
    * The name of the validated user who sent the message. Useful for auditing.
    */
   SimpleString HDR_VALIDATED_USER = new SimpleString("_AMQ_VALIDATED_USER");

   /**
    * The Routing Type for this message.  Ensures that this message is only routed to queues with matching routing type.
    */
   SimpleString HDR_ROUTING_TYPE = new SimpleString("_AMQ_ROUTING_TYPE");

   byte DEFAULT_TYPE = 0;

   byte OBJECT_TYPE = 2;

   byte TEXT_TYPE = 3;

   byte BYTES_TYPE = 4;

   byte MAP_TYPE = 5;

   byte STREAM_TYPE = 6;

   /**
    * Returns the messageID.
    * <br>
    * The messageID is set when the message is handled by the server.
    */
   long getMessageID();

   /**
    * Returns the userID - this is an optional user specified UUID that can be set to identify the message
    * and will be passed around with the message
    *
    * @return the user id
    */
   UUID getUserID();

   /**
    * Sets the user ID
    *
    * @param userID
    */
   Message setUserID(UUID userID);

   /**
    * Returns the address this message is sent to.
    */
   SimpleString getAddress();

   /**
    * Sets the address to send this message to.
    *
    * @param address address to send the message to
    */
   Message setAddress(SimpleString address);

   /**
    * Returns this message type.
    * <p>
    * See fields {@literal *_TYPE} for possible values.
    */
   byte getType();

   /**
    * Returns whether this message is durable or not.
    */
   boolean isDurable();

   /**
    * Sets whether this message is durable or not.
    *
    * @param durable {@code true} to flag this message as durable, {@code false} else
    */
   Message setDurable(boolean durable);

   /**
    * Returns the expiration time of this message.
    */
   long getExpiration();

   /**
    * Returns whether this message is expired or not.
    */
   boolean isExpired();

   /**
    * Sets the expiration of this message.
    *
    * @param expiration expiration time
    */
   Message setExpiration(long expiration);

   /**
    * Returns the message timestamp.
    * <br>
    * The timestamp corresponds to the time this message
    * was handled by an ActiveMQ Artemis server.
    */
   long getTimestamp();

   /**
    * Sets the message timestamp.
    *
    * @param timestamp timestamp
    */
   Message setTimestamp(long timestamp);

   /**
    * Returns the message priority.
    * <p>
    * Values range from 0 (less priority) to 9 (more priority) inclusive.
    */
   byte getPriority();

   /**
    * Sets the message priority.
    * <p>
    * Value must be between 0 and 9 inclusive.
    *
    * @param priority the new message priority
    */
   Message setPriority(byte priority);

   /**
    * Returns the size of the <em>encoded</em> message.
    */
   int getEncodeSize();

   /**
    * Returns whether this message is a <em>large message</em> or a regular message.
    */
   boolean isLargeMessage();

   /**
    * Returns the message body as an ActiveMQBuffer
    */
   ActiveMQBuffer getBodyBuffer();

   /**
    * Writes the input byte array to the message body ActiveMQBuffer
    */
   Message writeBodyBufferBytes(byte[] bytes);

   /**
    * Writes the input String to the message body ActiveMQBuffer
    */
   Message writeBodyBufferString(String string);

   /**
    * Returns a <em>copy</em> of the message body as an ActiveMQBuffer. Any modification
    * of this buffer should not impact the underlying buffer.
    */
   ActiveMQBuffer getBodyBufferDuplicate();

   // Properties
   // -----------------------------------------------------------------

   /**
    * Puts a boolean property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putBooleanProperty(SimpleString key, boolean value);

   /**
    * @see #putBooleanProperty(SimpleString, boolean)
    */
   Message putBooleanProperty(String key, boolean value);

   /**
    * Puts a byte property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putByteProperty(SimpleString key, byte value);

   /**
    * @see #putByteProperty(SimpleString, byte)
    */
   Message putByteProperty(String key, byte value);

   /**
    * Puts a byte[] property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putBytesProperty(SimpleString key, byte[] value);

   /**
    * @see #putBytesProperty(SimpleString, byte[])
    */
   Message putBytesProperty(String key, byte[] value);

   /**
    * Puts a short property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putShortProperty(SimpleString key, short value);

   /**
    * @see #putShortProperty(SimpleString, short)
    */
   Message putShortProperty(String key, short value);

   /**
    * Puts a char property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putCharProperty(SimpleString key, char value);

   /**
    * @see #putCharProperty(SimpleString, char)
    */
   Message putCharProperty(String key, char value);

   /**
    * Puts an int property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putIntProperty(SimpleString key, int value);

   /**
    * @see #putIntProperty(SimpleString, int)
    */
   Message putIntProperty(String key, int value);

   /**
    * Puts a long property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putLongProperty(SimpleString key, long value);

   /**
    * @see #putLongProperty(SimpleString, long)
    */
   Message putLongProperty(String key, long value);

   /**
    * Puts a float property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putFloatProperty(SimpleString key, float value);

   /**
    * @see #putFloatProperty(SimpleString, float)
    */
   Message putFloatProperty(String key, float value);

   /**
    * Puts a double property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putDoubleProperty(SimpleString key, double value);

   /**
    * @see #putDoubleProperty(SimpleString, double)
    */
   Message putDoubleProperty(String key, double value);

   /**
    * Puts a SimpleString property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putStringProperty(SimpleString key, SimpleString value);

   /**
    * Puts a String property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putStringProperty(String key, String value);

   /**
    * Puts an Object property in this message. <br>
    * Accepted types are:
    * <ul>
    * <li>Boolean</li>
    * <li>Byte</li>
    * <li>Short</li>
    * <li>Character</li>
    * <li>Integer</li>
    * <li>Long</li>
    * <li>Float</li>
    * <li>Double</li>
    * <li>String</li>
    * <li>SimpleString</li>
    * </ul>
    * Using any other type will throw a PropertyConversionException.
    *
    * @param key   property name
    * @param value property value
    * @throws ActiveMQPropertyConversionException if the value is not one of the accepted property
    *                                             types.
    */
   Message putObjectProperty(SimpleString key, Object value) throws ActiveMQPropertyConversionException;

   /**
    * @see #putObjectProperty(SimpleString, Object)
    */
   Message putObjectProperty(String key, Object value) throws ActiveMQPropertyConversionException;

   /**
    * Removes the property corresponding to the specified key.
    *
    * @param key property name
    * @return the value corresponding to the specified key or @{code null}
    */
   Object removeProperty(SimpleString key);

   /**
    * @see #removeProperty(SimpleString)
    */
   Object removeProperty(String key);

   /**
    * Returns {@code true} if this message contains a property with the given key, {@code false} else.
    *
    * @param key property name
    */
   boolean containsProperty(SimpleString key);

   /**
    * @see #containsProperty(SimpleString)
    */
   boolean containsProperty(String key);

   /**
    * Returns the property corresponding to the specified key as a Boolean.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a Boolean
    */
   Boolean getBooleanProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getBooleanProperty(SimpleString)
    */
   Boolean getBooleanProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key as a Byte.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a Byte
    */
   Byte getByteProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getByteProperty(SimpleString)
    */
   Byte getByteProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key as a Double.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a Double
    */
   Double getDoubleProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getDoubleProperty(SimpleString)
    */
   Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key as an Integer.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to an Integer
    */
   Integer getIntProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getIntProperty(SimpleString)
    */
   Integer getIntProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key as a Long.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a Long
    */
   Long getLongProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getLongProperty(SimpleString)
    */
   Long getLongProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key
    */
   Object getObjectProperty(SimpleString key);

   /**
    * @see #getBooleanProperty(SimpleString)
    */
   Object getObjectProperty(String key);

   /**
    * Returns the property corresponding to the specified key as a Short.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a Short
    */
   Short getShortProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getShortProperty(SimpleString)
    */
   Short getShortProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key as a Float.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a Float
    */
   Float getFloatProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getFloatProperty(SimpleString)
    */
   Float getFloatProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key as a String.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a String
    */
   String getStringProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getStringProperty(SimpleString)
    */
   String getStringProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key as a SimpleString.
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a SimpleString
    */
   SimpleString getSimpleStringProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getSimpleStringProperty(SimpleString)
    */
   SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns the property corresponding to the specified key as a byte[].
    *
    * @throws ActiveMQPropertyConversionException if the value can not be converted to a byte[]
    */
   byte[] getBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   /**
    * @see #getBytesProperty(SimpleString)
    */
   byte[] getBytesProperty(String key) throws ActiveMQPropertyConversionException;

   /**
    * Returns all the names of the properties for this message.
    */
   Set<SimpleString> getPropertyNames();

   /**
    * @return Returns the message in Map form, useful when encoding to JSON
    */
   Map<String, Object> toMap();

   /**
    * @return Returns the message properties in Map form, useful when encoding to JSON
    */
   Map<String, Object> toPropertyMap();
}
