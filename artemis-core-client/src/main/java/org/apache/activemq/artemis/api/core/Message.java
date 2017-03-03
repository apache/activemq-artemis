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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.encode.BodyType;
import org.apache.activemq.artemis.core.message.LargeBodyEncoder;
import org.apache.activemq.artemis.core.persistence.Persister;

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
 *
 *
 * User cases that will be covered by Message
 *
 * Receiving a buffer:
 *
 * Message encode = new CoreMessage(); // or any other implementation
 * encode.receiveBuffer(buffer);
 *
 *
 * Sending to a buffer:
 *
 * Message encode;
 * size = encode.getEncodeSize();
 * encode.encodeDirectly(bufferOutput);
 *
 *
 * Disabling temporary buffer:
 *
 * // This will make the message to only be encoded directly to the output stream, useful on client core API
 * encode.disableInternalBuffer();

 */
public interface Message {


   SimpleString HDR_ROUTE_TO_IDS = new SimpleString("_AMQ_ROUTE_TO");

   SimpleString HDR_SCALEDOWN_TO_IDS = new SimpleString("_AMQ_SCALEDOWN_TO");

   SimpleString HDR_ROUTE_TO_ACK_IDS = new SimpleString("_AMQ_ACK_ROUTE_TO");

   // used by the bridges to set duplicates
   SimpleString HDR_BRIDGE_DUPLICATE_ID = new SimpleString("_AMQ_BRIDGE_DUP");

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


   void messageChanged();

   /**
    * Careful: Unless you are changing the body of the message, prefer getReadOnlyBodyBuffer
    */
   ActiveMQBuffer getBodyBuffer();

   ActiveMQBuffer getReadOnlyBodyBuffer();

   /** Used in the cases of large messages */
   LargeBodyEncoder getBodyEncoder() throws ActiveMQException;

   /** Context can be used by the application server to inject extra control, like a protocol specific on the server.
    * There is only one per Object, use it wisely!
    *
    * Note: the intent of this was to replace PageStore reference on Message, but it will be later increased by adidn a ServerPojo
    * */
   RefCountMessageListener getContext();

   Message setContext(RefCountMessageListener context);

   /** The buffer will belong to this message, until release is called. */
   Message setBuffer(ByteBuf buffer);

   // TODO-now: Do we need this?
   byte getType();

   // TODO-now: Do we need this?
   Message setType(byte type);

   /**
    * Returns whether this message is a <em>large message</em> or a regular message.
    */
   boolean isLargeMessage();

   /**
    * TODO: There's currently some treatment on LargeMessage that is done for server's side large message
    *       This needs to be refactored, this Method shouldn't be used at all.
    * @Deprecated do not use this, internal use only. *It will* be removed for sure even on minor releases.
    * */
   @Deprecated
   default boolean isServerMessage() {
      return false;
   }

   ByteBuf getBuffer();

   /** It will generate a new instance of the message encode, being a deep copy, new properties, new everything */
   Message copy();

   /** It will generate a new instance of the message encode, being a deep copy, new properties, new everything */
   Message copy(long newID);

   /**
    * Returns the messageID.
    * <br>
    * The messageID is set when the message is handled by the server.
    */
   long getMessageID();

   Message setMessageID(long id);

   /**
    * Returns the expiration time of this message.
    */
   long getExpiration();

   /**
    * Sets the expiration of this message.
    *
    * @param expiration expiration time
    */
   Message setExpiration(long expiration);

   /**
    * Returns whether this message is expired or not.
    */
   default boolean isExpired() {
      if (getExpiration() == 0) {
         return false;
      }

      return System.currentTimeMillis() - getExpiration() >= 0;
   }


   /**
    * Returns the userID - this is an optional user specified UUID that can be set to identify the message
    * and will be passed around with the message
    *
    * @return the user id
    */
   Object getUserID();

   Message setUserID(Object userID);

   void copyHeadersAndProperties(final Message msg);

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

   Persister<Message> getPersister();

   Object getProtocol();

   Message setProtocol(Object protocol);

   Object getBody();

   BodyType getBodyType();

   Message setBody(BodyType type, Object body);

   String getAddress();

   Message setAddress(String address);

   SimpleString getAddressSimpleString();

   Message setAddress(SimpleString address);

   long getTimestamp();

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

   /** Used to receive this message from an encoded medium buffer */
   void receiveBuffer(ByteBuf buffer);

   /** Used to send this message to an encoded medium buffer.
    * @param buffer the buffer used.
    * @param deliveryCount Some protocols (AMQP) will have this as part of the message. */
   void sendBuffer(ByteBuf buffer, int deliveryCount);

   int getPersistSize();

   void persist(ActiveMQBuffer targetRecord);

   void reloadPersistence(ActiveMQBuffer record);

   default void releaseBuffer() {
      ByteBuf buffer = getBuffer();
      if (buffer != null) {
         buffer.release();
      }
      setBuffer(null);
   }

   default String getText() {
      if (getBodyType() == BodyType.Text) {
         return getBody().toString();
      } else {
         return null;
      }
   }

   // TODO-now: move this to some utility class
   default void referenceOriginalMessage(final Message original, String originalQueue) {
      String queueOnMessage = original.getStringProperty(Message.HDR_ORIGINAL_QUEUE.toString());

      if (queueOnMessage != null) {
         putStringProperty(Message.HDR_ORIGINAL_QUEUE.toString(), queueOnMessage);
      } else if (originalQueue != null) {
         putStringProperty(Message.HDR_ORIGINAL_QUEUE.toString(), originalQueue);
      }

      if (original.containsProperty(Message.HDR_ORIG_MESSAGE_ID.toString())) {
         putStringProperty(Message.HDR_ORIGINAL_ADDRESS.toString(), original.getStringProperty(Message.HDR_ORIGINAL_ADDRESS.toString()));

         putLongProperty(Message.HDR_ORIG_MESSAGE_ID.toString(), original.getLongProperty(Message.HDR_ORIG_MESSAGE_ID.toString()));
      } else {
         putStringProperty(Message.HDR_ORIGINAL_ADDRESS.toString(), original.getAddress());

         putLongProperty(Message.HDR_ORIG_MESSAGE_ID.toString(), original.getMessageID());
      }

      // reset expiry
      setExpiration(0);
   }

   /**
    * it will translate a property named HDR_DUPLICATE_DETECTION_ID.
    * TODO-NOW: this can probably be replaced by an utility.
    * @return
    */
   default byte[] getDuplicateIDBytes() {
      Object duplicateID = getDuplicateProperty();

      if (duplicateID == null) {
         return null;
      } else {
         if (duplicateID instanceof SimpleString) {
            return ((SimpleString) duplicateID).getData();
         } else if (duplicateID instanceof String) {
            return new SimpleString(duplicateID.toString()).getData();
         } else {
            return (byte[]) duplicateID;
         }
      }
   }

   /**
    * it will translate a property named HDR_DUPLICATE_DETECTION_ID.
    * TODO-NOW: this can probably be replaced by an utility.
    * @return
    */
   default Object getDuplicateProperty() {
      return getObjectProperty(Message.HDR_DUPLICATE_DETECTION_ID.toString());
   }


   Message putBooleanProperty(String key, boolean value);

   Message putByteProperty(String key, byte value);

   Message putBytesProperty(String key, byte[] value);

   Message putShortProperty(String key, short value);

   Message putCharProperty(String key, char value);

   Message putIntProperty(String key, int value);

   Message putLongProperty(String key, long value);

   Message putFloatProperty(String key, float value);

   Message putDoubleProperty(String key, double value);



   Message putBooleanProperty(SimpleString key, boolean value);

   Message putByteProperty(SimpleString key, byte value);

   Message putBytesProperty(SimpleString key, byte[] value);

   Message putShortProperty(SimpleString key, short value);

   Message putCharProperty(SimpleString key, char value);

   Message putIntProperty(SimpleString key, int value);

   Message putLongProperty(SimpleString key, long value);

   Message putFloatProperty(SimpleString key, float value);

   Message putDoubleProperty(SimpleString key, double value);

   /**
    * Puts a String property in this message.
    *
    * @param key   property name
    * @param value property value
    */
   Message putStringProperty(String key, String value);

   Message putObjectProperty(String key, Object value) throws ActiveMQPropertyConversionException;

   Message putObjectProperty(SimpleString key, Object value) throws ActiveMQPropertyConversionException;

   Object removeProperty(String key);

   boolean containsProperty(String key);

   Boolean getBooleanProperty(String key) throws ActiveMQPropertyConversionException;

   Byte getByteProperty(String key) throws ActiveMQPropertyConversionException;

   Double getDoubleProperty(String key) throws ActiveMQPropertyConversionException;

   Integer getIntProperty(String key) throws ActiveMQPropertyConversionException;

   Long getLongProperty(String key) throws ActiveMQPropertyConversionException;

   Object getObjectProperty(String key);

   Short getShortProperty(String key) throws ActiveMQPropertyConversionException;

   Float getFloatProperty(String key) throws ActiveMQPropertyConversionException;

   String getStringProperty(String key) throws ActiveMQPropertyConversionException;

   SimpleString getSimpleStringProperty(String key) throws ActiveMQPropertyConversionException;

   byte[] getBytesProperty(String key) throws ActiveMQPropertyConversionException;


   Object removeProperty(SimpleString key);

   boolean containsProperty(SimpleString key);

   Boolean getBooleanProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   Byte getByteProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   Double getDoubleProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   Integer getIntProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   Long getLongProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   Object getObjectProperty(SimpleString key);

   Short getShortProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   Float getFloatProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   String getStringProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   SimpleString getSimpleStringProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   byte[] getBytesProperty(SimpleString key) throws ActiveMQPropertyConversionException;

   Message putStringProperty(SimpleString key, SimpleString value);

   /**
    * Returns the size of the <em>encoded</em> message.
    */
   int getEncodeSize();

   /**
    * Returns all the names of the properties for this message.
    */
   Set<SimpleString> getPropertyNames();



   int getRefCount();

   int incrementRefCount() throws Exception;

   int decrementRefCount() throws Exception;

   int incrementDurableRefCount();

   int decrementDurableRefCount();

   /**
    * @return Returns the message in Map form, useful when encoding to JSON
    */
   default Map<String, Object> toMap() {
      Map map = toPropertyMap();
      map.put("messageID", getMessageID());
      Object userID = getUserID();
      if (getUserID() != null) {
         map.put("userID", "ID:" + userID.toString());
      }

      map.put("address", getAddress());
      map.put("type", getBodyType().toString());
      map.put("durable", isDurable());
      map.put("expiration", getExpiration());
      map.put("timestamp", getTimestamp());
      map.put("priority", (int)getPriority());

      return map;
   }

   /**
    * @return Returns the message properties in Map form, useful when encoding to JSON
    */
   default Map<String, Object> toPropertyMap() {
      Map map = new HashMap<>();
      for (SimpleString name : getPropertyNames()) {
         map.put(name.toString(), getObjectProperty(name.toString()));
      }
      return map;
   }


   /** This should make you convert your message into Core format. */
   Message toCore();

   int getMemoryEstimate();



}
