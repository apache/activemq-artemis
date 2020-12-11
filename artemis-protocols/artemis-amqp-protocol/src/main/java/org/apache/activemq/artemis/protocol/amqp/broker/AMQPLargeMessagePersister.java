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

package org.apache.activemq.artemis.protocol.amqp.broker;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.persistence.PersisterIDs.AMQPLargeMessagePersister_ID;

public class AMQPLargeMessagePersister extends MessagePersister {
   private static final Logger log = Logger.getLogger(AMQPLargeMessagePersister.class);

   public static final byte ID = AMQPLargeMessagePersister_ID;

   public static AMQPLargeMessagePersister theInstance;

   public static AMQPLargeMessagePersister getInstance() {
      if (theInstance == null) {
         theInstance = new AMQPLargeMessagePersister();
      }
      return theInstance;
   }

   @Override
   public byte getID() {
      return ID;
   }

   public AMQPLargeMessagePersister() {
      super();
   }


   @Override
   public int getEncodeSize(Message record) {
      AMQPLargeMessage msgEncode = (AMQPLargeMessage) record;
      ByteBuf buf = msgEncode.getSavedEncodeBuffer();

      try {
         int encodeSize = DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + DataConstants.SIZE_LONG + DataConstants.SIZE_LONG + SimpleString.sizeofNullableString(record.getAddressSimpleString()) + DataConstants.SIZE_BOOLEAN + buf.writerIndex() +
            DataConstants.SIZE_LONG +  // expiredTime
            DataConstants.SIZE_BOOLEAN; // reencoded

         TypedProperties properties = ((AMQPMessage) record).getExtraProperties();

         return encodeSize + (properties != null ? properties.getEncodeSize() : 0);
      } finally {
         msgEncode.releaseEncodedBuffer();
      }
   }

   /**
    * Sub classes must add the first short as the protocol-id
    */
   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      super.encode(buffer, record);

      AMQPLargeMessage msgEncode = (AMQPLargeMessage) record;

      buffer.writeLong(record.getMessageID());
      buffer.writeBoolean(record.isDurable());
      buffer.writeLong(msgEncode.getMessageFormat());
      buffer.writeNullableSimpleString(record.getAddressSimpleString());
      TypedProperties properties = ((AMQPMessage) record).getExtraProperties();
      if (properties == null) {
         buffer.writeInt(0);
      } else {
         buffer.writeInt(properties.getEncodeSize());
         properties.encode(buffer.byteBuf());
      }

      ByteBuf savedEncodeBuffer = msgEncode.getSavedEncodeBuffer();
      buffer.writeBytes(savedEncodeBuffer, 0, savedEncodeBuffer.writerIndex());
      buffer.writeLong(record.getExpiration());
      buffer.writeBoolean(msgEncode.isReencoded());
      msgEncode.releaseEncodedBufferAfterWrite(); // we need two releases, as getSavedEncodedBuffer will keep 1 for himself until encoding has happened
                                                  // which this is the expected event where we need to release the extra refCounter
   }

   @Override
   public Message decode(ActiveMQBuffer buffer, Message record, CoreMessageObjectPools pools) {

      long id = buffer.readLong();
      boolean durable = buffer.readBoolean();
      long format = buffer.readLong();
      SimpleString address = buffer.readNullableSimpleString();

      int size = buffer.readInt();

      TypedProperties properties;

      if (size != 0) {
         properties = new TypedProperties(Message.INTERNAL_PROPERTY_NAMES_PREDICATE);
         properties.decode(buffer.byteBuf());
      } else {
         properties = null;
      }

      AMQPLargeMessage largeMessage = new AMQPLargeMessage(id, format, properties, null, null);

      largeMessage.setFileDurable(durable);
      if (address != null) {
         largeMessage.setAddress(address);
      }

      largeMessage.readSavedEncoding(buffer.byteBuf());

      if (buffer.readableBytes() >= DataConstants.SIZE_LONG) {
         largeMessage.reloadExpiration(buffer.readLong());
      }

      if (buffer.readable()) {
         boolean reEncoded = buffer.readBoolean();
         largeMessage.setReencoded(reEncoded);
      }

      return largeMessage;
   }

}
