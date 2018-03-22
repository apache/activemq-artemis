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

package org.apache.activemq.artemis.api.core;

import java.io.InputStream;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.core.message.LargeBodyEncoder;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;

/**
 * This interface is only to determine the API of methods required for Core Messages
 */
public interface ICoreMessage extends Message {

   LargeBodyEncoder getBodyEncoder() throws ActiveMQException;

   int getHeadersAndPropertiesEncodeSize();

   @Override
   InputStream getBodyInputStream();

   /**
    * Returns a new Buffer slicing the current Body.
    */
   ActiveMQBuffer getReadOnlyBodyBuffer();

   /**
    * Returns a readOnlyBodyBuffer or a decompressed one if the message is compressed.
    * or the large message buffer.
    * @return
    */
   ActiveMQBuffer getDataBuffer();

   /**
    * Return the type of the message
    */
   @Override
   byte getType();

   /**
    * the type of the message
    */
   @Override
   CoreMessage setType(byte type);

   /**
    * We are really interested if this is a LargeServerMessage.
    *
    * @return
    */
   boolean isServerMessage();

   /**
    * The body used for this message.
    *
    * @return
    */
   @Override
   ActiveMQBuffer getBodyBuffer();

   int getEndOfBodyPosition();

   /**
    * Used on large messages treatment
    */
   void copyHeadersAndProperties(Message msg);

   void sendBuffer_1X(ByteBuf sendBuffer);

   /**
    * it will fix the body of incoming messages from 1.x and before versions
    */
   void receiveBuffer_1X(ByteBuf buffer);

   /**
    * @return Returns the message in Map form, useful when encoding to JSON
    */
   @Override
   default Map<String, Object> toMap() {
      Map map = toPropertyMap();
      map.put("messageID", getMessageID());
      Object userID = getUserID();
      if (getUserID() != null) {
         map.put("userID", "ID:" + userID.toString());
      }

      map.put("address", getAddress() == null ? "" : getAddress());
      map.put("type", getType());
      map.put("durable", isDurable());
      map.put("expiration", getExpiration());
      map.put("timestamp", getTimestamp());
      map.put("priority", getPriority());

      return map;
   }

}
