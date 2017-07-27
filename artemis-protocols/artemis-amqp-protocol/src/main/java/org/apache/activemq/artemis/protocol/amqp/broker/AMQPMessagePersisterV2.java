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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.collections.TypedProperties;

public class AMQPMessagePersisterV2 extends AMQPMessagePersister {
   public static final byte ID = 3;

   public static AMQPMessagePersisterV2 theInstance;

   public static AMQPMessagePersisterV2 getInstance() {
      if (theInstance == null) {
         theInstance = new AMQPMessagePersisterV2();
      }
      return theInstance;
   }

   @Override
   public byte getID() {
      return ID;
   }

   public AMQPMessagePersisterV2() {
      super();
   }


   @Override
   public int getEncodeSize(Message record) {
      int encodeSize = super.getEncodeSize(record) + DataConstants.SIZE_INT;

      TypedProperties properties = ((AMQPMessage)record).getExtraProperties();

      return encodeSize + (properties != null ? properties.getEncodeSize() : 0);
   }


   /** Sub classes must add the first short as the protocol-id */
   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      super.encode(buffer, record);

      TypedProperties properties = ((AMQPMessage)record).getExtraProperties();
      if (properties == null) {
         buffer.writeInt(0);
      } else {
         buffer.writeInt(properties.getEncodeSize());
         properties.encode(buffer.byteBuf());
      }
   }


   @Override
   public Message decode(ActiveMQBuffer buffer, Message record) {
      AMQPMessage message = (AMQPMessage)super.decode(buffer, record);
      int size = buffer.readInt();

      if (size != 0) {
         TypedProperties properties = new TypedProperties();
         properties.decode(buffer.byteBuf());
         message.setExtraProperties(properties);
      }
      return message;
   }

}
