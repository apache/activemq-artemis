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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import java.util.Arrays;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

public final class ReplicationLargeMessageWriteMessage extends PacketImpl {

   private long messageId;

   private byte[] body;

   public ReplicationLargeMessageWriteMessage() {
      super(PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE);
   }

   /**
    * @param messageId
    * @param body
    */
   public ReplicationLargeMessageWriteMessage(final long messageId, final byte[] body) {
      this();

      this.messageId = messageId;
      this.body = body;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeLong(messageId);
      buffer.writeInt(body.length);
      buffer.writeBytes(body);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      messageId = buffer.readLong();
      int size = buffer.readInt();
      body = new byte[size];
      buffer.readBytes(body);
   }

   /**
    * @return the messageId
    */
   public long getMessageId() {
      return messageId;
   }

   /**
    * @return the body
    */
   public byte[] getBody() {
      return body;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + Arrays.hashCode(body);
      result = prime * result + (int) (messageId ^ (messageId >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "ReplicationLargeMessageWriteMessage{" +
         "messageId=" + messageId +
         ", body.size=" + body.length +
         '}';
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof ReplicationLargeMessageWriteMessage))
         return false;
      ReplicationLargeMessageWriteMessage other = (ReplicationLargeMessageWriteMessage) obj;
      if (!Arrays.equals(body, other.body))
         return false;
      if (messageId != other.messageId)
         return false;
      return true;
   }
}
