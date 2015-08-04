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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.DataConstants;

public class SessionSendMessage extends MessagePacket {

   private boolean requiresResponse;

   /**
    * In case, we are using a different handler than the one set on the {@link org.apache.activemq.artemis.api.core.client.ClientSession}
    * <br>
    * This field is only used at the client side.
    *
    * @see org.apache.activemq.artemis.api.core.client.ClientSession#setSendAcknowledgementHandler(SendAcknowledgementHandler)
    * @see org.apache.activemq.artemis.api.core.client.ClientProducer#send(org.apache.activemq.artemis.api.core.SimpleString, org.apache.activemq.artemis.api.core.Message, SendAcknowledgementHandler)
    */
   private final transient SendAcknowledgementHandler handler;

   public SessionSendMessage(final MessageInternal message,
                             final boolean requiresResponse,
                             final SendAcknowledgementHandler handler) {
      super(SESS_SEND, message);
      this.handler = handler;
      this.requiresResponse = requiresResponse;
   }

   public SessionSendMessage(final MessageInternal message) {
      super(SESS_SEND, message);
      this.handler = null;
   }

   // Public --------------------------------------------------------

   public boolean isRequiresResponse() {
      return requiresResponse;
   }

   public SendAcknowledgementHandler getHandler() {
      return handler;
   }

   @Override
   public ActiveMQBuffer encode(final RemotingConnection connection) {
      ActiveMQBuffer buffer = message.getEncodedBuffer();

      // Sanity check
      if (buffer.writerIndex() != message.getEndOfMessagePosition()) {
         throw new IllegalStateException("Wrong encode position");
      }

      buffer.writeBoolean(requiresResponse);

      size = buffer.writerIndex();

      // Write standard headers

      int len = size - DataConstants.SIZE_INT;
      buffer.setInt(0, len);
      buffer.setByte(DataConstants.SIZE_INT, getType());
      buffer.setLong(DataConstants.SIZE_INT + DataConstants.SIZE_BYTE, channelID);

      // Position reader for reading by Netty
      buffer.readerIndex(0);

      message.resetCopied();

      return buffer;
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      // Buffer comes in after having read standard headers and positioned at Beginning of body part

      message.decodeFromBuffer(buffer);

      int ri = buffer.readerIndex();

      requiresResponse = buffer.readBoolean();

      buffer.readerIndex(ri);

   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (requiresResponse ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionSendMessage))
         return false;
      SessionSendMessage other = (SessionSendMessage) obj;
      if (requiresResponse != other.requiresResponse)
         return false;
      return true;
   }

}
