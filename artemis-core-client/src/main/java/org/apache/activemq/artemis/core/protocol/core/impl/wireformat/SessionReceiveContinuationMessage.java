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
import org.apache.activemq.artemis.utils.DataConstants;

public class SessionReceiveContinuationMessage extends SessionContinuationMessage {

   // Constants -----------------------------------------------------

   public static final int SESSION_RECEIVE_CONTINUATION_BASE_SIZE = SESSION_CONTINUATION_BASE_SIZE + DataConstants.SIZE_LONG;

   // Attributes ----------------------------------------------------

   private long consumerID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionReceiveContinuationMessage() {
      super(SESS_RECEIVE_CONTINUATION);
   }

   /**
    * @param consumerID
    * @param body
    * @param continues
    * @param requiresResponse
    */
   public SessionReceiveContinuationMessage(final long consumerID,
                                            final byte[] body,
                                            final boolean continues,
                                            final boolean requiresResponse) {
      super(SESS_RECEIVE_CONTINUATION, body, continues);
      this.consumerID = consumerID;
   }

   public SessionReceiveContinuationMessage(final long consumerID,
                                            final byte[] body,
                                            final boolean continues,
                                            final boolean requiresResponse,
                                            final int packetSize) {
      this(consumerID, body, continues, requiresResponse);
      this.size = packetSize;
   }

   /**
    * @return the consumerID
    */
   public long getConsumerID() {
      return consumerID;
   }

   // Protected -----------------------------------------------------

   @Override
   public int expectedEncodeSize() {
      return super.expectedEncodeSize() + DataConstants.SIZE_LONG;
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeLong(consumerID);
   }

   @Override
   public int getPacketSize() {
      if (size == -1) {
         // This packet was created by the LargeMessageController
         return 0;
      } else {
         return size;
      }
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      consumerID = buffer.readLong();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int) (consumerID ^ (consumerID >>> 32));
      return result;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", consumerID=" + consumerID);
      buff.append("]");
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionReceiveContinuationMessage))
         return false;
      SessionReceiveContinuationMessage other = (SessionReceiveContinuationMessage) obj;
      if (consumerID != other.consumerID)
         return false;
      return true;
   }

}
