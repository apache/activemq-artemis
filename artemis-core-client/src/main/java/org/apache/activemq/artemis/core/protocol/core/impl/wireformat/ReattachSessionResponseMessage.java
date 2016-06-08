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
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

public class ReattachSessionResponseMessage extends PacketImpl {

   private int lastConfirmedCommandID;

   private boolean reattached;

   public ReattachSessionResponseMessage(final int lastConfirmedCommandID, final boolean reattached) {
      super(REATTACH_SESSION_RESP);

      this.lastConfirmedCommandID = lastConfirmedCommandID;

      this.reattached = reattached;
   }

   public ReattachSessionResponseMessage() {
      super(REATTACH_SESSION_RESP);
   }

   // Public --------------------------------------------------------

   public int getLastConfirmedCommandID() {
      return lastConfirmedCommandID;
   }

   public boolean isReattached() {
      return reattached;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeInt(lastConfirmedCommandID);
      buffer.writeBoolean(reattached);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      lastConfirmedCommandID = buffer.readInt();
      reattached = buffer.readBoolean();
   }

   @Override
   public boolean isResponse() {
      return true;
   }

   @Override
   public final boolean isRequiresConfirmations() {
      return false;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + lastConfirmedCommandID;
      result = prime * result + (reattached ? 1231 : 1237);
      return result;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", lastConfirmedCommandID=" + lastConfirmedCommandID);
      buff.append(", reattached=" + reattached);
      buff.append("]");
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof ReattachSessionResponseMessage))
         return false;
      ReattachSessionResponseMessage other = (ReattachSessionResponseMessage) obj;
      if (lastConfirmedCommandID != other.lastConfirmedCommandID)
         return false;
      if (reattached != other.reattached)
         return false;
      return true;
   }
}
