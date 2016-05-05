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

public class CreateSessionResponseMessage extends PacketImpl {

   private int serverVersion;

   public CreateSessionResponseMessage(final int serverVersion) {
      super(CREATESESSION_RESP);

      this.serverVersion = serverVersion;
   }

   public CreateSessionResponseMessage() {
      super(CREATESESSION_RESP);
   }

   @Override
   public boolean isResponse() {
      return true;
   }

   public int getServerVersion() {
      return serverVersion;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeInt(serverVersion);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      serverVersion = buffer.readInt();
   }

   @Override
   public final boolean isRequiresConfirmations() {
      return false;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + serverVersion;
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof CreateSessionResponseMessage))
         return false;
      CreateSessionResponseMessage other = (CreateSessionResponseMessage) obj;
      if (serverVersion != other.serverVersion)
         return false;
      return true;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", serverVersion=" + serverVersion);
      buff.append("]");
      return buff.toString();
   }
}
