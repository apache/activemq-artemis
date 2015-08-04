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

/**
 * Ping is sent on the client side by {@link org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl}. At the server's
 * side it is handled by org.apache.activemq.artemis.core.remoting.server.impl.RemotingServiceImpl
 */
public final class Ping extends PacketImpl {

   private long connectionTTL;

   public Ping(final long connectionTTL) {
      super(PING);

      this.connectionTTL = connectionTTL;
   }

   public Ping() {
      super(PING);
   }

   public long getConnectionTTL() {
      return connectionTTL;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeLong(connectionTTL);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      connectionTTL = buffer.readLong();
   }

   @Override
   public boolean isRequiresConfirmations() {
      return false;
   }

   @Override
   public String toString() {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", connectionTTL=" + connectionTTL);
      buf.append("]");
      return buf.toString();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int) (connectionTTL ^ (connectionTTL >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof Ping)) {
         return false;
      }
      Ping other = (Ping) obj;
      if (connectionTTL != other.connectionTTL) {
         return false;
      }
      return true;
   }
}
