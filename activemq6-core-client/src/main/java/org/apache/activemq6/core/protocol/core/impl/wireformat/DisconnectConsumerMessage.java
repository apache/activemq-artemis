/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.core.protocol.core.impl.wireformat;


import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.core.protocol.core.impl.PacketImpl;

public class DisconnectConsumerMessage extends PacketImpl
{
   private long consumerId;

   public DisconnectConsumerMessage(final long consumerId)
   {
      super(DISCONNECT_CONSUMER);
      this.consumerId = consumerId;
   }

   public DisconnectConsumerMessage()
   {
      super(DISCONNECT_CONSUMER);
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(consumerId);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      consumerId = buffer.readLong();
   }

   public long getConsumerId()
   {
      return consumerId;
   }
}
