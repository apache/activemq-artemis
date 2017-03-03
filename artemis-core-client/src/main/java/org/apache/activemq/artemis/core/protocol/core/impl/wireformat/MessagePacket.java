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


import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

public abstract class MessagePacket extends PacketImpl implements MessagePacketI {

   protected Message message;

   public MessagePacket(final byte type, final Message message) {
      super(type);

      this.message = message;
   }

   @Override
   public Message getMessage() {
      return message;
   }

   @Override
   public String getParentString() {
      return super.getParentString() + ", message=" + message;
   }

   protected ActiveMQBuffer internalCreatePacket(int size, RemotingConnection connection, boolean usePooled) {
      if (connection == null) {
         return new ChannelBufferWrapper(Unpooled.buffer(size));
      } else {
         return connection.createTransportBuffer(size, usePooled);
      }
   }

}
