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
package org.apache.activemq.artemis.core.protocol;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.client.impl.ClientLargeMessageImpl;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketDecoder;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveClientLargeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveMessage;

import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_LARGE_MSG;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_MSG;

public class ClientPacketDecoder extends PacketDecoder {

   private static final long serialVersionUID = 6952614096979334582L;
   public static final ClientPacketDecoder INSTANCE = new ClientPacketDecoder();

   @Override
   public Packet decode(final ActiveMQBuffer in) {
      final byte packetType = in.readByte();

      Packet packet = decode(packetType);

      packet.decode(in);

      return packet;
   }

   @Override
   public Packet decode(byte packetType) {
      Packet packet;

      switch (packetType) {
         case SESS_RECEIVE_MSG: {
            packet = new SessionReceiveMessage(new ClientMessageImpl());
            break;
         }
         case SESS_RECEIVE_LARGE_MSG: {
            packet = new SessionReceiveClientLargeMessage(new ClientLargeMessageImpl());
            break;
         }
         default: {
            packet = super.decode(packetType);
         }
      }
      return packet;
   }
}
