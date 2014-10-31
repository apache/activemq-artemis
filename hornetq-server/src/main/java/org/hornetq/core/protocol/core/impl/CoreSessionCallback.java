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
package org.hornetq.core.protocol.core.impl;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.DisconnectConsumerMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsFailMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.remoting.ReadyListener;

/**
 * A CoreSessionCallback
 *
 * @author Tim Fox
 */
public final class CoreSessionCallback implements SessionCallback
{
   private final Channel channel;

   private ProtocolManager protocolManager;

   private String name;

   public CoreSessionCallback(String name, ProtocolManager protocolManager, Channel channel)
   {
      this.name = name;
      this.protocolManager = protocolManager;
      this.channel = channel;
   }

   public int sendLargeMessage(ServerMessage message, long consumerID, long bodySize, int deliveryCount)
   {
      Packet packet = new SessionReceiveLargeMessage(consumerID, message, bodySize, deliveryCount);

      channel.send(packet);

      int size = packet.getPacketSize();

      return size;
   }

   public int sendLargeMessageContinuation(long consumerID, byte[] body, boolean continues, boolean requiresResponse)
   {
      Packet packet = new SessionReceiveContinuationMessage(consumerID, body, continues, requiresResponse);

      channel.send(packet);

      return packet.getPacketSize();
   }

   public int sendMessage(ServerMessage message, long consumerID, int deliveryCount)
   {
      Packet packet = new SessionReceiveMessage(consumerID, message, deliveryCount);

      int size = 0;

      if (channel.sendBatched(packet))
      {
         size = packet.getPacketSize();
      }

      return size;
   }

   public void sendProducerCreditsMessage(int credits, SimpleString address)
   {
      Packet packet = new SessionProducerCreditsMessage(credits, address);

      channel.send(packet);
   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address)
   {
      Packet packet = new SessionProducerCreditsFailMessage(credits, address);

      channel.send(packet);
   }

   public void closed()
   {
      protocolManager.removeHandler(name);
   }

   public void addReadyListener(final ReadyListener listener)
   {
      channel.getConnection().getTransportConnection().addReadyListener(listener);
   }

   public void removeReadyListener(final ReadyListener listener)
   {
      channel.getConnection().getTransportConnection().removeReadyListener(listener);
   }

   @Override
   public void disconnect(long consumerId, String queueName)
   {
      if (channel.supports(PacketImpl.DISCONNECT_CONSUMER))
      {
         channel.send(new DisconnectConsumerMessage(consumerId));
      }
      else
      {
         HornetQServerLogger.LOGGER.warnDisconnectOldClient(queueName);
      }
   }
}