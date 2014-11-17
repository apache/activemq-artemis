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
package org.apache.activemq.core.protocol.core.impl;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.protocol.core.Channel;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.core.protocol.core.impl.wireformat.DisconnectConsumerMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.SessionProducerCreditsFailMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.SessionProducerCreditsMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.ServerConsumer;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.spi.core.protocol.ProtocolManager;
import org.apache.activemq.spi.core.protocol.SessionCallback;
import org.apache.activemq.spi.core.remoting.ReadyListener;

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

   public int sendLargeMessage(ServerMessage message, ServerConsumer consumer, long bodySize, int deliveryCount)
   {
      Packet packet = new SessionReceiveLargeMessage(consumer.getID(), message, bodySize, deliveryCount);

      channel.send(packet);

      int size = packet.getPacketSize();

      return size;
   }

   public int sendLargeMessageContinuation(ServerConsumer consumer, byte[] body, boolean continues, boolean requiresResponse)
   {
      Packet packet = new SessionReceiveContinuationMessage(consumer.getID(), body, continues, requiresResponse);

      channel.send(packet);

      return packet.getPacketSize();
   }

   public int sendMessage(ServerMessage message, ServerConsumer consumer, int deliveryCount)
   {
      Packet packet = new SessionReceiveMessage(consumer.getID(), message, deliveryCount);

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
   public void disconnect(ServerConsumer consumerId, String queueName)
   {
      if (channel.supports(PacketImpl.DISCONNECT_CONSUMER))
      {
         channel.send(new DisconnectConsumerMessage(consumerId.getID()));
      }
      else
      {
         HornetQServerLogger.LOGGER.warnDisconnectOldClient(queueName);
      }
   }


   @Override
   public boolean hasCredits(ServerConsumer consumer)
   {
      // This one will always return has credits
      // as the flow control is done by hornetq
      return true;
   }
}