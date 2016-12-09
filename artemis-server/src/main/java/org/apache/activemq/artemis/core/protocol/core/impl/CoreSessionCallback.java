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
package org.apache.activemq.artemis.core.protocol.core.impl;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.DisconnectConsumerMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionProducerCreditsFailMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionProducerCreditsMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;

public final class CoreSessionCallback implements SessionCallback {

   private final Channel channel;

   private ProtocolManager protocolManager;

   private final RemotingConnection connection;

   private String name;

   public CoreSessionCallback(String name,
                              ProtocolManager protocolManager,
                              Channel channel,
                              RemotingConnection connection) {
      this.name = name;
      this.protocolManager = protocolManager;
      this.channel = channel;
      this.connection = connection;
   }

   @Override
   public boolean isWritable(ReadyListener callback, Object protocolContext) {
      return connection.isWritable(callback);
   }

   @Override
   public boolean updateDeliveryCountAfterCancel(ServerConsumer consumer, MessageReference ref, boolean failed) {
      return false;
   }

   @Override
   public int sendLargeMessage(MessageReference ref,
                               ServerMessage message,
                               ServerConsumer consumer,
                               long bodySize,
                               int deliveryCount) {
      Packet packet = new SessionReceiveLargeMessage(consumer.getID(), message, bodySize, deliveryCount);

      channel.send(packet);

      int size = packet.getPacketSize();

      return size;
   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumer,
                                           byte[] body,
                                           boolean continues,
                                           boolean requiresResponse) {
      Packet packet = new SessionReceiveContinuationMessage(consumer.getID(), body, continues, requiresResponse);

      channel.send(packet);

      return packet.getPacketSize();
   }

   @Override
   public int sendMessage(MessageReference ref, ServerMessage message, ServerConsumer consumer, int deliveryCount) {
      Packet packet = new SessionReceiveMessage(consumer.getID(), message, deliveryCount);

      int size = 0;

      if (channel.sendBatched(packet)) {
         size = packet.getPacketSize();
      }

      return size;
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address) {
      Packet packet = new SessionProducerCreditsMessage(credits, address);

      channel.send(packet);
   }

   @Override
   public void browserFinished(ServerConsumer consumer) {

   }

   @Override
   public void afterDelivery() throws Exception {

   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address) {
      Packet packet = new SessionProducerCreditsFailMessage(credits, address);

      channel.send(packet);
   }

   @Override
   public void closed() {
      protocolManager.removeHandler(name);
   }

   @Override
   public void disconnect(ServerConsumer consumerId, String queueName) {
      if (channel.supports(PacketImpl.DISCONNECT_CONSUMER)) {
         channel.send(new DisconnectConsumerMessage(consumerId.getID()));
      } else {
         ActiveMQServerLogger.LOGGER.warnDisconnectOldClient(queueName);
      }
   }

   @Override
   public boolean hasCredits(ServerConsumer consumer) {
      // This one will always return has credits
      // as the flow control is done by activemq
      return true;
   }
}
