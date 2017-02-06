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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQRemoteDisconnectException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.DisconnectConsumerWithKillMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.DisconnectMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.DisconnectMessage_V2;
import org.apache.activemq.artemis.core.security.ActiveMQPrincipal;
import org.apache.activemq.artemis.spi.core.protocol.AbstractRemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.jboss.logging.Logger;
import org.jctools.maps.NonBlockingHashMapLong;

public class RemotingConnectionImpl extends AbstractRemotingConnection implements CoreRemotingConnection {

   private static final Logger logger = Logger.getLogger(RemotingConnectionImpl.class);

   private final PacketDecoder packetDecoder;

   private final NonBlockingHashMapLong<Channel> channels = new NonBlockingHashMapLong<>();

   private final long blockingCallTimeout;

   private final long blockingCallFailoverTimeout;

   private final List<Interceptor> incomingInterceptors;

   private final List<Interceptor> outgoingInterceptors;

   private volatile boolean destroyed;

   private final boolean client;

   private int clientVersion;

   private volatile SimpleIDGenerator idGenerator = new SimpleIDGenerator(CHANNEL_ID.USER.id);

   private boolean idGeneratorSynced = false;

   private final Object transferLock = new Object();

   private final Object failLock = new Object();

   private final SimpleString nodeID;

   private String clientID;

   // Constructors
   // ---------------------------------------------------------------------------------

   /*
    * Create a client side connection
    */
   public RemotingConnectionImpl(final PacketDecoder packetDecoder,
                                 final Connection transportConnection,
                                 final long blockingCallTimeout,
                                 final long blockingCallFailoverTimeout,
                                 final List<Interceptor> incomingInterceptors,
                                 final List<Interceptor> outgoingInterceptors) {
      this(packetDecoder, transportConnection, blockingCallTimeout, blockingCallFailoverTimeout, incomingInterceptors, outgoingInterceptors, true, null, null);
   }

   /*
    * Create a server side connection
    */
   RemotingConnectionImpl(final PacketDecoder packetDecoder,
                          final Connection transportConnection,
                          final List<Interceptor> incomingInterceptors,
                          final List<Interceptor> outgoingInterceptors,
                          final Executor executor,
                          final SimpleString nodeID) {
      this(packetDecoder, transportConnection, -1, -1, incomingInterceptors, outgoingInterceptors, false, executor, nodeID);
   }

   private RemotingConnectionImpl(final PacketDecoder packetDecoder,
                                  final Connection transportConnection,
                                  final long blockingCallTimeout,
                                  final long blockingCallFailoverTimeout,
                                  final List<Interceptor> incomingInterceptors,
                                  final List<Interceptor> outgoingInterceptors,
                                  final boolean client,
                                  final Executor executor,
                                  final SimpleString nodeID) {
      super(transportConnection, executor);

      this.packetDecoder = packetDecoder;

      this.blockingCallTimeout = blockingCallTimeout;

      this.blockingCallFailoverTimeout = blockingCallFailoverTimeout;

      this.incomingInterceptors = incomingInterceptors;

      this.outgoingInterceptors = outgoingInterceptors;

      this.client = client;

      this.nodeID = nodeID;

      transportConnection.setProtocolConnection(this);
   }

   // RemotingConnection implementation
   // ------------------------------------------------------------

   @Override
   public String toString() {
      return "RemotingConnectionImpl [clientID=" + clientID +
         ", nodeID=" +
         nodeID +
         ", transportConnection=" +
         getTransportConnection() +
         "]";
   }

   /**
    * @return the clientVersion
    */
   @Override
   public int getClientVersion() {
      return clientVersion;
   }

   /**
    * @param clientVersion the clientVersion to set
    */
   @Override
   public void setClientVersion(int clientVersion) {
      this.clientVersion = clientVersion;
   }

   @Override
   public synchronized Channel getChannel(final long channelID, final int confWindowSize) {
      Channel channel = channels.get(channelID);

      if (channel == null) {
         channel = new ChannelImpl(this, channelID, confWindowSize, outgoingInterceptors);

         channels.put(channelID, channel);
      }

      return channel;
   }

   @Override
   public synchronized boolean removeChannel(final long channelID) {
      return channels.remove(channelID) != null;
   }

   @Override
   public synchronized void putChannel(final long channelID, final Channel channel) {
      channels.put(channelID, channel);
   }

   @Override
   public void fail(final ActiveMQException me, String scaleDownTargetNodeID) {
      synchronized (failLock) {
         if (destroyed) {
            return;
         }

         destroyed = true;
      }

      if (!(me instanceof ActiveMQRemoteDisconnectException)) {
         ActiveMQClientLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());
      }

      try {
         transportConnection.forceClose();
      } catch (Throwable e) {
         ActiveMQClientLogger.LOGGER.warn(e.getMessage(), e);
      }

      // Then call the listeners
      callFailureListeners(me, scaleDownTargetNodeID);

      callClosingListeners();

      internalClose();

      for (Channel channel : channels.values()) {
         channel.returnBlocking(me);
      }
   }

   @Override
   public void destroy() {
      synchronized (failLock) {
         if (destroyed) {
            return;
         }

         destroyed = true;
      }

      internalClose();

      callClosingListeners();
   }

   @Override
   public void disconnect(final boolean criticalError) {
      disconnect(null, criticalError);
   }

   @Override
   public void disconnect(String scaleDownNodeID, final boolean criticalError) {
      Channel channel0 = getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);

      // And we remove all channels from the connection, this ensures no more packets will be processed after this
      // method is
      // complete

      Set<Channel> allChannels = new HashSet<>(channels.values());

      if (!criticalError) {
         removeAllChannels();
      } else {
         // We can't hold a lock if a critical error is happening...
         // as other threads will be holding the lock while hanging on IO
         channels.clear();
      }

      // Now we are 100% sure that no more packets will be processed we can flush then send the disconnect

      if (!criticalError) {
         for (Channel channel : allChannels) {
            channel.flushConfirmations();
         }
      }
      Packet disconnect;

      if (channel0.supports(PacketImpl.DISCONNECT_V2)) {
         disconnect = new DisconnectMessage_V2(nodeID, scaleDownNodeID);
      } else {
         disconnect = new DisconnectMessage(nodeID);
      }
      channel0.sendAndFlush(disconnect);
   }

   @Override
   public long generateChannelID() {
      return idGenerator.generateID();
   }

   @Override
   public synchronized void syncIDGeneratorSequence(final long id) {
      if (!idGeneratorSynced) {
         idGenerator = new SimpleIDGenerator(id);

         idGeneratorSynced = true;
      }
   }

   @Override
   public long getIDGeneratorSequence() {
      return idGenerator.getCurrentID();
   }

   @Override
   public Object getTransferLock() {
      return transferLock;
   }

   @Override
   public boolean isClient() {
      return client;
   }

   @Override
   public boolean isDestroyed() {
      return destroyed;
   }

   @Override
   public long getBlockingCallTimeout() {
      return blockingCallTimeout;
   }

   @Override
   public long getBlockingCallFailoverTimeout() {
      return blockingCallFailoverTimeout;
   }

   //We flush any confirmations on the connection - this prevents idle bridges for example
   //sitting there with many unacked messages
   @Override
   public void flush() {
      synchronized (transferLock) {
         for (Channel channel : channels.values()) {
            channel.flushConfirmations();
         }
      }
   }

   @Override
   public ActiveMQPrincipal getDefaultActiveMQPrincipal() {
      return getTransportConnection().getDefaultActiveMQPrincipal();
   }

   @Override
   public boolean isSupportReconnect() {
      for (Channel channel : channels.values()) {
         if (channel.getConfirmationWindowSize() > 0) {
            return true;
         }
      }

      return false;
   }

   // Buffer Handler implementation
   // ----------------------------------------------------
   @Override
   public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer) {
      try {
         final Packet packet = packetDecoder.decode(buffer);

         if (logger.isTraceEnabled()) {
            logger.trace("handling packet " + packet);
         }

         dataReceived = true;
         doBufferReceived(packet);

         super.bufferReceived(connectionID, buffer);
      } catch (Exception e) {
         ActiveMQClientLogger.LOGGER.errorDecodingPacket(e);
      }
   }

   private void doBufferReceived(final Packet packet) {
      if (ChannelImpl.invokeInterceptors(packet, incomingInterceptors, this) != null) {
         return;
      }

      synchronized (transferLock) {
         final Channel channel = channels.get(packet.getChannelID());

         if (channel != null) {
            channel.handlePacket(packet);
         }
      }
   }

   protected void removeAllChannels() {
      // We get the transfer lock first - this ensures no packets are being processed AND
      // it's guaranteed no more packets will be processed once this method is complete
      synchronized (transferLock) {
         channels.clear();
      }
   }

   private void internalClose() {
      // We close the underlying transport connection
      getTransportConnection().close();

      for (Channel channel : channels.values()) {
         channel.close();
      }
   }

   public void setClientID(String cID) {
      clientID = cID;
   }

   public String getClientID() {
      return clientID;
   }

   @Override
   public void killMessage(SimpleString nodeID) {
      if (clientVersion < DisconnectConsumerWithKillMessage.VERSION_INTRODUCED) {
         return;
      }
      Channel clientChannel = getChannel(1, -1);
      DisconnectConsumerWithKillMessage response = new DisconnectConsumerWithKillMessage(nodeID);

      clientChannel.send(response, -1);
   }
}
