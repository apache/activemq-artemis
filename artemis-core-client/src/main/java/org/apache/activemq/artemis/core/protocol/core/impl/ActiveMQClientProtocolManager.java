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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.protocol.ClientPacketDecoder;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.ChannelHandler;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CheckFailoverMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CheckFailoverReplyMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V3;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.DisconnectMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.DisconnectMessage_V2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.Ping;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.apache.activemq.artemis.core.remoting.impl.netty.ActiveMQFrameDecoder2;
import org.apache.activemq.artemis.core.version.Version;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.SessionContext;
import org.apache.activemq.artemis.spi.core.remoting.TopologyResponseHandler;
import org.apache.activemq.artemis.utils.VersionLoader;
import org.jboss.logging.Logger;

/**
 * This class will return specific packets for different types of actions happening on a messaging protocol.
 *
 * This is trying to unify the Core client into multiple protocols.
 *
 * Returning null in certain packets means no action is taken on this specific protocol.
 *
 * Semantic properties could also be added to this implementation.
 *
 * Implementations of this class need to be stateless.
 */

public class ActiveMQClientProtocolManager implements ClientProtocolManager {

   private static final Logger logger = Logger.getLogger(ActiveMQClientProtocolManager.class);

   private static final String handshake = "ARTEMIS";

   private final int versionID = VersionLoader.getVersion().getIncrementingVersion();

   private ClientSessionFactoryInternal factoryInternal;

   /**
    * Guards assignments to {@link #inCreateSession} and {@link #inCreateSessionLatch}
    */
   private final Object inCreateSessionGuard = new Object();

   /**
    * Flag that tells whether we are trying to create a session.
    */
   private boolean inCreateSession;

   /**
    * Used to wait for the creation of a session.
    */
   private CountDownLatch inCreateSessionLatch;

   protected volatile RemotingConnectionImpl connection;

   protected TopologyResponseHandler topologyResponseHandler;

   /**
    * Flag that signals that the communication is closing. Causes many processes to exit.
    */
   private volatile boolean alive = true;

   private final CountDownLatch waitLatch = new CountDownLatch(1);

   public ActiveMQClientProtocolManager() {
   }

   @Override
   public String getName() {
      return ActiveMQClient.DEFAULT_CORE_PROTOCOL;
   }

   @Override
   public void setSessionFactory(ClientSessionFactory factory) {
      this.factoryInternal = (ClientSessionFactoryInternal) factory;
   }

   @Override
   public ClientSessionFactory getSessionFactory() {
      return this.factoryInternal;
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline) {
      pipeline.addLast("activemq-decoder", new ActiveMQFrameDecoder2());
   }

   @Override
   public boolean waitOnLatch(long milliseconds) throws InterruptedException {
      return waitLatch.await(milliseconds, TimeUnit.MILLISECONDS);
   }

   public Channel getChannel0() {
      if (connection == null) {
         return null;
      } else {
         return connection.getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);
      }
   }

   @Override
   public RemotingConnection getCurrentConnection() {
      return connection;
   }

   public Channel getChannel1() {
      if (connection == null) {
         return null;
      } else {
         return connection.getChannel(1, -1);
      }
   }

   @Override
   public Lock lockSessionCreation() {
      try {
         Lock localFailoverLock = factoryInternal.lockFailover();
         try {
            if (connection == null) {
               return null;
            }

            Lock lock = getChannel1().getLock();

            // Lock it - this must be done while the failoverLock is held
            while (isAlive() && !lock.tryLock(100, TimeUnit.MILLISECONDS)) {
            }

            return lock;
         } finally {
            localFailoverLock.unlock();
         }
         // We can now release the failoverLock
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return null;
      }
   }

   @Override
   public void stop() {
      alive = false;

      synchronized (inCreateSessionGuard) {
         if (inCreateSessionLatch != null)
            inCreateSessionLatch.countDown();
      }

      Channel channel1 = getChannel1();
      if (channel1 != null) {
         channel1.returnBlocking();
      }

      waitLatch.countDown();

   }

   @Override
   public boolean isAlive() {
      return alive;
   }

   @Override
   public void ping(long connectionTTL) {
      Channel channel = connection.getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);

      Ping ping = new Ping(connectionTTL);

      channel.send(ping);

      connection.flush();
   }

   @Override
   public void sendSubscribeTopology(final boolean isServer) {
      getChannel0().send(new SubscribeClusterTopologyUpdatesMessageV2(isServer, VersionLoader.getVersion().getIncrementingVersion()));
   }

   @Override
   public SessionContext createSessionContext(String name,
                                              String username,
                                              String password,
                                              boolean xa,
                                              boolean autoCommitSends,
                                              boolean autoCommitAcks,
                                              boolean preAcknowledge,
                                              int minLargeMessageSize,
                                              int confirmationWindowSize) throws ActiveMQException {
      for (Version clientVersion : VersionLoader.getClientVersions()) {
         try {
            return createSessionContext(clientVersion, name, username, password, xa, autoCommitSends, autoCommitAcks, preAcknowledge, minLargeMessageSize, confirmationWindowSize);
         } catch (ActiveMQException e) {
            if (e.getType() != ActiveMQExceptionType.INCOMPATIBLE_CLIENT_SERVER_VERSIONS) {
               throw e;
            }
         }
      }
      connection.destroy();
      throw new ActiveMQException(ActiveMQExceptionType.INCOMPATIBLE_CLIENT_SERVER_VERSIONS);
   }

   public SessionContext createSessionContext(Version clientVersion,
                                              String name,
                                              String username,
                                              String password,
                                              boolean xa,
                                              boolean autoCommitSends,
                                              boolean autoCommitAcks,
                                              boolean preAcknowledge,
                                              int minLargeMessageSize,
                                              int confirmationWindowSize) throws ActiveMQException {
      if (!isAlive())
         throw ActiveMQClientMessageBundle.BUNDLE.clientSessionClosed();

      Channel sessionChannel = null;
      CreateSessionResponseMessage response = null;

      boolean retry;
      do {
         retry = false;

         Lock lock = null;

         try {

            lock = lockSessionCreation();

            // We now set a flag saying createSession is executing
            synchronized (inCreateSessionGuard) {
               if (!isAlive())
                  throw ActiveMQClientMessageBundle.BUNDLE.clientSessionClosed();
               inCreateSession = true;
               inCreateSessionLatch = new CountDownLatch(1);
            }

            long sessionChannelID = connection.generateChannelID();

            Packet request = newCreateSessionPacket(clientVersion, name, username, password, xa, autoCommitSends, autoCommitAcks, preAcknowledge, minLargeMessageSize, confirmationWindowSize, sessionChannelID);

            try {
               // channel1 reference here has to go away
               response = (CreateSessionResponseMessage) getChannel1().sendBlocking(request, PacketImpl.CREATESESSION_RESP);
            } catch (ActiveMQException cause) {
               if (!isAlive())
                  throw cause;

               if (cause.getType() == ActiveMQExceptionType.UNBLOCKED) {
                  // This means the thread was blocked on create session and failover unblocked it
                  // so failover could occur

                  retry = true;

                  continue;
               } else {
                  throw cause;
               }
            }

            sessionChannel = connection.getChannel(sessionChannelID, confirmationWindowSize);

         } catch (Throwable t) {
            if (lock != null) {
               lock.unlock();
               lock = null;
            }

            if (t instanceof ActiveMQException) {
               throw (ActiveMQException) t;
            } else {
               throw ActiveMQClientMessageBundle.BUNDLE.failedToCreateSession(t);
            }
         } finally {
            if (lock != null) {
               lock.unlock();
            }

            // Execution has finished so notify any failover thread that may be waiting for us to be done
            inCreateSession = false;
            inCreateSessionLatch.countDown();
         }
      } while (retry);
      return newSessionContext(name, confirmationWindowSize, sessionChannel, response);

   }

   protected Packet newCreateSessionPacket(Version clientVersion,
                                           String name,
                                           String username,
                                           String password,
                                           boolean xa,
                                           boolean autoCommitSends,
                                           boolean autoCommitAcks,
                                           boolean preAcknowledge,
                                           int minLargeMessageSize,
                                           int confirmationWindowSize,
                                           long sessionChannelID) {
      return new CreateSessionMessage(name, sessionChannelID, clientVersion.getIncrementingVersion(), username, password, minLargeMessageSize, xa, autoCommitSends, autoCommitAcks, preAcknowledge, confirmationWindowSize, null);
   }

   protected SessionContext newSessionContext(String name,
                                              int confirmationWindowSize,
                                              Channel sessionChannel,
                                              CreateSessionResponseMessage response) {
      // these objects won't be null, otherwise it would keep retrying on the previous loop
      return new ActiveMQSessionContext(name, connection, sessionChannel, response.getServerVersion(), confirmationWindowSize);
   }

   @Override
   public boolean cleanupBeforeFailover(ActiveMQException cause) {

      boolean needToInterrupt;

      CountDownLatch exitLockLatch;
      Lock lock = lockSessionCreation();

      if (lock == null) {
         return false;
      }

      try {
         synchronized (inCreateSessionGuard) {
            needToInterrupt = inCreateSession;
            exitLockLatch = inCreateSessionLatch;
         }
      } finally {
         lock.unlock();
      }

      if (needToInterrupt) {
         forceReturnChannel1(cause);

         // Now we need to make sure that the thread has actually exited and returned it's
         // connections
         // before failover occurs

         while (inCreateSession && isAlive()) {
            try {
               if (exitLockLatch != null) {
                  exitLockLatch.await(500, TimeUnit.MILLISECONDS);
               }
            } catch (InterruptedException e1) {
               throw new ActiveMQInterruptedException(e1);
            }
         }
      }

      return true;
   }

   @Override
   public boolean checkForFailover(String liveNodeID) throws ActiveMQException {
      CheckFailoverMessage packet = new CheckFailoverMessage(liveNodeID);
      CheckFailoverReplyMessage message = (CheckFailoverReplyMessage) getChannel1().sendBlocking(packet, PacketImpl.CHECK_FOR_FAILOVER_REPLY);
      return message.isOkToFailover();
   }

   @Override
   public RemotingConnection connect(Connection transportConnection,
                                     long callTimeout,
                                     long callFailoverTimeout,
                                     List<Interceptor> incomingInterceptors,
                                     List<Interceptor> outgoingInterceptors,
                                     TopologyResponseHandler topologyResponseHandler) {
      this.connection = new RemotingConnectionImpl(getPacketDecoder(), transportConnection, callTimeout, callFailoverTimeout, incomingInterceptors, outgoingInterceptors);

      this.topologyResponseHandler = topologyResponseHandler;

      getChannel0().setHandler(new Channel0Handler(connection));

      sendHandshake(transportConnection);

      return connection;
   }

   protected void sendHandshake(Connection transportConnection) {
      if (transportConnection.isUsingProtocolHandling()) {
         // no need to send handshake on inVM as inVM is not using the NettyProtocolHandling
         ActiveMQBuffer amqbuffer = connection.createTransportBuffer(handshake.length());
         amqbuffer.writeBytes(handshake.getBytes());
         transportConnection.write(amqbuffer);
      }
   }

   private class Channel0Handler implements ChannelHandler {

      private final CoreRemotingConnection conn;

      private Channel0Handler(final CoreRemotingConnection conn) {
         this.conn = conn;
      }

      @Override
      public void handlePacket(final Packet packet) {
         final byte type = packet.getType();

         if (type == PacketImpl.DISCONNECT || type == PacketImpl.DISCONNECT_V2) {
            final DisconnectMessage msg = (DisconnectMessage) packet;
            String scaleDownTargetNodeID = null;

            SimpleString nodeID = msg.getNodeID();

            if (packet instanceof DisconnectMessage_V2) {
               final DisconnectMessage_V2 msg_v2 = (DisconnectMessage_V2) packet;
               scaleDownTargetNodeID = msg_v2.getScaleDownNodeID() == null ? null : msg_v2.getScaleDownNodeID().toString();
            }

            if (topologyResponseHandler != null)
               topologyResponseHandler.nodeDisconnected(conn, nodeID == null ? null : nodeID.toString(), scaleDownTargetNodeID);
         } else if (type == PacketImpl.CLUSTER_TOPOLOGY) {
            ClusterTopologyChangeMessage topMessage = (ClusterTopologyChangeMessage) packet;
            notifyTopologyChange(topMessage);
         } else if (type == PacketImpl.CLUSTER_TOPOLOGY_V2) {
            ClusterTopologyChangeMessage_V2 topMessage = (ClusterTopologyChangeMessage_V2) packet;
            notifyTopologyChange(topMessage);
         } else if (type == PacketImpl.CLUSTER_TOPOLOGY || type == PacketImpl.CLUSTER_TOPOLOGY_V2 || type == PacketImpl.CLUSTER_TOPOLOGY_V3) {
            ClusterTopologyChangeMessage topMessage = (ClusterTopologyChangeMessage) packet;
            notifyTopologyChange(topMessage);
         } else if (type == PacketImpl.CHECK_FOR_FAILOVER_REPLY) {
            System.out.println("Channel0Handler.handlePacket");
         }
      }

      /**
       * @param topMessage
       */
      private void notifyTopologyChange(final ClusterTopologyChangeMessage topMessage) {
         final long eventUID;
         final String backupGroupName;
         final String scaleDownGroupName;
         if (topMessage instanceof ClusterTopologyChangeMessage_V3) {
            eventUID = ((ClusterTopologyChangeMessage_V3) topMessage).getUniqueEventID();
            backupGroupName = ((ClusterTopologyChangeMessage_V3) topMessage).getBackupGroupName();
            scaleDownGroupName = ((ClusterTopologyChangeMessage_V3) topMessage).getScaleDownGroupName();
         } else if (topMessage instanceof ClusterTopologyChangeMessage_V2) {
            eventUID = ((ClusterTopologyChangeMessage_V2) topMessage).getUniqueEventID();
            backupGroupName = ((ClusterTopologyChangeMessage_V2) topMessage).getBackupGroupName();
            scaleDownGroupName = null;
         } else {
            eventUID = System.currentTimeMillis();
            backupGroupName = null;
            scaleDownGroupName = null;
         }

         if (topMessage.isExit()) {
            if (logger.isDebugEnabled()) {
               logger.debug("Notifying " + topMessage.getNodeID() + " going down");
            }

            if (topologyResponseHandler != null) {
               topologyResponseHandler.notifyNodeDown(eventUID, topMessage.getNodeID());
            }
         } else {
            Pair<TransportConfiguration, TransportConfiguration> transportConfig = topMessage.getPair();
            if (transportConfig.getA() == null && transportConfig.getB() == null) {
               transportConfig = new Pair<>(conn.getTransportConnection().getConnectorConfig(), null);
            }

            if (topologyResponseHandler != null) {
               topologyResponseHandler.notifyNodeUp(eventUID, topMessage.getNodeID(), backupGroupName, scaleDownGroupName, transportConfig, topMessage.isLast());
            }
         }
      }
   }

   protected PacketDecoder getPacketDecoder() {
      return ClientPacketDecoder.INSTANCE;
   }

   private void forceReturnChannel1(ActiveMQException cause) {
      if (connection != null) {
         Channel channel1 = connection.getChannel(1, -1);

         if (channel1 != null) {
            channel1.returnBlocking(cause);
         }
      }
   }
}
