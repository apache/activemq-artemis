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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.HornetQClientMessageBundle;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.protocol.ClientPacketDecoder;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.CheckFailoverMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CheckFailoverReplyMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V2;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V3;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.DisconnectMessage;
import org.hornetq.core.protocol.core.impl.wireformat.DisconnectMessage_V2;
import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.hornetq.core.version.Version;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.ClientProtocolManager;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ProtocolResponseHandler;
import org.hornetq.spi.core.remoting.SessionContext;
import org.hornetq.utils.VersionLoader;

/**
 * This class will return specific packets for different types of actions happening on a messaging protocol.
 * <p/>
 * This is trying to unify the Core client into multiple protocols.
 * <p/>
 * Returning null in certain packets means no action is taken on this specific protocol.
 * <p/>
 * Semantic properties could also be added to this implementation.
 * <p/>
 * Implementations of this class need to be stateless.
 *
 * @author Clebert Suconic
 */

public class HornetQClientProtocolManager implements ClientProtocolManager
{
   private final int versionID = VersionLoader.getVersion().getIncrementingVersion();

   private final ClientSessionFactoryInternal factoryInternal;

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


   protected PacketDecoder packetDecoder = ClientPacketDecoder.INSTANCE;

   protected volatile RemotingConnectionImpl connection;

   protected ProtocolResponseHandler callbackHandler;

   /**
    * Flag that signals that the communication is closing. Causes many processes to exit.
    */
   private volatile boolean alive = true;

   private final CountDownLatch waitLatch = new CountDownLatch(1);


   public HornetQClientProtocolManager(ClientSessionFactoryInternal factory)
   {
      this.factoryInternal = factory;
   }


   public void replacePacketDecoder(PacketDecoder decoder)
   {
      this.packetDecoder = decoder;
   }

   public boolean waitOnLatch(long milliseconds) throws InterruptedException
   {
      return waitLatch.await(milliseconds, TimeUnit.MILLISECONDS);
   }

   public Channel getChannel0()
   {
      if (connection == null)
      {
         return null;
      }
      else
      {
         return connection.getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);
      }
   }

   public RemotingConnection getCurrentConnection()
   {
      return connection;
   }


   public Channel getChannel1()
   {
      if (connection == null)
      {
         return null;
      }
      else
      {
         return connection.getChannel(1, -1);
      }
   }

   public Lock lockSessionCreation()
   {
      try
      {
         Lock localFailoverLock = factoryInternal.lockFailover();
         try
         {
            if (connection == null)
            {
               return null;
            }

            Lock lock = getChannel1().getLock();

            // Lock it - this must be done while the failoverLock is held
            while (isAlive() && !lock.tryLock(100, TimeUnit.MILLISECONDS))
            {
            }

            return lock;
         }
         finally
         {
            localFailoverLock.unlock();
         }
         // We can now release the failoverLock
      }
      catch (InterruptedException e)
      {
         Thread.currentThread().interrupt();
         return null;
      }
   }


   public void stop()
   {
      alive = false;


      synchronized (inCreateSessionGuard)
      {
         if (inCreateSessionLatch != null)
            inCreateSessionLatch.countDown();
      }
      forceReturnChannel1();


      Channel channel1 = getChannel1();
      if (channel1 != null)
      {
         channel1.returnBlocking();
      }

      waitLatch.countDown();

   }

   public boolean isAlive()
   {
      return alive;
   }


   public void setConnection(RemotingConnection connection)
   {
      this.connection = (RemotingConnectionImpl) connection;
   }

   @Override
   public void shakeHands()
   {
   }

   @Override
   public void ping(long connectionTTL)
   {
      Channel channel = connection.getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);

      Ping ping = new Ping(connectionTTL);

      channel.send(ping);

      connection.flush();
   }

   public void setResponseHandler(ProtocolResponseHandler handler)
   {
      this.callbackHandler = handler;

      getChannel0().setHandler(new Channel0Handler(connection));
   }


   @Override
   public void sendSubscribeTopology(final boolean isServer)
   {
      getChannel0().send(new SubscribeClusterTopologyUpdatesMessageV2(isServer,
                                                                      VersionLoader.getVersion()
                                                                         .getIncrementingVersion()));
   }

   @Override
   public SessionContext createSessionContext(String name, String username, String password,
                                              boolean xa, boolean autoCommitSends, boolean autoCommitAcks,
                                              boolean preAcknowledge, int minLargeMessageSize, int confirmationWindowSize) throws HornetQException
   {
      for (Version clientVersion : VersionLoader.getClientVersions())
      {
         try
         {
            return createSessionContext(clientVersion,
                                        name,
                                        username,
                                        password,
                                        xa,
                                        autoCommitSends,
                                        autoCommitAcks,
                                        preAcknowledge,
                                        minLargeMessageSize,
                                        confirmationWindowSize);
         }
         catch (HornetQException e)
         {
            if (e.getType() != HornetQExceptionType.INCOMPATIBLE_CLIENT_SERVER_VERSIONS)
            {
               throw e;
            }
         }
      }
      connection.destroy();
      throw new HornetQException(HornetQExceptionType.INCOMPATIBLE_CLIENT_SERVER_VERSIONS);
   }

   public SessionContext createSessionContext(Version clientVersion, String name, String username, String password,
                                              boolean xa, boolean autoCommitSends, boolean autoCommitAcks,
                                              boolean preAcknowledge, int minLargeMessageSize, int confirmationWindowSize) throws HornetQException
   {
      if (!isAlive())
         throw HornetQClientMessageBundle.BUNDLE.clientSessionClosed();

      Channel sessionChannel = null;
      CreateSessionResponseMessage response = null;

      boolean retry;
      do
      {
         retry = false;

         Lock lock = null;

         try
         {

            lock = lockSessionCreation();

            // We now set a flag saying createSession is executing
            synchronized (inCreateSessionGuard)
            {
               if (!isAlive())
                  throw HornetQClientMessageBundle.BUNDLE.clientSessionClosed();
               inCreateSession = true;
               inCreateSessionLatch = new CountDownLatch(1);
            }

            long sessionChannelID = connection.generateChannelID();

            Packet request = new CreateSessionMessage(name,
                                                      sessionChannelID,
                                                      clientVersion.getIncrementingVersion(),
                                                      username,
                                                      password,
                                                      minLargeMessageSize,
                                                      xa,
                                                      autoCommitSends,
                                                      autoCommitAcks,
                                                      preAcknowledge,
                                                      confirmationWindowSize,
                                                      null);


            try
            {
               // channel1 reference here has to go away
               response = (CreateSessionResponseMessage) getChannel1().sendBlocking(request, PacketImpl.CREATESESSION_RESP);
            }
            catch (HornetQException cause)
            {
               if (!isAlive())
                  throw cause;

               if (cause.getType() == HornetQExceptionType.UNBLOCKED)
               {
                  // This means the thread was blocked on create session and failover unblocked it
                  // so failover could occur

                  retry = true;

                  continue;
               }
               else
               {
                  throw cause;
               }
            }

            sessionChannel = connection.getChannel(sessionChannelID, confirmationWindowSize);


         }
         catch (Throwable t)
         {
            if (lock != null)
            {
               lock.unlock();
               lock = null;
            }

            if (t instanceof HornetQException)
            {
               throw (HornetQException) t;
            }
            else
            {
               throw HornetQClientMessageBundle.BUNDLE.failedToCreateSession(t);
            }
         }
         finally
         {
            if (lock != null)
            {
               lock.unlock();
            }

            // Execution has finished so notify any failover thread that may be waiting for us to be done
            inCreateSession = false;
            inCreateSessionLatch.countDown();
         }
      }
      while (retry);


      // these objects won't be null, otherwise it would keep retrying on the previous loop
      return new HornetQSessionContext(name, connection, sessionChannel, response.getServerVersion(), confirmationWindowSize);

   }

   public boolean cleanupBeforeFailover()
   {

      boolean needToInterrupt;

      CountDownLatch exitLockLatch;
      Lock lock = lockSessionCreation();

      if (lock == null)
      {
         return false;
      }

      try
      {
         synchronized (inCreateSessionGuard)
         {
            needToInterrupt = inCreateSession;
            exitLockLatch = inCreateSessionLatch;
         }
      }
      finally
      {
         lock.unlock();
      }

      if (needToInterrupt)
      {
         forceReturnChannel1();

         // Now we need to make sure that the thread has actually exited and returned it's
         // connections
         // before failover occurs

         while (inCreateSession && isAlive())
         {
            try
            {
               if (exitLockLatch != null)
               {
                  exitLockLatch.await(500, TimeUnit.MILLISECONDS);
               }
            }
            catch (InterruptedException e1)
            {
               throw new HornetQInterruptedException(e1);
            }
         }
      }

      return true;
   }

   @Override
   public boolean checkForFailover(String liveNodeID) throws HornetQException
   {
      CheckFailoverMessage packet = new CheckFailoverMessage(liveNodeID);
      CheckFailoverReplyMessage message = (CheckFailoverReplyMessage) getChannel1().sendBlocking(packet,
                                                                                                 PacketImpl.CHECK_FOR_FAILOVER_REPLY);
      return message.isOkToFailover();
   }


   public RemotingConnection connect(Connection transportConnection, long callTimeout, long callFailoverTimeout,
                                     List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors,
                                     ProtocolResponseHandler protocolResponseHandler)
   {
      RemotingConnectionImpl remotingConnection = new RemotingConnectionImpl(packetDecoder, transportConnection,
                                                                             callTimeout, callFailoverTimeout,
                                                                             incomingInterceptors, outgoingInterceptors);
      setConnection(remotingConnection);
      this.setResponseHandler(protocolResponseHandler);

      return remotingConnection;
   }


   private class Channel0Handler implements ChannelHandler
   {
      private final CoreRemotingConnection conn;

      private Channel0Handler(final CoreRemotingConnection conn)
      {
         this.conn = conn;
      }

      public void handlePacket(final Packet packet)
      {
         final byte type = packet.getType();

         if (type == PacketImpl.DISCONNECT || type == PacketImpl.DISCONNECT_V2)
         {
            final DisconnectMessage msg = (DisconnectMessage) packet;
            String scaleDownTargetNodeID = null;

            SimpleString nodeID = msg.getNodeID();

            if (packet instanceof DisconnectMessage_V2)
            {
               final DisconnectMessage_V2 msg_v2 = (DisconnectMessage_V2) packet;
               scaleDownTargetNodeID = msg_v2.getScaleDownNodeID() == null ? null : msg_v2.getScaleDownNodeID().toString();
            }

            if (callbackHandler != null)
               callbackHandler.nodeDisconnected(conn, nodeID == null ? null : nodeID.toString(), scaleDownTargetNodeID);
         }
         else if (type == PacketImpl.CLUSTER_TOPOLOGY || type == PacketImpl.CLUSTER_TOPOLOGY_V2 || type == PacketImpl.CLUSTER_TOPOLOGY_V3)
         {
            ClusterTopologyChangeMessage topMessage = (ClusterTopologyChangeMessage) packet;
            notifyTopologyChange(topMessage);
         }
         else if (type == PacketImpl.CHECK_FOR_FAILOVER_REPLY)
         {
            System.out.println("Channel0Handler.handlePacket");
         }
      }

      /**
       * @param topMessage
       */
      private void notifyTopologyChange(final ClusterTopologyChangeMessage topMessage)
      {
         final long eventUID;
         final String backupGroupName;
         final String scaleDownGroupName;
         if (topMessage instanceof ClusterTopologyChangeMessage_V3)
         {
            eventUID = ((ClusterTopologyChangeMessage_V3) topMessage).getUniqueEventID();
            backupGroupName = ((ClusterTopologyChangeMessage_V3) topMessage).getBackupGroupName();
            scaleDownGroupName = ((ClusterTopologyChangeMessage_V3) topMessage).getScaleDownGroupName();
         }
         else if (topMessage instanceof ClusterTopologyChangeMessage_V2)
         {
            eventUID = ((ClusterTopologyChangeMessage_V2) topMessage).getUniqueEventID();
            backupGroupName = ((ClusterTopologyChangeMessage_V2) topMessage).getBackupGroupName();
            scaleDownGroupName = null;
         }
         else
         {
            eventUID = System.currentTimeMillis();
            backupGroupName = null;
            scaleDownGroupName = null;
         }

         if (topMessage.isExit())
         {
            if (HornetQClientLogger.LOGGER.isDebugEnabled())
            {
               HornetQClientLogger.LOGGER.debug("Notifying " + topMessage.getNodeID() + " going down");
            }

            if (callbackHandler != null)
               callbackHandler.notifyNodeDown(eventUID, topMessage.getNodeID());
         }
         else
         {
            Pair<TransportConfiguration, TransportConfiguration> transportConfig = topMessage.getPair();
            if (transportConfig.getA() == null && transportConfig.getB() == null)
            {
               transportConfig = new Pair<>(conn.getTransportConnection()
                                               .getConnectorConfig(),
                                            null);
            }

            if (callbackHandler != null)
               callbackHandler.notifyNodeUp(eventUID, topMessage.getNodeID(), backupGroupName, scaleDownGroupName, transportConfig, topMessage.isLast());
         }
      }
   }

   private void forceReturnChannel1()
   {
      if (connection != null)
      {
         Channel channel1 = connection.getChannel(1, -1);

         if (channel1 != null)
         {
            channel1.returnBlocking();
         }
      }
   }
}
