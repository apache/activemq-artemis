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

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.HornetQExceptionType;
import org.apache.activemq.api.core.HornetQInternalErrorException;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.protocol.core.Channel;
import org.apache.activemq.core.protocol.core.ChannelHandler;
import org.apache.activemq.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.core.protocol.core.ServerSessionPacketHandler;
import org.apache.activemq.core.protocol.core.impl.wireformat.CheckFailoverMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.CheckFailoverReplyMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.CreateQueueMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.HornetQExceptionMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.ReattachSessionMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.ReattachSessionResponseMessage;
import org.apache.activemq.core.security.HornetQPrincipal;
import org.apache.activemq.core.server.HornetQMessageBundle;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.ServerSession;
import org.apache.activemq.core.version.Version;

/**
 * A packet handler for all packets that need to be handled at the server level
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class HornetQPacketHandler implements ChannelHandler
{
   private final HornetQServer server;

   private final Channel channel1;

   private final CoreRemotingConnection connection;

   private final CoreProtocolManager protocolManager;

   public HornetQPacketHandler(final CoreProtocolManager protocolManager,
                               final HornetQServer server,
                               final Channel channel1,
                               final CoreRemotingConnection connection)
   {
      this.protocolManager = protocolManager;

      this.server = server;

      this.channel1 = channel1;

      this.connection = connection;
   }

   public void handlePacket(final Packet packet)
   {
      byte type = packet.getType();

      switch (type)
      {
         case PacketImpl.CREATESESSION:
         {
            CreateSessionMessage request = (CreateSessionMessage) packet;

            handleCreateSession(request);

            break;
         }
         case PacketImpl.CHECK_FOR_FAILOVER:
         {
            CheckFailoverMessage request = (CheckFailoverMessage) packet;

            handleCheckForFailover(request);

            break;
         }
         case PacketImpl.REATTACH_SESSION:
         {
            ReattachSessionMessage request = (ReattachSessionMessage) packet;

            handleReattachSession(request);

            break;
         }
         case PacketImpl.CREATE_QUEUE:
         {
            // Create queue can also be fielded here in the case of a replicated store and forward queue creation

            CreateQueueMessage request = (CreateQueueMessage) packet;

            handleCreateQueue(request);

            break;
         }
         default:
         {
            HornetQServerLogger.LOGGER.invalidPacket(packet);
         }
      }
   }

   private void handleCheckForFailover(CheckFailoverMessage failoverMessage)
   {
      String nodeID = failoverMessage.getNodeID();
      boolean okToFailover = nodeID == null ||
            !(server.getHAPolicy().canScaleDown() && !server.hasScaledDown(new SimpleString(nodeID)));
      channel1.send(new CheckFailoverReplyMessage(okToFailover));
   }

   private void handleCreateSession(final CreateSessionMessage request)
   {
      boolean incompatibleVersion = false;
      Packet response;
      try
      {
         Version version = server.getVersion();
         if (!version.isCompatible(request.getVersion()))
         {
            throw HornetQMessageBundle.BUNDLE.incompatibleClientServer();
         }

         if (!server.isStarted())
         {
            throw HornetQMessageBundle.BUNDLE.serverNotStarted();
         }

         // XXX HORNETQ-720 Taylor commented out this test. Should be verified.
         /*if (!server.checkActivate())
         {
            throw new HornetQException(HornetQException.SESSION_CREATION_REJECTED,
                                       "Server will not accept create session requests");
         }*/


         if (connection.getClientVersion() == 0)
         {
            connection.setClientVersion(request.getVersion());
         }
         else if (connection.getClientVersion() != request.getVersion())
         {
            HornetQServerLogger.LOGGER.incompatibleVersionAfterConnect(request.getVersion(), connection.getClientVersion());
         }

         Channel channel = connection.getChannel(request.getSessionChannelID(), request.getWindowSize());

         HornetQPrincipal hornetQPrincipal = null;

         if (request.getUsername() == null)
         {
            hornetQPrincipal = connection.getDefaultHornetQPrincipal();
         }

         ServerSession session = server.createSession(request.getName(),
                                                      hornetQPrincipal == null ? request.getUsername() : hornetQPrincipal.getUserName(),
                                                      hornetQPrincipal == null ? request.getPassword() : hornetQPrincipal.getPassword(),
                                                      request.getMinLargeMessageSize(),
                                                      connection,
                                                      request.isAutoCommitSends(),
                                                      request.isAutoCommitAcks(),
                                                      request.isPreAcknowledge(),
                                                      request.isXA(),
                                                      request.getDefaultAddress(),
                                                      new CoreSessionCallback(request.getName(),
                                                                              protocolManager,
                                                                              channel), null);

         ServerSessionPacketHandler handler = new ServerSessionPacketHandler(session,
                                                                             server.getStorageManager(),
                                                                             channel);
         channel.setHandler(handler);

         // TODO - where is this removed?
         protocolManager.addSessionHandler(request.getName(), handler);

         response = new CreateSessionResponseMessage(server.getVersion().getIncrementingVersion());
      }
      catch (HornetQException e)
      {
         if (e.getType() == HornetQExceptionType.INCOMPATIBLE_CLIENT_SERVER_VERSIONS)
         {
            incompatibleVersion = true;
            HornetQServerLogger.LOGGER.debug("Sending HornetQException after Incompatible client", e);
         }
         else
         {
            HornetQServerLogger.LOGGER.failedToCreateSession(e);
         }

         response = new HornetQExceptionMessage(e);
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.failedToCreateSession(e);

         response = new HornetQExceptionMessage(new HornetQInternalErrorException());
      }

      // send the exception to the client and destroy
      // the connection if the client and server versions
      // are not compatible
      if (incompatibleVersion)
      {
         channel1.sendAndFlush(response);
      }
      else
      {
         channel1.send(response);
      }
   }

   private void handleReattachSession(final ReattachSessionMessage request)
   {
      Packet response = null;

      try
      {

         if (!server.isStarted())
         {
            response = new ReattachSessionResponseMessage(-1, false);
         }

         HornetQServerLogger.LOGGER.debug("Reattaching request from " + connection.getRemoteAddress());


         ServerSessionPacketHandler sessionHandler = protocolManager.getSessionHandler(request.getName());

         // HORNETQ-720 XXX ataylor?
         if (/*!server.checkActivate() || */ sessionHandler == null)
         {
            response = new ReattachSessionResponseMessage(-1, false);
         }
         else
         {
            if (sessionHandler.getChannel().getConfirmationWindowSize() == -1)
            {
               // Even though session exists, we can't reattach since confi window size == -1,
               // i.e. we don't have a resend cache for commands, so we just close the old session
               // and let the client recreate

               HornetQServerLogger.LOGGER.reattachRequestFailed(connection.getRemoteAddress());

               sessionHandler.closeListeners();
               sessionHandler.close();

               response = new ReattachSessionResponseMessage(-1, false);
            }
            else
            {
               // Reconnect the channel to the new connection
               int serverLastConfirmedCommandID = sessionHandler.transferConnection(connection,
                                                                                    request.getLastConfirmedCommandID());

               response = new ReattachSessionResponseMessage(serverLastConfirmedCommandID, true);
            }
         }
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.failedToReattachSession(e);

         response = new HornetQExceptionMessage(new HornetQInternalErrorException());
      }

      channel1.send(response);
   }

   private void handleCreateQueue(final CreateQueueMessage request)
   {
      try
      {
         server.createQueue(request.getAddress(),
                            request.getQueueName(),
                            request.getFilterString(),
                            request.isDurable(),
                            request.isTemporary());
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.failedToHandleCreateQueue(e);
      }
   }
}