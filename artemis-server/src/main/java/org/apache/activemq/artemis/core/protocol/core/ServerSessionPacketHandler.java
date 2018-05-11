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
package org.apache.activemq.artemis.core.protocol.core;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueMaxConsumerLimitReached;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.exception.ActiveMQXAException;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.protocol.core.impl.CoreProtocolManager;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ActiveMQExceptionMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateAddressMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateQueueMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateQueueMessage_V2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateSharedQueueMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.CreateSharedQueueMessage_V2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.NullResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.RollbackMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionAcknowledgeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionAddMetaDataMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionAddMetaDataMessageV2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionBindingQueryMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionBindingQueryResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionBindingQueryResponseMessage_V2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionBindingQueryResponseMessage_V3;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionBindingQueryResponseMessage_V4;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionConsumerCloseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionCreateConsumerMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionDeleteQueueMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionExpireMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionForceConsumerDelivery;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionIndividualAcknowledgeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionQueueQueryMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionQueueQueryResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionQueueQueryResponseMessage_V2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionQueueQueryResponseMessage_V3;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionRequestProducerCreditsMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendContinuationMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendLargeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionUniqueAddMetaDataMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAAfterFailedMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXACommitMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAEndMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAForgetMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAJoinMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAPrepareMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAResumeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXARollbackMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXASetTimeoutMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionXAStartMessage;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.spi.core.protocol.EmbedMessageUtil;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.SimpleFuture;
import org.apache.activemq.artemis.utils.SimpleFutureImpl;
import org.apache.activemq.artemis.utils.actors.Actor;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.CREATE_ADDRESS;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.CREATE_QUEUE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.CREATE_QUEUE_V2;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.CREATE_SHARED_QUEUE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.CREATE_SHARED_QUEUE_V2;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.DELETE_QUEUE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_ACKNOWLEDGE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_BINDINGQUERY;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_CLOSE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_COMMIT;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_CONSUMER_CLOSE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_CREATECONSUMER;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_EXPIRED;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_FLOWTOKEN;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_FORCE_CONSUMER_DELIVERY;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_INDIVIDUAL_ACKNOWLEDGE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_PRODUCER_REQUEST_CREDITS;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_QUEUEQUERY;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_ROLLBACK;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_SEND;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_SEND_CONTINUATION;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_SEND_LARGE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_START;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_STOP;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_COMMIT;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_END;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_FAILED;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_FORGET;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_GET_TIMEOUT;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_INDOUBT_XIDS;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_JOIN;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_PREPARE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_RESUME;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_ROLLBACK;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_SET_TIMEOUT;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_START;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_XA_SUSPEND;

public class ServerSessionPacketHandler implements ChannelHandler {

   private static final Logger logger = Logger.getLogger(ServerSessionPacketHandler.class);

   private final ServerSession session;

   private final StorageManager storageManager;

   private final Channel channel;

   private volatile CoreRemotingConnection remotingConnection;

   private final Actor<Packet> packetActor;

   private final ArtemisExecutor callExecutor;

   private final CoreProtocolManager manager;

   // The current currentLargeMessage being processed
   private volatile LargeServerMessage currentLargeMessage;

   private final boolean direct;


   public ServerSessionPacketHandler(final ActiveMQServer server,
                                     final CoreProtocolManager manager,
                                     final ServerSession session,
                                     final StorageManager storageManager,
                                     final Channel channel) {
      this.manager = manager;

      this.session = session;

      session.addCloseable((boolean failed) -> clearLargeMessage());

      this.storageManager = storageManager;

      this.channel = channel;

      this.remotingConnection = channel.getConnection();

      Connection conn = remotingConnection.getTransportConnection();

      this.callExecutor = server.getExecutorFactory().getExecutor();

      // In an optimized way packetActor should use the threadPool as the parent executor
      // directly from server.getThreadPool();
      // However due to how transferConnection is handled we need to
      // use the same executor
      this.packetActor = new Actor<>(callExecutor, this::onMessagePacket);

      if (conn instanceof NettyConnection) {
         direct = ((NettyConnection) conn).isDirectDeliver();
      } else {
         direct = false;
      }
   }

   private void clearLargeMessage() {
      if (currentLargeMessage != null) {
         try {
            currentLargeMessage.deleteFile();
         } catch (Throwable error) {
            ActiveMQServerLogger.LOGGER.errorDeletingLargeMessageFile(error);
         }
      }
   }

   public ServerSession getSession() {
      return session;
   }

   public long getID() {
      return channel.getID();
   }

   public void connectionFailed(final ActiveMQException exception, boolean failedOver) {
      ActiveMQServerLogger.LOGGER.clientConnectionFailed(session.getName());

      closeExecutors();

      try {
         session.close(true);
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorClosingSession(e);
      }

      ActiveMQServerLogger.LOGGER.clearingUpSession(session.getName());
   }

   public void closeExecutors() {
      packetActor.shutdown();
      callExecutor.shutdown();
   }

   public void close() {
      closeExecutors();

      channel.flushConfirmations();

      try {
         session.close(false);
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorClosingSession(e);
      }
   }

   public Channel getChannel() {
      return channel;
   }

   @Override
   public void handlePacket(final Packet packet) {

      // This method will call onMessagePacket through an actor
      packetActor.act(packet);
   }

   private void onMessagePacket(final Packet packet) {
      if (logger.isTraceEnabled()) {
         logger.trace("ServerSessionPacketHandler::handlePacket," + packet);
      }
      final byte type = packet.getType();
      switch (type) {
         case SESS_SEND: {
            onSessionSend(packet);
            break;
         }
         case SESS_ACKNOWLEDGE: {
            onSessionAcknowledge(packet);
            break;
         }
         case SESS_PRODUCER_REQUEST_CREDITS: {
            onSessionRequestProducerCredits(packet);
            break;
         }
         case SESS_FLOWTOKEN: {
            onSessionConsumerFlowCredit(packet);
            break;
         }
         default:
            // separating a method for everything else as JIT was faster this way
            slowPacketHandler(packet);
            break;
      }
   }

   // This is being separated from onMessagePacket as JIT was more efficient with a small method for the
   // hot executions.
   private void slowPacketHandler(final Packet packet) {
      final byte type = packet.getType();
      storageManager.setContext(session.getSessionContext());

      Packet response = null;
      boolean flush = false;
      boolean closeChannel = false;
      boolean requiresResponse = false;

      try {
         try {
            switch (type) {
               case SESS_SEND_LARGE: {
                  SessionSendLargeMessage message = (SessionSendLargeMessage) packet;
                  sendLarge(message.getLargeMessage());
                  break;
               }
               case SESS_SEND_CONTINUATION: {
                  SessionSendContinuationMessage message = (SessionSendContinuationMessage) packet;
                  requiresResponse = message.isRequiresResponse();
                  sendContinuations(message.getPacketSize(), message.getMessageBodySize(), message.getBody(), message.isContinues());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case SESS_CREATECONSUMER: {
                  SessionCreateConsumerMessage request = (SessionCreateConsumerMessage) packet;
                  requiresResponse = request.isRequiresResponse();
                  session.createConsumer(request.getID(), request.getQueueName(), request.getFilterString(), request.isBrowseOnly());
                  if (requiresResponse) {
                     // We send back queue information on the queue as a response- this allows the queue to
                     // be automatically recreated on failover
                     QueueQueryResult queueQueryResult = session.executeQueueQuery(request.getQueueName());

                     if (channel.supports(PacketImpl.SESS_QUEUEQUERY_RESP_V3)) {
                        response = new SessionQueueQueryResponseMessage_V3(queueQueryResult);
                     } else if (channel.supports(PacketImpl.SESS_QUEUEQUERY_RESP_V2)) {
                        response = new SessionQueueQueryResponseMessage_V2(queueQueryResult);
                     } else {
                        response = new SessionQueueQueryResponseMessage(queueQueryResult);
                     }
                  }

                  break;
               }
               case CREATE_ADDRESS: {
                  CreateAddressMessage request = (CreateAddressMessage) packet;
                  requiresResponse = request.isRequiresResponse();
                  session.createAddress(request.getAddress(), request.getRoutingTypes(), request.isAutoCreated());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case CREATE_QUEUE: {
                  CreateQueueMessage request = (CreateQueueMessage) packet;
                  requiresResponse = request.isRequiresResponse();
                  session.createQueue(request.getAddress(), request.getQueueName(), RoutingType.MULTICAST, request.getFilterString(), request.isTemporary(), request.isDurable());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case CREATE_QUEUE_V2: {
                  CreateQueueMessage_V2 request = (CreateQueueMessage_V2) packet;
                  requiresResponse = request.isRequiresResponse();
                  session.createQueue(request.getAddress(), request.getQueueName(), request.getRoutingType(), request.getFilterString(), request.isTemporary(), request.isDurable(), request.getMaxConsumers(), request.isPurgeOnNoConsumers(),
                                      request.isExclusive(), request.isLastValue(), request.isAutoCreated());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case CREATE_SHARED_QUEUE: {
                  CreateSharedQueueMessage request = (CreateSharedQueueMessage) packet;
                  requiresResponse = request.isRequiresResponse();
                  session.createSharedQueue(request.getAddress(), request.getQueueName(), request.isDurable(), request.getFilterString());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case CREATE_SHARED_QUEUE_V2: {
                  CreateSharedQueueMessage_V2 request = (CreateSharedQueueMessage_V2) packet;
                  requiresResponse = request.isRequiresResponse();
                  session.createSharedQueue(request.getAddress(), request.getQueueName(), request.getRoutingType(), request.getFilterString(), request.isDurable(), request.getMaxConsumers(), request.isPurgeOnNoConsumers(), request.isExclusive(), request.isLastValue());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case DELETE_QUEUE: {
                  requiresResponse = true;
                  SessionDeleteQueueMessage request = (SessionDeleteQueueMessage) packet;
                  session.deleteQueue(request.getQueueName());
                  response = new NullResponseMessage();
                  break;
               }
               case SESS_QUEUEQUERY: {
                  requiresResponse = true;
                  SessionQueueQueryMessage request = (SessionQueueQueryMessage) packet;
                  QueueQueryResult result = session.executeQueueQuery(request.getQueueName());

                  if (result.isExists() && remotingConnection.getChannelVersion() < PacketImpl.ADDRESSING_CHANGE_VERSION) {
                     result.setAddress(SessionQueueQueryMessage.getOldPrefixedAddress(result.getAddress(), result.getRoutingType()));
                  }

                  if (channel.supports(PacketImpl.SESS_QUEUEQUERY_RESP_V3)) {
                     response = new SessionQueueQueryResponseMessage_V3(result);
                  } else if (channel.supports(PacketImpl.SESS_QUEUEQUERY_RESP_V2)) {
                     response = new SessionQueueQueryResponseMessage_V2(result);
                  } else {
                     response = new SessionQueueQueryResponseMessage(result);
                  }
                  break;
               }
               case SESS_BINDINGQUERY: {
                  requiresResponse = true;
                  SessionBindingQueryMessage request = (SessionBindingQueryMessage) packet;
                  final int clientVersion = remotingConnection.getChannelVersion();
                  BindingQueryResult result = session.executeBindingQuery(request.getAddress());

                  /* if the session is JMS and it's from an older client then we need to add the old prefix to the queue
                   * names otherwise the older client won't realize the queue exists and will try to create it and receive
                   * an error
                   */
                  if (result.isExists() && clientVersion < PacketImpl.ADDRESSING_CHANGE_VERSION && session.getMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY) != null) {
                     final List<SimpleString> queueNames = result.getQueueNames();
                     if (!queueNames.isEmpty()) {
                        final List<SimpleString> convertedQueueNames = request.convertQueueNames(clientVersion, queueNames);
                        if (convertedQueueNames != queueNames) {
                           result = new BindingQueryResult(result.isExists(), result.getAddressInfo(), convertedQueueNames, result.isAutoCreateQueues(), result.isAutoCreateAddresses(), result.isDefaultPurgeOnNoConsumers(), result.getDefaultMaxConsumers(), result.isDefaultExclusive(), result.isDefaultLastValue());
                        }
                     }
                  }

                  if (channel.supports(PacketImpl.SESS_BINDINGQUERY_RESP_V4)) {
                     response = new SessionBindingQueryResponseMessage_V4(result.isExists(), result.getQueueNames(), result.isAutoCreateQueues(), result.isAutoCreateAddresses(), result.isDefaultPurgeOnNoConsumers(), result.getDefaultMaxConsumers(), result.isDefaultExclusive(), result.isDefaultLastValue());
                  } else if (channel.supports(PacketImpl.SESS_BINDINGQUERY_RESP_V3)) {
                     response = new SessionBindingQueryResponseMessage_V3(result.isExists(), result.getQueueNames(), result.isAutoCreateQueues(), result.isAutoCreateAddresses());
                  } else if (channel.supports(PacketImpl.SESS_BINDINGQUERY_RESP_V2)) {
                     response = new SessionBindingQueryResponseMessage_V2(result.isExists(), result.getQueueNames(), result.isAutoCreateQueues());
                  } else {
                     response = new SessionBindingQueryResponseMessage(result.isExists(), result.getQueueNames());
                  }
                  break;
               }
               case SESS_EXPIRED: {
                  SessionExpireMessage message = (SessionExpireMessage) packet;
                  session.expire(message.getConsumerID(), message.getMessageID());
                  break;
               }
               case SESS_COMMIT: {
                  requiresResponse = true;
                  session.commit();
                  response = new NullResponseMessage();
                  break;
               }
               case SESS_ROLLBACK: {
                  requiresResponse = true;
                  session.rollback(((RollbackMessage) packet).isConsiderLastMessageAsDelivered());
                  response = new NullResponseMessage();
                  break;
               }
               case SESS_XA_COMMIT: {
                  requiresResponse = true;
                  SessionXACommitMessage message = (SessionXACommitMessage) packet;
                  session.xaCommit(message.getXid(), message.isOnePhase());
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_END: {
                  requiresResponse = true;
                  SessionXAEndMessage message = (SessionXAEndMessage) packet;
                  session.xaEnd(message.getXid());
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_FORGET: {
                  requiresResponse = true;
                  SessionXAForgetMessage message = (SessionXAForgetMessage) packet;
                  session.xaForget(message.getXid());
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_JOIN: {
                  requiresResponse = true;
                  SessionXAJoinMessage message = (SessionXAJoinMessage) packet;
                  session.xaJoin(message.getXid());
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_RESUME: {
                  requiresResponse = true;
                  SessionXAResumeMessage message = (SessionXAResumeMessage) packet;
                  session.xaResume(message.getXid());
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_ROLLBACK: {
                  requiresResponse = true;
                  SessionXARollbackMessage message = (SessionXARollbackMessage) packet;
                  session.xaRollback(message.getXid());
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_START: {
                  requiresResponse = true;
                  SessionXAStartMessage message = (SessionXAStartMessage) packet;
                  session.xaStart(message.getXid());
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_FAILED: {
                  requiresResponse = true;
                  SessionXAAfterFailedMessage message = (SessionXAAfterFailedMessage) packet;
                  session.xaFailed(message.getXid());
                  // no response on this case
                  break;
               }
               case SESS_XA_SUSPEND: {
                  requiresResponse = true;
                  session.xaSuspend();
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_PREPARE: {
                  requiresResponse = true;
                  SessionXAPrepareMessage message = (SessionXAPrepareMessage) packet;
                  session.xaPrepare(message.getXid());
                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  break;
               }
               case SESS_XA_INDOUBT_XIDS: {
                  requiresResponse = true;
                  List<Xid> xids = session.xaGetInDoubtXids();
                  response = new SessionXAGetInDoubtXidsResponseMessage(xids);
                  break;
               }
               case SESS_XA_GET_TIMEOUT: {
                  requiresResponse = true;
                  int timeout = session.xaGetTimeout();
                  response = new SessionXAGetTimeoutResponseMessage(timeout);
                  break;
               }
               case SESS_XA_SET_TIMEOUT: {
                  requiresResponse = true;
                  SessionXASetTimeoutMessage message = (SessionXASetTimeoutMessage) packet;
                  session.xaSetTimeout(message.getTimeoutSeconds());
                  response = new SessionXASetTimeoutResponseMessage(true);
                  break;
               }
               case SESS_START: {
                  session.start();
                  break;
               }
               case SESS_STOP: {
                  requiresResponse = true;
                  session.stop();
                  response = new NullResponseMessage();
                  break;
               }
               case SESS_CLOSE: {
                  requiresResponse = true;
                  session.close(false);
                  // removeConnectionListeners();
                  response = new NullResponseMessage();
                  flush = true;
                  closeChannel = true;
                  break;
               }
               case SESS_INDIVIDUAL_ACKNOWLEDGE: {
                  SessionIndividualAcknowledgeMessage message = (SessionIndividualAcknowledgeMessage) packet;
                  requiresResponse = message.isRequiresResponse();
                  session.individualAcknowledge(message.getConsumerID(), message.getMessageID());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case SESS_CONSUMER_CLOSE: {
                  requiresResponse = true;
                  SessionConsumerCloseMessage message = (SessionConsumerCloseMessage) packet;
                  session.closeConsumer(message.getConsumerID());
                  response = new NullResponseMessage();
                  break;
               }
               case SESS_FORCE_CONSUMER_DELIVERY: {
                  SessionForceConsumerDelivery message = (SessionForceConsumerDelivery) packet;
                  session.forceConsumerDelivery(message.getConsumerID(), message.getSequence());
                  break;
               }
               case PacketImpl.SESS_ADD_METADATA: {
                  response = new NullResponseMessage();
                  SessionAddMetaDataMessage message = (SessionAddMetaDataMessage) packet;
                  session.addMetaData(message.getKey(), message.getData());
                  break;
               }
               case PacketImpl.SESS_ADD_METADATA2: {
                  requiresResponse = true;
                  SessionAddMetaDataMessageV2 message = (SessionAddMetaDataMessageV2) packet;
                  if (message.isRequiresConfirmations()) {
                     response = new NullResponseMessage();
                  }
                  session.addMetaData(message.getKey(), message.getData());
                  break;
               }
               case PacketImpl.SESS_UNIQUE_ADD_METADATA: {
                  requiresResponse = true;
                  SessionUniqueAddMetaDataMessage message = (SessionUniqueAddMetaDataMessage) packet;
                  if (session.addUniqueMetaData(message.getKey(), message.getData())) {
                     response = new NullResponseMessage();
                  } else {
                     response = new ActiveMQExceptionMessage(ActiveMQMessageBundle.BUNDLE.duplicateMetadata(message.getKey(), message.getData()));
                  }
                  break;
               }
            }
         } catch (ActiveMQIOErrorException e) {
            response = onActiveMQIOErrorExceptionWhileHandlePacket(e, requiresResponse, response, this.session);
         } catch (ActiveMQXAException e) {
            response = onActiveMQXAExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQQueueMaxConsumerLimitReached e) {
            response = onActiveMQQueueMaxConsumerLimitReachedWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQException e) {
            response = onActiveMQExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (Throwable t) {
            response = onCatchThrowableWhileHandlePacket(t, requiresResponse, response, this.session);
         }
         sendResponse(packet, response, flush, closeChannel);
      } finally {
         storageManager.clearContext();
      }
   }

   private void onSessionAcknowledge(Packet packet) {
      this.storageManager.setContext(session.getSessionContext());
      try {
         Packet response = null;
         boolean requiresResponse = false;
         try {
            final SessionAcknowledgeMessage message = (SessionAcknowledgeMessage) packet;
            requiresResponse = message.isRequiresResponse();
            this.session.acknowledge(message.getConsumerID(), message.getMessageID());
            if (requiresResponse) {
               response = new NullResponseMessage();
            }
         } catch (ActiveMQIOErrorException e) {
            response = onActiveMQIOErrorExceptionWhileHandlePacket(e, requiresResponse, response, this.session);
         } catch (ActiveMQXAException e) {
            response = onActiveMQXAExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQQueueMaxConsumerLimitReached e) {
            response = onActiveMQQueueMaxConsumerLimitReachedWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQException e) {
            response = onActiveMQExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (Throwable t) {
            response = onCatchThrowableWhileHandlePacket(t, requiresResponse, response, this.session);
         }
         sendResponse(packet, response, false, false);
      } finally {
         this.storageManager.clearContext();
      }
   }

   private void onSessionSend(Packet packet) {
      this.storageManager.setContext(session.getSessionContext());
      try {
         Packet response = null;
         boolean requiresResponse = false;
         try {
            final SessionSendMessage message = (SessionSendMessage) packet;
            requiresResponse = message.isRequiresResponse();
            this.session.send(EmbedMessageUtil.extractEmbedded(message.getMessage()), this.direct);
            if (requiresResponse) {
               response = new NullResponseMessage();
            }
         } catch (ActiveMQIOErrorException e) {
            response = onActiveMQIOErrorExceptionWhileHandlePacket(e, requiresResponse, response, this.session);
         } catch (ActiveMQXAException e) {
            response = onActiveMQXAExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQQueueMaxConsumerLimitReached e) {
            response = onActiveMQQueueMaxConsumerLimitReachedWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQException e) {
            response = onActiveMQExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (Throwable t) {
            response = onCatchThrowableWhileHandlePacket(t, requiresResponse, response, this.session);
         }
         sendResponse(packet, response, false, false);
      } finally {
         this.storageManager.clearContext();
      }
   }

   private void onSessionRequestProducerCredits(Packet packet) {
      this.storageManager.setContext(session.getSessionContext());
      try {
         Packet response = null;
         boolean requiresResponse = false;
         try {
            SessionRequestProducerCreditsMessage message = (SessionRequestProducerCreditsMessage) packet;
            session.requestProducerCredits(message.getAddress(), message.getCredits());
         } catch (ActiveMQIOErrorException e) {
            response = onActiveMQIOErrorExceptionWhileHandlePacket(e, requiresResponse, response, this.session);
         } catch (ActiveMQXAException e) {
            response = onActiveMQXAExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQQueueMaxConsumerLimitReached e) {
            response = onActiveMQQueueMaxConsumerLimitReachedWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQException e) {
            response = onActiveMQExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (Throwable t) {
            response = onCatchThrowableWhileHandlePacket(t, requiresResponse, response, this.session);
         }
         sendResponse(packet, response, false, false);
      } finally {
         this.storageManager.clearContext();
      }
   }

   private void onSessionConsumerFlowCredit(Packet packet) {
      this.storageManager.setContext(session.getSessionContext());
      try {
         Packet response = null;
         boolean requiresResponse = false;
         try {
            SessionConsumerFlowCreditMessage message = (SessionConsumerFlowCreditMessage) packet;
            session.receiveConsumerCredits(message.getConsumerID(), message.getCredits());
         } catch (ActiveMQIOErrorException e) {
            response = onActiveMQIOErrorExceptionWhileHandlePacket(e, requiresResponse, response, this.session);
         } catch (ActiveMQXAException e) {
            response = onActiveMQXAExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQQueueMaxConsumerLimitReached e) {
            response = onActiveMQQueueMaxConsumerLimitReachedWhileHandlePacket(e, requiresResponse, response);
         } catch (ActiveMQException e) {
            response = onActiveMQExceptionWhileHandlePacket(e, requiresResponse, response);
         } catch (Throwable t) {
            response = onCatchThrowableWhileHandlePacket(t, requiresResponse, response, this.session);
         }
         sendResponse(packet, response, false, false);
      } finally {
         this.storageManager.clearContext();
      }
   }


   private static Packet onActiveMQIOErrorExceptionWhileHandlePacket(ActiveMQIOErrorException e,
                                                                     boolean requiresResponse,
                                                                     Packet response,
                                                                     ServerSession session) {
      session.markTXFailed(e);
      if (requiresResponse) {
         logger.debug("Sending exception to client", e);
         response = new ActiveMQExceptionMessage(e);
      } else {
         ActiveMQServerLogger.LOGGER.caughtException(e);
      }
      return response;
   }

   private static Packet onActiveMQXAExceptionWhileHandlePacket(ActiveMQXAException e,
                                                                boolean requiresResponse,
                                                                Packet response) {
      if (requiresResponse) {
         logger.debug("Sending exception to client", e);
         response = new SessionXAResponseMessage(true, e.errorCode, e.getMessage());
      } else {
         ActiveMQServerLogger.LOGGER.caughtXaException(e);
      }
      return response;
   }

   private static Packet onActiveMQQueueMaxConsumerLimitReachedWhileHandlePacket(ActiveMQQueueMaxConsumerLimitReached e,
                                                                                 boolean requiresResponse,
                                                                                 Packet response) {
      if (requiresResponse) {
         logger.debug("Sending exception to client", e);
         response = new ActiveMQExceptionMessage(e);
      } else {
         ActiveMQServerLogger.LOGGER.caughtException(e);
      }
      return response;
   }

   private static Packet onActiveMQExceptionWhileHandlePacket(ActiveMQException e,
                                                              boolean requiresResponse,
                                                              Packet response) {
      if (requiresResponse) {
         logger.debug("Sending exception to client", e);
         response = new ActiveMQExceptionMessage(e);
      } else {
         if (e.getType() == ActiveMQExceptionType.QUEUE_EXISTS) {
            logger.debug("Caught exception", e);
         } else {
            ActiveMQServerLogger.LOGGER.caughtException(e);
         }
      }
      return response;
   }

   private static Packet onCatchThrowableWhileHandlePacket(Throwable t,
                                                           boolean requiresResponse,
                                                           Packet response,
                                                           ServerSession session) {
      session.markTXFailed(t);
      if (requiresResponse) {
         ActiveMQServerLogger.LOGGER.sendingUnexpectedExceptionToClient(t);
         ActiveMQException activeMQInternalErrorException = new ActiveMQInternalErrorException();
         activeMQInternalErrorException.initCause(t);
         response = new ActiveMQExceptionMessage(activeMQInternalErrorException);
      } else {
         ActiveMQServerLogger.LOGGER.caughtException(t);
      }
      return response;
   }



   private void sendResponse(final Packet confirmPacket,
                             final Packet response,
                             final boolean flush,
                             final boolean closeChannel) {
      if (logger.isTraceEnabled()) {
         logger.trace("ServerSessionPacketHandler::scheduling response::" + response);
      }

      storageManager.afterCompleteOperations(new IOCallback() {
         @Override
         public void onError(final int errorCode, final String errorMessage) {
            ActiveMQServerLogger.LOGGER.errorProcessingIOCallback(errorCode, errorMessage);

            ActiveMQExceptionMessage exceptionMessage = new ActiveMQExceptionMessage(ActiveMQExceptionType.createException(errorCode, errorMessage));

            doConfirmAndResponse(confirmPacket, exceptionMessage, flush, closeChannel);

            if (logger.isTraceEnabled()) {
               logger.trace("ServerSessionPacketHandler::exception response sent::" + exceptionMessage);
            }

         }

         @Override
         public void done() {
            if (logger.isTraceEnabled()) {
               logger.trace("ServerSessionPacketHandler::regular response sent::" + response);
            }

            doConfirmAndResponse(confirmPacket, response, flush, closeChannel);
         }
      });
   }

   private void doConfirmAndResponse(final Packet confirmPacket,
                                     final Packet response,
                                     final boolean flush,
                                     final boolean closeChannel) {
      if (confirmPacket != null) {
         channel.confirm(confirmPacket);

         if (flush) {
            channel.flushConfirmations();
         }
      }

      if (response != null) {
         channel.send(response);
      }

      if (closeChannel) {
         channel.close();
      }
   }

   public void closeListeners() {
      List<CloseListener> listeners = remotingConnection.removeCloseListeners();

      for (CloseListener closeListener : listeners) {
         closeListener.connectionClosed();
         if (closeListener instanceof FailureListener) {
            remotingConnection.removeFailureListener((FailureListener) closeListener);
         }
      }
   }

   public int transferConnection(final CoreRemotingConnection newConnection, final int lastReceivedCommandID) {

      SimpleFuture<Integer> future = new SimpleFutureImpl<>();
      callExecutor.execute(() -> {
         int value = internaltransferConnection(newConnection, lastReceivedCommandID);
         future.set(value);
      });

      try {
         return future.get().intValue();
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }
   }

   private int internaltransferConnection(final CoreRemotingConnection newConnection, final int lastReceivedCommandID) {
      // We need to disable delivery on all the consumers while the transfer is occurring- otherwise packets might get
      // delivered
      // after the channel has transferred but *before* packets have been replayed - this will give the client the wrong
      // sequence of packets.
      // It is not sufficient to just stop the session, since right after stopping the session, another session start
      // might be executed
      // before we have transferred the connection, leaving it in a started state
      session.setTransferring(true);

      List<CloseListener> closeListeners = remotingConnection.removeCloseListeners();
      List<FailureListener> failureListeners = remotingConnection.removeFailureListeners();

      // Note. We do not destroy the replicating connection here. In the case the live server has really crashed
      // then the connection will get cleaned up anyway when the server ping timeout kicks in.
      // In the case the live server is really still up, i.e. a split brain situation (or in tests), then closing
      // the replicating connection will cause the outstanding responses to be be replayed on the live server,
      // if these reach the client who then subsequently fails over, on reconnection to backup, it will have
      // received responses that the backup did not know about.

      channel.transferConnection(newConnection);

      newConnection.syncIDGeneratorSequence(remotingConnection.getIDGeneratorSequence());

      Connection oldTransportConnection = remotingConnection.getTransportConnection();

      remotingConnection = newConnection;

      remotingConnection.setCloseListeners(closeListeners);
      remotingConnection.setFailureListeners(failureListeners);

      int serverLastReceivedCommandID = channel.getLastConfirmedCommandID();

      channel.replayCommands(lastReceivedCommandID);

      channel.setTransferring(false);

      session.setTransferring(false);

      // We do this because the old connection could be out of credits on netty
      // this will force anything to resume after the reattach through the ReadyListener callbacks
      oldTransportConnection.fireReady(true);

      return serverLastReceivedCommandID;
   }

   // Large Message is part of the core protocol, we have these functions here as part of Packet handler
   private void sendLarge(final Message message) throws Exception {
      // need to create the LargeMessage before continue
      long id = storageManager.generateID();

      LargeServerMessage largeMsg = storageManager.createLargeMessage(id, message);

      if (logger.isTraceEnabled()) {
         logger.trace("sendLarge::" + largeMsg);
      }

      if (currentLargeMessage != null) {
         ActiveMQServerLogger.LOGGER.replacingIncompleteLargeMessage(currentLargeMessage.getMessageID());
      }

      currentLargeMessage = largeMsg;
   }

   private void sendContinuations(final int packetSize,
                                  final long messageBodySize,
                                  final byte[] body,
                                  final boolean continues) throws Exception {
      if (currentLargeMessage == null) {
         throw ActiveMQMessageBundle.BUNDLE.largeMessageNotInitialised();
      }

      // Immediately release the credits for the continuations- these don't contribute to the in-memory size
      // of the message

      currentLargeMessage.addBytes(body);

      if (!continues) {
         currentLargeMessage.releaseResources();

         if (messageBodySize >= 0) {
            currentLargeMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, messageBodySize);
         }

         session.doSend(session.getCurrentTransaction(), currentLargeMessage, null, false, false);

         currentLargeMessage = null;
      }
   }

}
