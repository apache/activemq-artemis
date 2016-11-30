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
import org.apache.activemq.artemis.core.exception.ActiveMQXAException;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.StorageManager;
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
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
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

   private final boolean direct;

   public ServerSessionPacketHandler(final ServerSession session,
                                     final StorageManager storageManager,
                                     final Channel channel) {
      this.session = session;

      this.storageManager = storageManager;

      this.channel = channel;

      this.remotingConnection = channel.getConnection();

      //TODO think of a better way of doing this
      Connection conn = remotingConnection.getTransportConnection();

      if (conn instanceof NettyConnection) {
         direct = ((NettyConnection) conn).isDirectDeliver();
      } else {
         direct = false;
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

      try {
         session.close(true);
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.errorClosingSession(e);
      }

      ActiveMQServerLogger.LOGGER.clearingUpSession(session.getName());
   }

   public void close() {
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
      byte type = packet.getType();

      storageManager.setContext(session.getSessionContext());

      Packet response = null;
      boolean flush = false;
      boolean closeChannel = false;
      boolean requiresResponse = false;

      if (logger.isTraceEnabled()) {
         logger.trace("ServerSessionPacketHandler::handlePacket," + packet);
      }

      try {
         try {
            switch (type) {
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
                  session.createQueue(request.getAddress(), request.getQueueName(), request.getRoutingType(), request.getFilterString(), request.isTemporary(), request.isDurable(), request.getMaxConsumers(), request.isDeleteOnNoConsumers(),
                                      request.isAutoCreated());
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
                  session.createSharedQueue(request.getAddress(), request.getQueueName(), request.getRoutingType(), request.isDurable(), request.getFilterString());
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
                  BindingQueryResult result = session.executeBindingQuery(request.getAddress());
                  if (channel.supports(PacketImpl.SESS_BINDINGQUERY_RESP_V3)) {
                     response = new SessionBindingQueryResponseMessage_V3(result.isExists(), result.getQueueNames(), result.isAutoCreateJmsQueues(), result.isAutoCreateJmsTopics());
                  } else if (channel.supports(PacketImpl.SESS_BINDINGQUERY_RESP_V2)) {
                     response = new SessionBindingQueryResponseMessage_V2(result.isExists(), result.getQueueNames(), result.isAutoCreateJmsQueues());
                  } else {
                     response = new SessionBindingQueryResponseMessage(result.isExists(), result.getQueueNames());
                  }
                  break;
               }
               case SESS_ACKNOWLEDGE: {
                  SessionAcknowledgeMessage message = (SessionAcknowledgeMessage) packet;
                  requiresResponse = message.isRequiresResponse();
                  session.acknowledge(message.getConsumerID(), message.getMessageID());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
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
               case SESS_FLOWTOKEN: {
                  SessionConsumerFlowCreditMessage message = (SessionConsumerFlowCreditMessage) packet;
                  session.receiveConsumerCredits(message.getConsumerID(), message.getCredits());
                  break;
               }
               case SESS_SEND: {
                  SessionSendMessage message = (SessionSendMessage) packet;
                  requiresResponse = message.isRequiresResponse();
                  session.send((ServerMessage) message.getMessage(), direct);
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case SESS_SEND_LARGE: {
                  SessionSendLargeMessage message = (SessionSendLargeMessage) packet;
                  session.sendLarge(message.getLargeMessage());
                  break;
               }
               case SESS_SEND_CONTINUATION: {
                  SessionSendContinuationMessage message = (SessionSendContinuationMessage) packet;
                  requiresResponse = message.isRequiresResponse();
                  session.sendContinuations(message.getPacketSize(), message.getMessageBodySize(), message.getBody(), message.isContinues());
                  if (requiresResponse) {
                     response = new NullResponseMessage();
                  }
                  break;
               }
               case SESS_FORCE_CONSUMER_DELIVERY: {
                  SessionForceConsumerDelivery message = (SessionForceConsumerDelivery) packet;
                  session.forceConsumerDelivery(message.getConsumerID(), message.getSequence());
                  break;
               }
               case PacketImpl.SESS_PRODUCER_REQUEST_CREDITS: {
                  SessionRequestProducerCreditsMessage message = (SessionRequestProducerCreditsMessage) packet;
                  session.requestProducerCredits(message.getAddress(), message.getCredits());
                  break;
               }
               case PacketImpl.SESS_ADD_METADATA: {
                  response = new NullResponseMessage();
                  SessionAddMetaDataMessage message = (SessionAddMetaDataMessage) packet;
                  session.addMetaData(message.getKey(), message.getData());
                  break;
               }
               case PacketImpl.SESS_ADD_METADATA2: {
                  SessionAddMetaDataMessageV2 message = (SessionAddMetaDataMessageV2) packet;
                  if (message.isRequiresConfirmations()) {
                     response = new NullResponseMessage();
                  }
                  session.addMetaData(message.getKey(), message.getData());
                  break;
               }
               case PacketImpl.SESS_UNIQUE_ADD_METADATA: {
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
            getSession().markTXFailed(e);
            if (requiresResponse) {
               logger.debug("Sending exception to client", e);
               response = new ActiveMQExceptionMessage(e);
            } else {
               ActiveMQServerLogger.LOGGER.caughtException(e);
            }
         } catch (ActiveMQXAException e) {
            if (requiresResponse) {
               logger.debug("Sending exception to client", e);
               response = new SessionXAResponseMessage(true, e.errorCode, e.getMessage());
            } else {
               ActiveMQServerLogger.LOGGER.caughtXaException(e);
            }
         } catch (ActiveMQQueueMaxConsumerLimitReached e) {
            if (requiresResponse) {
               logger.debug("Sending exception to client", e);
               response = new ActiveMQExceptionMessage(e);
            } else {
               ActiveMQServerLogger.LOGGER.caughtException(e);
            }
         } catch (ActiveMQException e) {
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
         } catch (Throwable t) {
            getSession().markTXFailed(t);
            if (requiresResponse) {
               ActiveMQServerLogger.LOGGER.warn("Sending unexpected exception to the client", t);
               ActiveMQException activeMQInternalErrorException = new ActiveMQInternalErrorException();
               activeMQInternalErrorException.initCause(t);
               response = new ActiveMQExceptionMessage(activeMQInternalErrorException);
            } else {
               ActiveMQServerLogger.LOGGER.caughtException(t);
            }
         }

         sendResponse(packet, response, flush, closeChannel);
      } finally {
         storageManager.clearContext();
      }
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
               logger.trace("ServerSessionPacketHandler::response sent::" + response);
            }

         }

         @Override
         public void done() {
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
}
