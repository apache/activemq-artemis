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
package org.apache.activemq.artemis.core.protocol.openwire;

import javax.jms.InvalidClientIDException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSSecurityException;
import javax.transaction.xa.XAResource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQCompositeConsumerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConnectionContext;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQProducerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQServerConsumer;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSession;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSingleConsumerBrokerExchange;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.SlowConsumerDetectionListener;
import org.apache.activemq.artemis.spi.core.protocol.AbstractRemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.IntegerResponse;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ConsumerState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.transport.TransmitCallback;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Represents an activemq connection.
 */
public class OpenWireConnection extends AbstractRemotingConnection implements SecurityAuth {

   private final OpenWireProtocolManager protocolManager;

   private boolean destroyed = false;

   private final Object sendLock = new Object();

   private final OpenWireFormat wireFormat;

   private AMQConnectionContext context;

   private final AtomicBoolean stopping = new AtomicBoolean(false);

   private boolean inServiceException;

   private final AtomicBoolean asyncException = new AtomicBoolean(false);

   // Clebert: Artemis session has meta-data support, perhaps we could reuse it here
   private Map<String, SessionId> sessionIdMap = new ConcurrentHashMap<>();


   private final Map<ConsumerId, AMQConsumerBrokerExchange> consumerExchanges = new HashMap<>();
   private final Map<ProducerId, AMQProducerBrokerExchange> producerExchanges = new HashMap<>();

   // Clebert TODO: Artemis already stores the Session. Why do we need a different one here
   private Map<SessionId, AMQSession> sessions = new ConcurrentHashMap<>();



   private ConnectionState state;

   private final Set<ActiveMQDestination> tempQueues = new ConcurrentHashSet<>();

   private Map<TransactionId, TransactionInfo> txMap = new ConcurrentHashMap<>();

   private volatile AMQSession advisorySession;

   // TODO-NOW: check on why there are two connections created for every createConnection on the client.
   public OpenWireConnection(Connection connection,
                             Executor executor,
                             OpenWireProtocolManager openWireProtocolManager,
                             OpenWireFormat wf) {
      super(connection, executor);
      this.protocolManager = openWireProtocolManager;
      this.wireFormat = wf;
   }

   // SecurityAuth implementation
   @Override
   public String getUsername() {
      ConnectionInfo info = getConnectionInfo();
      if (info == null) {
         return null;
      }
      return info.getUserName();
   }

   // SecurityAuth implementation
   @Override
   public RemotingConnection getRemotingConnection() {
      return this;
   }

   // SecurityAuth implementation
   @Override
   public String getPassword() {
      ConnectionInfo info = getConnectionInfo();
      if (info == null) {
         return null;
      }
      return info.getPassword();
   }

   private ConnectionInfo getConnectionInfo() {
      if (state == null) {
         return null;
      }
      ConnectionInfo info = state.getInfo();
      if (info == null) {
         return null;
      }
      return info;
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
      super.bufferReceived(connectionID, buffer);
      try {

         // TODO-NOW: set OperationContext

         Command command = (Command) wireFormat.unmarshal(buffer);

         boolean responseRequired = command.isResponseRequired();
         int commandId = command.getCommandId();


         // TODO-NOW: the server should send packets to the client based on the requested times
         //           need to look at what Andy did on AMQP

         // the connection handles pings, negotiations directly.
         // and delegate all other commands to manager.
         if (command.getClass() == KeepAliveInfo.class) {
            KeepAliveInfo info = (KeepAliveInfo) command;
            info.setResponseRequired(false);
            // if we don't respond to KeepAlive commands then the client will think the server is dead and timeout
            // for some reason KeepAliveInfo.isResponseRequired() is always false
            sendCommand(info);
         }
         else {
            Response response = null;

            try {
               setLastCommand(command);
               response = command.visit(commandProcessorInstance);
            }
            catch (Exception e) {
               if (responseRequired) {
                  response = new ExceptionResponse(e);
               }
            }
            finally {
               setLastCommand(null);
            }

            if (response instanceof ExceptionResponse) {
               if (!responseRequired) {
                  Throwable cause = ((ExceptionResponse) response).getException();
                  serviceException(cause);
                  response = null;
               }
            }

            if (responseRequired) {
               if (response == null) {
                  response = new Response();
                  response.setCorrelationId(command.getCommandId());
               }
            }

            // The context may have been flagged so that the response is not
            // sent.
            if (context != null) {
               if (context.isDontSendReponse()) {
                  context.setDontSendReponse(false);
                  response = null;
               }
            }

            // TODO-NOW: response through operation-context

            if (response != null && !protocolManager.isStopping()) {
               response.setCorrelationId(commandId);
               dispatchSync(response);
            }

         }
      }
      catch (Exception e) {
         ActiveMQServerLogger.LOGGER.debug(e);

         sendException(e);
      }
   }

   public void sendException(Exception e) {
      Response resp;
      if (e instanceof ActiveMQSecurityException) {
         resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
      }
      else if (e instanceof ActiveMQNonExistentQueueException) {
         resp = new ExceptionResponse(new InvalidDestinationException(e.getMessage()));
      }
      else {
         resp = new ExceptionResponse(e);
      }
      try {
         dispatch(resp);
      }
      catch (IOException e2) {
         ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e2);
      }
   }

   private void setLastCommand(Command command) {
      if (context != null) {
         context.setLastCommand(command);
      }
   }

   @Override
   public void destroy() {
      fail(null, null);
   }

   @Override
   public boolean isClient() {
      return false;
   }

   @Override
   public boolean isDestroyed() {
      return destroyed;
   }

   @Override
   public void disconnect(boolean criticalError) {
      this.disconnect(null, null, criticalError);
   }

   @Override
   public boolean checkDataReceived() {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   @Override
   public void flush() {
   }

   private void callFailureListeners(final ActiveMQException me) {
      final List<FailureListener> listenersClone = new ArrayList<>(failureListeners);

      for (final FailureListener listener : listenersClone) {
         try {
            listener.connectionFailed(me, false);
         }
         catch (final Throwable t) {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            ActiveMQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   // send a WireFormatInfo to the peer
   public void sendHandshake() {
      WireFormatInfo info = wireFormat.getPreferedWireFormatInfo();
      sendCommand(info);
   }

   public ConnectionState getState() {
      return state;
   }

   public void physicalSend(Command command) throws IOException {
      try {
         ByteSequence bytes = wireFormat.marshal(command);
         ActiveMQBuffer buffer = OpenWireUtil.toActiveMQBuffer(bytes);
         synchronized (sendLock) {
            getTransportConnection().write(buffer, false, false);
         }
      }
      catch (IOException e) {
         throw e;
      }
      catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.error("error sending", t);
      }

   }

   public void dispatchAsync(Command message) throws Exception {
      dispatchSync(message);
   }

   public void dispatchSync(Command message) throws Exception {
      processDispatch(message);
   }

   public void serviceException(Throwable e) throws Exception {
      ConnectionError ce = new ConnectionError();
      ce.setException(e);
      dispatchAsync(ce);
   }

   public void dispatch(Command command) throws IOException {
      this.physicalSend(command);
   }

   protected void processDispatch(Command command) throws IOException {
      MessageDispatch messageDispatch = (MessageDispatch) (command.isMessageDispatch() ? command : null);
      try {
         if (!stopping.get()) {
            if (messageDispatch != null) {
               protocolManager.preProcessDispatch(messageDispatch);
            }
            dispatch(command);
         }
      }
      catch (IOException e) {
         if (messageDispatch != null) {
            TransmitCallback sub = messageDispatch.getTransmitCallback();
            protocolManager.postProcessDispatch(messageDispatch);
            if (sub != null) {
               sub.onFailure();
            }
            messageDispatch = null;
            throw e;
         }
      }
      finally {
         if (messageDispatch != null) {
            TransmitCallback sub = messageDispatch.getTransmitCallback();
            protocolManager.postProcessDispatch(messageDispatch);
            if (sub != null) {
               sub.onSuccess();
            }
         }
      }
   }

   private void addConsumerBrokerExchange(ConsumerId id,
                                         AMQSession amqSession,
                                         List<AMQConsumer> consumerList) {
      AMQConsumerBrokerExchange result = consumerExchanges.get(id);
      if (result == null) {
         if (consumerList.size() == 1) {
            result = new AMQSingleConsumerBrokerExchange(amqSession, consumerList.get(0));
         }
         else {
            result = new AMQCompositeConsumerBrokerExchange(amqSession, consumerList);
         }
         synchronized (consumerExchanges) {
            consumerExchanges.put(id, result);
         }
      }
   }

   private AMQProducerBrokerExchange getProducerBrokerExchange(ProducerId id) throws IOException {
      AMQProducerBrokerExchange result = producerExchanges.get(id);
      if (result == null) {
         synchronized (producerExchanges) {
            result = new AMQProducerBrokerExchange();
            result.setConnectionContext(context);
            //todo implement reconnect https://issues.apache.org/jira/browse/ARTEMIS-194
            //todo: this used to check for  && this.acceptorUsed.isAuditNetworkProducers()
            if (context.isReconnect() || (context.isNetworkConnection())) {
               // once implemented ARTEMIS-194, we need to set the storedSequenceID here somehow
               // We have different semantics on Artemis Journal, but we could adapt something for this
               // TBD during the implemetnation of ARTEMIS-194
               result.setLastStoredSequenceId(0);
            }
            SessionState ss = state.getSessionState(id.getParentId());
            if (ss != null) {
               result.setProducerState(ss.getProducerState(id));
               ProducerState producerState = ss.getProducerState(id);
               if (producerState != null && producerState.getInfo() != null) {
                  ProducerInfo info = producerState.getInfo();
               }
            }
            producerExchanges.put(id, result);
         }
      }
      return result;
   }

   private void removeConsumerBrokerExchange(ConsumerId id) {
      synchronized (consumerExchanges) {
         consumerExchanges.remove(id);
      }
   }

   public void deliverMessage(MessageDispatch dispatch) {
      Message m = dispatch.getMessage();
      if (m != null) {
         long endTime = System.currentTimeMillis();
         m.setBrokerOutTime(endTime);
      }

      sendCommand(dispatch);
   }

   public WireFormat getMarshaller() {
      return this.wireFormat;
   }

   public void registerTempQueue(ActiveMQDestination queue) {
      tempQueues.add(queue);
   }

   private void shutdown(boolean fail) {
      if (fail) {
         transportConnection.forceClose();
      }
      else {
         transportConnection.close();
      }
   }

   private void disconnect(ActiveMQException me, String reason, boolean fail)  {

      if (context == null || destroyed) {
         return;
      }
      // Don't allow things to be added to the connection state while we
      // are shutting down.
      // is it necessary? even, do we need state at all?
      state.shutdown();

      // Then call the listeners
      // this should closes underlying sessions
      callFailureListeners(me);

      // this should clean up temp dests
      synchronized (sendLock) {
         callClosingListeners();
      }

      destroyed = true;

      //before closing transport, sendCommand the last response if any
      Command command = context.getLastCommand();
      if (command != null && command.isResponseRequired()) {
         Response lastResponse = new Response();
         lastResponse.setCorrelationId(command.getCommandId());
         try {
            dispatchSync(lastResponse);
         }
         catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
   }

   @Override
   public void disconnect(String reason, boolean fail) {
      this.disconnect(null, reason, fail);
   }

   @Override
   public void fail(ActiveMQException me, String message) {
      if (me != null) {
         ActiveMQServerLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());
      }
      try {
         protocolManager.removeConnection(this.getConnectionInfo(), me);
      }
      catch (InvalidClientIDException e) {
         ActiveMQServerLogger.LOGGER.warn("Couldn't close connection because invalid clientID", e);
      }
      shutdown(true);
   }

   public void setAdvisorySession(AMQSession amqSession) {
      this.advisorySession = amqSession;
   }

   public AMQSession getAdvisorySession() {
      return this.advisorySession;
   }

   public AMQConnectionContext getContext() {
      return this.context;
   }

   public void updateClient(ConnectionControl control) throws Exception {
      if (protocolManager.isUpdateClusterClients()) {
         dispatchAsync(control);
      }
   }

   public AMQConnectionContext initContext(ConnectionInfo info) {
      WireFormatInfo wireFormatInfo = wireFormat.getPreferedWireFormatInfo();
      // Older clients should have been defaulting this field to true.. but
      // they were not.
      if (wireFormatInfo != null && wireFormatInfo.getVersion() <= 2) {
         info.setClientMaster(true);
      }

      state = new ConnectionState(info);

      context = new AMQConnectionContext();

      state.reset(info);

      // Setup the context.
      String clientId = info.getClientId();
      context.setBroker(protocolManager);
      context.setClientId(clientId);
      context.setClientMaster(info.isClientMaster());
      context.setConnection(this);
      context.setConnectionId(info.getConnectionId());
      // for now we pass the manager as the connector and see what happens
      // it should be related to activemq's Acceptor
      context.setFaultTolerant(info.isFaultTolerant());
      context.setUserName(info.getUserName());
      context.setWireFormatInfo(wireFormatInfo);
      context.setReconnect(info.isFailoverReconnect());
      context.setConnectionState(state);
      if (info.getClientIp() == null) {
         info.setClientIp(getRemoteAddress());
      }

      return context;
   }

   //raise the refCount of context
   public void reconnect(AMQConnectionContext existingContext, ConnectionInfo info) {
      this.context = existingContext;
      WireFormatInfo wireFormatInfo = wireFormat.getPreferedWireFormatInfo();
      // Older clients should have been defaulting this field to true.. but
      // they were not.
      if (wireFormatInfo != null && wireFormatInfo.getVersion() <= 2) {
         info.setClientMaster(true);
      }
      if (info.getClientIp() == null) {
         info.setClientIp(getRemoteAddress());
      }

      state = new ConnectionState(info);
      state.reset(info);

      context.setConnection(this);
      context.setConnectionState(state);
      context.setClientMaster(info.isClientMaster());
      context.setFaultTolerant(info.isFaultTolerant());
      context.setReconnect(true);
      context.incRefCount();
   }

   /**
    * This will answer with commands to the client
    */
   public boolean sendCommand(final Command command) {
      if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
         ActiveMQServerLogger.LOGGER.trace("sending " + command);
      }

      if (isDestroyed()) {
         return false;
      }

      try {
         physicalSend(command);
      }
      catch (Exception e) {
         return false;
      }
      catch (Throwable t) {
         return false;
      }
      return true;
   }

   public void addDestination(DestinationInfo info) throws Exception {
      ActiveMQDestination dest = info.getDestination();
      if (dest.isQueue()) {
         SimpleString qName = OpenWireUtil.toCoreAddress(dest);
         QueueBinding binding = (QueueBinding) protocolManager.getServer().getPostOffice().getBinding(qName);
         if (binding == null) {
            if (getState().getInfo() != null) {

               CheckType checkType = dest.isTemporary() ? CheckType.CREATE_NON_DURABLE_QUEUE : CheckType.CREATE_DURABLE_QUEUE;
               protocolManager.getServer().getSecurityStore().check(qName, checkType, this);

               protocolManager.getServer().checkQueueCreationLimit(getUsername());
            }
            ConnectionInfo connInfo = getState().getInfo();
            protocolManager.getServer().createQueue(qName, qName, null, connInfo == null ? null : SimpleString.toSimpleString(connInfo.getUserName()), false, dest.isTemporary());
         }

         if (dest.isTemporary()) {
            registerTempQueue(dest);
         }
      }

      if (!AdvisorySupport.isAdvisoryTopic(dest)) {
         AMQConnectionContext context = getContext();
         DestinationInfo advInfo = new DestinationInfo(context.getConnectionId(), DestinationInfo.ADD_OPERATION_TYPE, dest);

         ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(dest);
         protocolManager.fireAdvisory(context, topic, advInfo);
      }
   }


   public void updateConsumer(ConsumerControl consumerControl) {
      SessionId sessionId = consumerControl.getConsumerId().getParentId();
      AMQSession amqSession = sessions.get(sessionId);
      amqSession.updateConsumerPrefetchSize(consumerControl.getConsumerId(), consumerControl.getPrefetch());
   }

   public void addConsumer(ConsumerInfo info) throws Exception {
      // Todo: add a destination interceptors holder here (amq supports this)
      SessionId sessionId = info.getConsumerId().getParentId();
      ConnectionId connectionId = sessionId.getParentId();
      ConnectionState cs = getState();
      if (cs == null) {
         throw new IllegalStateException("Cannot add a consumer to a connection that had not been registered: " + connectionId);
      }
      SessionState ss = cs.getSessionState(sessionId);
      if (ss == null) {
         throw new IllegalStateException(protocolManager.getServer() + " Cannot add a consumer to a session that had not been registered: " + sessionId);
      }
      // Avoid replaying dup commands
      if (!ss.getConsumerIds().contains(info.getConsumerId())) {

         AMQSession amqSession = sessions.get(sessionId);
         if (amqSession == null) {
            throw new IllegalStateException("Session not exist! : " + sessionId);
         }

         List<AMQConsumer> consumersList = amqSession.createConsumer(info, amqSession, new SlowConsumerDetection());

         this.addConsumerBrokerExchange(info.getConsumerId(), amqSession, consumersList);
         ss.addConsumer(info);
         amqSession.start();
      }
   }

   class SlowConsumerDetection implements SlowConsumerDetectionListener {

      @Override
      public void onSlowConsumer(ServerConsumer consumer) {
         if (consumer instanceof AMQServerConsumer) {
            AMQServerConsumer serverConsumer = (AMQServerConsumer)consumer;
            ActiveMQTopic topic = AdvisorySupport.getSlowConsumerAdvisoryTopic(serverConsumer.getAmqConsumer().getOpenwireDestination());
            ActiveMQMessage advisoryMessage = new ActiveMQMessage();
            try {
               advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_CONSUMER_ID, serverConsumer.getAmqConsumer().getId().toString());
               protocolManager.fireAdvisory(context, topic, advisoryMessage, serverConsumer.getAmqConsumer().getId());
            }
            catch (Exception e) {
               // TODO-NOW: LOGGING
               e.printStackTrace();
            }
         }
      }
   }

   public void addSessions(Set<SessionId> sessionSet) {
      Iterator<SessionId> iter = sessionSet.iterator();
      while (iter.hasNext()) {
         SessionId sid = iter.next();
         addSession(getState().getSessionState(sid).getInfo(), true);
      }
   }

   public AMQSession addSession(SessionInfo ss) {
      return addSession(ss, false);
   }

   public AMQSession addSession(SessionInfo ss, boolean internal) {
      AMQSession amqSession = new AMQSession(getState().getInfo(), ss, protocolManager.getServer(), this, protocolManager.getScheduledPool(), protocolManager);
      amqSession.initialize();
      amqSession.setInternal(internal);
      sessions.put(ss.getSessionId(), amqSession);
      sessionIdMap.put(amqSession.getCoreSession().getName(), ss.getSessionId());
      return amqSession;
   }

   public void removeSession(AMQConnectionContext context, SessionInfo info) throws Exception {
      AMQSession session = sessions.remove(info.getSessionId());
      if (session != null) {
         session.close();
      }
   }

   public AMQSession getSession(SessionId sessionId) {
      return sessions.get(sessionId);
   }

   public void removeDestination(ActiveMQDestination dest) throws Exception {
      if (dest.isQueue()) {
         SimpleString qName = new SimpleString("jms.queue." + dest.getPhysicalName());
         protocolManager.getServer().destroyQueue(qName);
      }
      else {
         Bindings bindings = protocolManager.getServer().getPostOffice().getBindingsForAddress(SimpleString.toSimpleString("jms.topic." + dest.getPhysicalName()));
         Iterator<Binding> iterator = bindings.getBindings().iterator();

         while (iterator.hasNext()) {
            Queue b = (Queue) iterator.next().getBindable();
            if (b.getConsumerCount() > 0) {
               throw new Exception("Destination still has an active subscription: " + dest.getPhysicalName());
            }
            if (b.isDurable()) {
               throw new Exception("Destination still has durable subscription: " + dest.getPhysicalName());
            }
            b.deleteQueue();
         }
      }

      if (!AdvisorySupport.isAdvisoryTopic(dest)) {
         AMQConnectionContext context = getContext();
         DestinationInfo advInfo = new DestinationInfo(context.getConnectionId(), DestinationInfo.REMOVE_OPERATION_TYPE, dest);

         ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(dest);
         protocolManager.fireAdvisory(context, topic, advInfo);
      }
   }

   /**
    * Checks to see if this destination exists.  If it does not throw an invalid destination exception.
    *
    * @param destination
    */
   private void validateDestination(ActiveMQDestination destination) throws Exception {
      if (destination.isQueue()) {
         SimpleString physicalName = OpenWireUtil.toCoreAddress(destination);
         BindingQueryResult result = protocolManager.getServer().bindingQuery(physicalName);
         if (!result.isExists() && !result.isAutoCreateJmsQueues()) {
            throw ActiveMQMessageBundle.BUNDLE.noSuchQueue(physicalName);
         }
      }
   }


   CommandProcessor commandProcessorInstance = new CommandProcessor();


   // This will listen for commands throught the protocolmanager
   public class CommandProcessor implements CommandVisitor {

      public AMQConnectionContext getContext() {
         return OpenWireConnection.this.getContext();
      }

      @Override
      public Response processAddConnection(ConnectionInfo info) throws Exception {
         try {
            protocolManager.addConnection(OpenWireConnection.this, info);
         }
         catch (Exception e) {
            Response resp = new ExceptionResponse(e);
            return resp;
         }
         if (info.isManageable() && protocolManager.isUpdateClusterClients()) {
            // send ConnectionCommand
            ConnectionControl command = protocolManager.newConnectionControl();
            command.setFaultTolerant(protocolManager.isFaultTolerantConfiguration());
            if (info.isFailoverReconnect()) {
               command.setRebalanceConnection(false);
            }
            dispatchAsync(command);
         }
         return null;

      }

      @Override
      public Response processAddProducer(ProducerInfo info) throws Exception {
         SessionId sessionId = info.getProducerId().getParentId();
         ConnectionState cs = getState();

         if (cs == null) {
            throw new IllegalStateException("Cannot add a producer to a connection that had not been registered: " + sessionId.getParentId());
         }

         SessionState ss = cs.getSessionState(sessionId);
         if (ss == null) {
            throw new IllegalStateException("Cannot add a producer to a session that had not been registered: " + sessionId);
         }

         // Avoid replaying dup commands
         if (!ss.getProducerIds().contains(info.getProducerId())) {
            ActiveMQDestination destination = info.getDestination();

            if (destination != null && !AdvisorySupport.isAdvisoryTopic(destination)) {
               if (destination.isQueue()) {
                  OpenWireConnection.this.validateDestination(destination);
               }
               DestinationInfo destInfo = new DestinationInfo(getContext().getConnectionId(), DestinationInfo.ADD_OPERATION_TYPE, destination);
               OpenWireConnection.this.addDestination(destInfo);
            }

            ss.addProducer(info);

         }
         return null;
      }

      @Override
      public Response processAddConsumer(ConsumerInfo info) throws Exception {
         addConsumer(info);
         return null;
      }

      @Override
      public Response processRemoveDestination(DestinationInfo info) throws Exception {
         ActiveMQDestination dest = info.getDestination();
         removeDestination(dest);
         return null;
      }

      @Override
      public Response processRemoveProducer(ProducerId id) throws Exception {

         // TODO-now: proper implement this method
         return null;
      }

      @Override
      public Response processRemoveSession(SessionId id, long lastDeliveredSequenceId) throws Exception {
         SessionState session = state.getSessionState(id);
         if (session == null) {
            throw new IllegalStateException("Cannot remove session that had not been registered: " + id);
         }
         // Don't let new consumers or producers get added while we are closing
         // this down.
         session.shutdown();
         // Cascade the connection stop producers.
         // we don't stop consumer because in core
         // closing the session will do the job
         for (ProducerId producerId : session.getProducerIds()) {
            try {
               processRemoveProducer(producerId);
            }
            catch (Throwable e) {
               // LOG.warn("Failed to remove producer: {}", producerId, e);
            }
         }
         state.removeSession(id);
         removeSession(context, session.getInfo());
         return null;
      }

      @Override
      public Response processRemoveSubscription(RemoveSubscriptionInfo subInfo) throws Exception {
         protocolManager.removeSubscription(subInfo);
         return null;
      }

      @Override
      public Response processRollbackTransaction(TransactionInfo info) throws Exception {
         protocolManager.rollbackTransaction(info);
         TransactionId txId = info.getTransactionId();
         txMap.remove(txId);
         return null;
      }

      @Override
      public Response processShutdown(ShutdownInfo info) throws Exception {
         OpenWireConnection.this.shutdown(false);
         return null;
      }

      @Override
      public Response processWireFormat(WireFormatInfo command) throws Exception {
         wireFormat.renegotiateWireFormat(command);
         //throw back a brokerInfo here
         protocolManager.sendBrokerInfo(OpenWireConnection.this);
         return null;
      }

      @Override
      public Response processAddDestination(DestinationInfo dest) throws Exception {
         Response resp = null;
         try {
            addDestination(dest);
         }
         catch (Exception e) {
            if (e instanceof ActiveMQSecurityException) {
               resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
            }
            else {
               resp = new ExceptionResponse(e);
            }
         }
         return resp;
      }

      @Override
      public Response processAddSession(SessionInfo info) throws Exception {
         // Avoid replaying dup commands
         if (!state.getSessionIds().contains(info.getSessionId())) {
            addSession(info);
            state.addSession(info);
         }
         return null;
      }

      @Override
      public Response processBeginTransaction(TransactionInfo info) throws Exception {
         TransactionId txId = info.getTransactionId();

         if (!txMap.containsKey(txId)) {
            txMap.put(txId, info);
         }
         return null;
      }

      @Override
      public Response processBrokerInfo(BrokerInfo arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception {
         try {
            protocolManager.commitTransactionOnePhase(info);
            TransactionId txId = info.getTransactionId();
            txMap.remove(txId);
         }
         catch (Exception e) {
            e.printStackTrace();
            throw e;
         }

         return null;
      }

      @Override
      public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception {
         protocolManager.commitTransactionTwoPhase(info);
         TransactionId txId = info.getTransactionId();
         txMap.remove(txId);

         return null;
      }

      @Override
      public Response processConnectionControl(ConnectionControl connectionControl) throws Exception {
         //activemq5 keeps a var to remember only the faultTolerant flag
         //this can be sent over a reconnected transport as the first command
         //before restoring the connection.
         return null;
      }

      @Override
      public Response processConnectionError(ConnectionError arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processConsumerControl(ConsumerControl consumerControl) throws Exception {
         //amq5 clients send this command to restore prefetchSize
         //after successful reconnect
         try {
            updateConsumer(consumerControl);
         }
         catch (Exception e) {
            //log error
         }
         return null;
      }

      @Override
      public Response processControlCommand(ControlCommand arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processEndTransaction(TransactionInfo info) throws Exception {
         protocolManager.endTransaction(info);
         TransactionId txId = info.getTransactionId();

         if (!txMap.containsKey(txId)) {
            txMap.put(txId, info);
         }
         return null;
      }

      @Override
      public Response processFlush(FlushCommand arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processForgetTransaction(TransactionInfo info) throws Exception {
         TransactionId txId = info.getTransactionId();
         txMap.remove(txId);

         protocolManager.forgetTransaction(info.getTransactionId());
         return null;
      }

      @Override
      public Response processKeepAlive(KeepAliveInfo arg0) throws Exception {
         throw new IllegalStateException("not implemented! ");
      }

      @Override
      public Response processMessage(Message messageSend) throws Exception {
         ProducerId producerId = messageSend.getProducerId();
         AMQProducerBrokerExchange producerExchange = getProducerBrokerExchange(producerId);
         final AMQConnectionContext pcontext = producerExchange.getConnectionContext();
         final ProducerInfo producerInfo = producerExchange.getProducerState().getInfo();
         boolean sendProducerAck = !messageSend.isResponseRequired() && producerInfo.getWindowSize() > 0 && !pcontext.isInRecoveryMode();

         AMQSession session = getSession(producerId.getParentId());

         session.send(producerInfo, messageSend, sendProducerAck);
         return null;
      }


      @Override
      public Response processMessageAck(MessageAck ack) throws Exception {
         AMQConsumerBrokerExchange consumerBrokerExchange = consumerExchanges.get(ack.getConsumerId());
         consumerBrokerExchange.acknowledge(ack);
         return null;
      }

      @Override
      public Response processMessageDispatch(MessageDispatch arg0) throws Exception {
         return null;
      }

      @Override
      public Response processMessageDispatchNotification(MessageDispatchNotification arg0) throws Exception {
         return null;
      }

      @Override
      public Response processMessagePull(MessagePull arg0) throws Exception {
         AMQConsumerBrokerExchange amqConsumerBrokerExchange = consumerExchanges.get(arg0.getConsumerId());
         if (amqConsumerBrokerExchange == null) {
            throw new IllegalStateException("Consumer does not exist");
         }
         amqConsumerBrokerExchange.processMessagePull(arg0);
         return null;
      }

      @Override
      public Response processPrepareTransaction(TransactionInfo info) throws Exception {
         protocolManager.prepareTransaction(info);
         //activemq needs a rdonly response
         return new IntegerResponse(XAResource.XA_RDONLY);
      }

      @Override
      public Response processProducerAck(ProducerAck arg0) throws Exception {
         // a broker doesn't do producers.. this shouldn't happen
         return null;
      }

      @Override
      public Response processRecoverTransactions(TransactionInfo info) throws Exception {
         Set<SessionId> sIds = state.getSessionIds();


         List<TransactionId> recovered = new ArrayList<>();
         if (sIds != null) {
            for (SessionId sid : sIds) {
               AMQSession s = sessions.get(sid);
               if (s != null) {
                  s.recover(recovered);
               }
            }
         }

         return new DataArrayResponse(recovered.toArray(new TransactionId[0]));
      }

      @Override
      public Response processRemoveConnection(ConnectionId id, long lastDeliveredSequenceId) throws Exception {
         //we let protocol manager to handle connection add/remove
         try {
            protocolManager.removeConnection(state.getInfo(), null);
         }
         catch (Throwable e) {
            // log
         }
         return null;
      }

      @Override
      public Response processRemoveConsumer(ConsumerId id, long lastDeliveredSequenceId) throws Exception {
         SessionId sessionId = id.getParentId();
         SessionState ss = state.getSessionState(sessionId);
         if (ss == null) {
            throw new IllegalStateException("Cannot remove a consumer from a session that had not been registered: " + sessionId);
         }
         ConsumerState consumerState = ss.removeConsumer(id);
         if (consumerState == null) {
            throw new IllegalStateException("Cannot remove a consumer that had not been registered: " + id);
         }
         ConsumerInfo info = consumerState.getInfo();
         info.setLastDeliveredSequenceId(lastDeliveredSequenceId);

         AMQConsumerBrokerExchange consumerBrokerExchange = consumerExchanges.get(id);

         consumerBrokerExchange.removeConsumer();

         removeConsumerBrokerExchange(id);

         return null;
      }

   }

}
