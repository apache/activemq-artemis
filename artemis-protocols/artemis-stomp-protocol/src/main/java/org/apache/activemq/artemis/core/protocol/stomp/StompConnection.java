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
package org.apache.activemq.artemis.core.protocol.stomp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.stomp.v10.StompFrameHandlerV10;
import org.apache.activemq.artemis.core.protocol.stomp.v12.StompFrameHandlerV12;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.VersionLoader;

import javax.security.auth.Subject;

import static org.apache.activemq.artemis.core.protocol.stomp.ActiveMQStompProtocolMessageBundle.BUNDLE;

public final class StompConnection implements RemotingConnection {

   protected static final String CONNECTION_ID_PROP = "__AMQ_CID";
   private static final String SERVER_NAME = "ActiveMQ-Artemis/" + VersionLoader.getVersion().getFullVersion() +
      " ActiveMQ Artemis Messaging Engine";

   private final StompProtocolManager manager;

   private final Connection transportConnection;

   private String login;

   private String passcode;

   private String clientID;

   //this means login is valid. (stomp connection ok)
   private boolean valid;

   private boolean destroyed = false;

   private final long creationTime;

   private final Acceptor acceptorUsed;

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<>();

   private final Object failLock = new Object();

   private boolean dataReceived;

   private final boolean enableMessageID;

   private final int minLargeMessageSize;

   private StompVersions version;

   private VersionedStompFrameHandler frameHandler;

   //this means the version negotiation done.
   private boolean initialized;

   private FrameEventListener stompListener;

   private final Object sendLock = new Object();

   private final ScheduledExecutorService scheduledExecutorService;

   private final ExecutorFactory factory;

   @Override
   public boolean isSupportReconnect() {
      return false;
   }

   public VersionedStompFrameHandler getStompVersionHandler() {
      return frameHandler;
   }

   public StompFrame decode(ActiveMQBuffer buffer) throws ActiveMQStompException {
      StompFrame frame = null;
      try {
         frame = frameHandler.decode(buffer);
      } catch (ActiveMQStompException e) {
         switch (e.getCode()) {
            case ActiveMQStompException.INVALID_EOL_V10:
               if (version != null)
                  throw e;
               frameHandler = new StompFrameHandlerV12(this, scheduledExecutorService, factory);
               buffer.resetReaderIndex();
               frame = decode(buffer);
               break;
            case ActiveMQStompException.INVALID_COMMAND:
            case ActiveMQStompException.UNDEFINED_ESCAPE:
               frameHandler.onError(e);
               break;
            default:
               throw e;
         }
      }
      return frame;
   }

   @Override
   public boolean isWritable(ReadyListener callback) {
      return transportConnection.isWritable(callback);
   }

   public boolean hasBytes() {
      return frameHandler.hasBytes();
   }

   StompConnection(final Acceptor acceptorUsed,
                   final Connection transportConnection,
                   final StompProtocolManager manager,
                   final ScheduledExecutorService scheduledExecutorService,
                   final ExecutorFactory factory) {
      this.scheduledExecutorService = scheduledExecutorService;

      this.factory = factory;

      this.transportConnection = transportConnection;

      this.manager = manager;

      this.frameHandler = new StompFrameHandlerV10(this, scheduledExecutorService, factory);

      this.creationTime = System.currentTimeMillis();

      this.acceptorUsed = acceptorUsed;

      this.enableMessageID = ConfigurationHelper.getBooleanProperty(TransportConstants.STOMP_ENABLE_MESSAGE_ID, false, acceptorUsed.getConfiguration());
      this.minLargeMessageSize = ConfigurationHelper.getIntProperty(TransportConstants.STOMP_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, acceptorUsed.getConfiguration());
   }

   @Override
   public void addFailureListener(final FailureListener listener) {
      if (listener == null) {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   @Override
   public boolean removeFailureListener(final FailureListener listener) {
      if (listener == null) {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   @Override
   public void addCloseListener(final CloseListener listener) {
      if (listener == null) {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      closeListeners.add(listener);
   }

   @Override
   public boolean removeCloseListener(final CloseListener listener) {
      if (listener == null) {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      return closeListeners.remove(listener);
   }

   @Override
   public List<CloseListener> removeCloseListeners() {
      List<CloseListener> ret = new ArrayList<>(closeListeners);

      closeListeners.clear();

      return ret;
   }

   @Override
   public List<FailureListener> removeFailureListeners() {
      List<FailureListener> ret = new ArrayList<>(failureListeners);

      failureListeners.clear();

      return ret;
   }

   @Override
   public void setCloseListeners(List<CloseListener> listeners) {
      closeListeners.clear();

      closeListeners.addAll(listeners);
   }

   @Override
   public void setFailureListeners(final List<FailureListener> listeners) {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   protected synchronized void setDataReceived() {
      dataReceived = true;
   }

   @Override
   public synchronized boolean checkDataReceived() {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   // TODO this should take a type - send or receive so it knows whether to check the address or the queue
   public void checkDestination(String destination) throws ActiveMQStompException {
      if (!manager.destinationExists(getSession().getCoreSession().removePrefix(SimpleString.toSimpleString(destination)).toString())) {
         throw BUNDLE.destinationNotExist(destination).setHandler(frameHandler);
      }
   }

   public void autoCreateDestinationIfPossible(String queue, RoutingType routingType) throws ActiveMQStompException {
      ServerSession session = getSession().getCoreSession();

      try {
         SimpleString simpleQueue = SimpleString.toSimpleString(queue);
         if (manager.getServer().getAddressInfo(simpleQueue) == null) {
            AddressSettings addressSettings = manager.getServer().getAddressSettingsRepository().getMatch(queue);

            RoutingType effectiveAddressRoutingType = routingType == null ? addressSettings.getDefaultAddressRoutingType() : routingType;
            if (addressSettings.isAutoCreateAddresses()) {
               session.createAddress(simpleQueue, effectiveAddressRoutingType, true);
            }

            // only auto create the queue if the address is ANYCAST
            if (effectiveAddressRoutingType == RoutingType.ANYCAST && addressSettings.isAutoCreateQueues()) {
               session.createQueue(simpleQueue, simpleQueue, routingType == null ? addressSettings.getDefaultQueueRoutingType() : routingType, null, false, true, true);
            }
         }
      } catch (ActiveMQQueueExistsException e) {
         // ignore
      } catch (Exception e) {
         throw new ActiveMQStompException(e.getMessage(), e).setHandler(frameHandler);
      }
   }

   public void checkRoutingSemantics(String destination, RoutingType routingType) throws ActiveMQStompException {
      AddressInfo addressInfo = manager.getServer().getAddressInfo(getSession().getCoreSession().removePrefix(SimpleString.toSimpleString(destination)));

      // may be null here if, for example, the management address is being checked
      if (addressInfo != null) {
         Set<RoutingType> actualDeliveryModesOfAddress = addressInfo.getRoutingTypes();
         if (routingType != null && !actualDeliveryModesOfAddress.contains(routingType)) {
            throw BUNDLE.illegalSemantics(routingType.toString(), actualDeliveryModesOfAddress.toString());
         }
      }
   }

   @Override
   public ActiveMQBuffer createTransportBuffer(int size) {
      return ActiveMQBuffers.dynamicBuffer(size);
   }

   @Override
   public void destroy() {
      synchronized (failLock) {
         if (destroyed) {
            return;
         }
      }

      destroyed = true;

      internalClose();

      synchronized (sendLock) {
         callClosingListeners();
      }
   }

   public Acceptor getAcceptorUsed() {
      return acceptorUsed;
   }

   private void internalClose() {
      transportConnection.close();

      manager.cleanup(this);
   }

   @Override
   public void fail(final ActiveMQException me) {
      synchronized (failLock) {
         if (destroyed) {
            return;
         }

         StompFrame frame = frameHandler.createStompFrame(Stomp.Responses.ERROR);
         frame.addHeader(Stomp.Headers.Error.MESSAGE, me.getMessage());
         sendFrame(frame, null);

         destroyed = true;
      }

      ActiveMQServerLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());

      if (frameHandler != null) {
         frameHandler.disconnect();
      }

      // Then call the listeners
      callFailureListeners(me);

      callClosingListeners();

      internalClose();
   }

   @Override
   public void fail(final ActiveMQException me, String scaleDownTargetNodeID) {
      fail(me);
   }

   @Override
   public void flush() {
   }

   @Override
   public List<FailureListener> getFailureListeners() {
      // we do not return the listeners otherwise the remoting service
      // would NOT destroy the connection.
      return Collections.emptyList();
   }

   @Override
   public Object getID() {
      return transportConnection.getID();
   }

   @Override
   public String getRemoteAddress() {
      return transportConnection.getRemoteAddress();
   }

   @Override
   public long getCreationTime() {
      return creationTime;
   }

   @Override
   public Connection getTransportConnection() {
      return transportConnection;
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
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
      manager.handleBuffer(this, buffer);
   }

   public String getLogin() {
      return login;
   }

   public String getPasscode() {
      return passcode;
   }

   @Override
   public void setClientID(String clientID) {
      this.clientID = clientID;
   }

   @Override
   public String getClientID() {
      return clientID;
   }

   public boolean isValid() {
      return valid;
   }

   public void setValid(boolean valid) {
      this.valid = valid;
   }

   private void callFailureListeners(final ActiveMQException me) {
      final List<FailureListener> listenersClone = new ArrayList<>(failureListeners);

      for (final FailureListener listener : listenersClone) {
         try {
            listener.connectionFailed(me, false);
         } catch (final Throwable t) {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            ActiveMQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   private void callClosingListeners() {
      final List<CloseListener> listenersClone = new ArrayList<>(closeListeners);

      for (final CloseListener listener : listenersClone) {
         try {
            listener.connectionClosed();
         } catch (final Throwable t) {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            ActiveMQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   /*
    * accept-version value takes form of "v1,v2,v3..."
    * we need to return the highest supported version
    */
   public void negotiateVersion(StompFrame frame) throws ActiveMQStompException {
      String acceptVersion = frame.getHeader(Stomp.Headers.ACCEPT_VERSION);

      if (acceptVersion == null) {
         this.version = StompVersions.V1_0;
      } else {
         StringTokenizer tokenizer = new StringTokenizer(acceptVersion, ",");
         Set<String> requestVersions = new HashSet<>(tokenizer.countTokens());
         while (tokenizer.hasMoreTokens()) {
            requestVersions.add(tokenizer.nextToken());
         }

         if (requestVersions.contains(StompVersions.V1_2.toString())) {
            this.version = StompVersions.V1_2;
         } else if (requestVersions.contains(StompVersions.V1_1.toString())) {
            this.version = StompVersions.V1_1;
         } else if (requestVersions.contains(StompVersions.V1_0.toString())) {
            this.version = StompVersions.V1_0;
         } else {
            //not a supported version!
            ActiveMQStompException error = BUNDLE.versionNotSupported(acceptVersion).setHandler(frameHandler);
            error.addHeader(Stomp.Headers.Error.VERSION, manager.getSupportedVersionsAsErrorVersion());
            error.addHeader(Stomp.Headers.CONTENT_TYPE, "text/plain");
            error.setBody("Supported protocol versions are " + manager.getSupportedVersionsAsString());
            error.setDisconnect(true);
            throw error;
         }
      }

      if (this.version != (StompVersions.V1_0)) {
         VersionedStompFrameHandler newHandler = VersionedStompFrameHandler.getHandler(this, this.version, scheduledExecutorService, factory);
         newHandler.initDecoder(this.frameHandler);
         this.frameHandler = newHandler;
      }
      this.initialized = true;
   }

   //reject if the host doesn't match
   public void setHost(String host) throws ActiveMQStompException {
      if (host == null) {
         ActiveMQStompException error = BUNDLE.nullHostHeader().setHandler(frameHandler);
         error.setBody(BUNDLE.hostCannotBeNull());
         throw error;
      }

      String localHost = manager.getVirtualHostName();
      if (!host.equals(localHost)) {
         ActiveMQStompException error = BUNDLE.hostNotMatch().setHandler(frameHandler);
         error.setBody(BUNDLE.hostNotMatchDetails(host));
         throw error;
      }
   }

   public void handleFrame(StompFrame request) {
      StompFrame reply = null;

      if (stompListener != null) {
         stompListener.requestAccepted(request);
      }

      String cmd = request.getCommand();
      try {
         if (isDestroyed()) {
            throw BUNDLE.connectionDestroyed().setHandler(frameHandler);
         }
         if (!initialized) {
            if (!(Stomp.Commands.CONNECT.equals(cmd) || Stomp.Commands.STOMP.equals(cmd))) {
               throw BUNDLE.connectionNotEstablished().setHandler(frameHandler);
            }
            //decide version
            negotiateVersion(request);
         }

         reply = frameHandler.handleFrame(request);
      } catch (ActiveMQStompException e) {
         reply = e.getFrame();
      }

      if (reply != null) {
         sendFrame(reply, null);
      }

      if (Stomp.Commands.DISCONNECT.equals(cmd)) {
         this.disconnect(false);
      }
   }

   public void sendFrame(StompFrame frame, StompPostReceiptFunction function) {
      manager.sendReply(this, frame, function);
   }

   public boolean validateUser(final String login, final String pass, final RemotingConnection connection) {
      this.valid = manager.validateUser(login, pass, connection);
      if (valid) {
         this.login = login;
         this.passcode = pass;
      }
      return valid;
   }

   public CoreMessage createServerMessage() {
      return manager.createServerMessage();
   }

   public StompSession getSession() throws ActiveMQStompException {
      return getSession(null);
   }

   public StompSession getSession(String txID) throws ActiveMQStompException {
      StompSession session = null;
      try {
         if (txID == null) {
            session = manager.getSession(this);
         } else {
            session = manager.getTransactedSession(this, txID);
         }
      } catch (Exception e) {
         throw BUNDLE.errorGetSession(e).setHandler(frameHandler);
      }

      return session;
   }

   protected void validate() throws ActiveMQStompException {
      if (!this.valid) {
         throw BUNDLE.invalidConnection().setHandler(frameHandler);
      }
   }

   protected void sendServerMessage(ICoreMessage message, String txID) throws ActiveMQStompException {
      StompSession stompSession = getSession(txID);

      if (stompSession.isNoLocal()) {
         message.putStringProperty(CONNECTION_ID_PROP, getID().toString());
      }
      if (isEnableMessageID()) {
         message.putStringProperty("amqMessageId", "STOMP" + message.getMessageID());
      }
      try {
         if (minLargeMessageSize == -1 || (message.getBodyBuffer().writerIndex() < minLargeMessageSize)) {
            stompSession.sendInternal(message, false);
         } else {
            stompSession.sendInternalLarge((CoreMessage)message, false);
         }
      } catch (Exception e) {
         throw BUNDLE.errorSendMessage(message, e).setHandler(frameHandler);
      }
   }

   @Override
   public void disconnect(final boolean criticalError) {
      disconnect(null, criticalError);
   }

   @Override
   public void disconnect(String scaleDownNodeID, final boolean criticalError) {
      destroy();
   }

   protected void beginTransaction(String txID) throws ActiveMQStompException {
      try {
         manager.beginTransaction(this, txID);
      } catch (ActiveMQStompException e) {
         throw e;
      } catch (Exception e) {
         throw BUNDLE.errorBeginTx(txID, e).setHandler(frameHandler);
      }
   }

   public void commitTransaction(String txID) throws ActiveMQStompException {
      try {
         manager.commitTransaction(this, txID);
      } catch (Exception e) {
         throw BUNDLE.errorCommitTx(txID, e).setHandler(frameHandler);
      }
   }

   public void abortTransaction(String txID) throws ActiveMQStompException {
      try {
         manager.abortTransaction(this, txID);
      } catch (ActiveMQStompException e) {
         throw e;
      } catch (Exception e) {
         throw BUNDLE.errorAbortTx(txID, e).setHandler(frameHandler);
      }
   }

   StompPostReceiptFunction subscribe(String destination,
                  String selector,
                  String ack,
                  String id,
                  String durableSubscriptionName,
                  boolean noLocal,
                  RoutingType subscriptionType) throws ActiveMQStompException {
      autoCreateDestinationIfPossible(destination, subscriptionType);
      checkDestination(destination);
      checkRoutingSemantics(destination, subscriptionType);
      if (noLocal) {
         String noLocalFilter = CONNECTION_ID_PROP + " <> '" + getID().toString() + "'";
         if (selector == null) {
            selector = noLocalFilter;
         } else {
            selector += " AND " + noLocalFilter;
         }
      }

      if (ack == null) {
         ack = Stomp.Headers.Subscribe.AckModeValues.AUTO;
      }

      String subscriptionID = null;
      if (id != null) {
         subscriptionID = id;
      } else {
         if (destination == null) {
            throw BUNDLE.noDestination().setHandler(frameHandler);
         }
         subscriptionID = "subscription/" + destination;
      }

      try {
         return manager.subscribe(this, subscriptionID, durableSubscriptionName, destination, selector, ack, noLocal);
      } catch (ActiveMQStompException e) {
         throw e;
      } catch (Exception e) {
         throw BUNDLE.errorCreatingSubscription(subscriptionID, e).setHandler(frameHandler);
      }
   }

   public void unsubscribe(String subscriptionID, String durableSubscriptionName) throws ActiveMQStompException {
      try {
         manager.unsubscribe(this, subscriptionID, durableSubscriptionName);
      } catch (ActiveMQStompException e) {
         throw e;
      } catch (Exception e) {
         throw BUNDLE.errorUnsubscribing(subscriptionID, e).setHandler(frameHandler);
      }
   }

   public void acknowledge(String messageID, String subscriptionID) throws ActiveMQStompException {
      try {
         manager.acknowledge(this, messageID, subscriptionID);
      } catch (ActiveMQStompException e) {
         throw e;
      } catch (Exception e) {
         throw BUNDLE.errorAck(messageID, e).setHandler(frameHandler);
      }
   }

   public String getVersion() {
      return String.valueOf(version);
   }

   public String getActiveMQServerName() {
      return SERVER_NAME;
   }

   public StompFrame createStompMessage(ICoreMessage serverMessage,
                                        ActiveMQBuffer bodyBuffer,
                                        StompSubscription subscription,
                                        int deliveryCount) throws Exception {
      return frameHandler.createMessageFrame(serverMessage, bodyBuffer, subscription, deliveryCount);
   }

   public void addStompEventListener(FrameEventListener listener) {
      this.stompListener = listener;
   }

   //send a ping stomp frame
   public void ping(StompFrame pingFrame) {
      manager.sendReply(this, pingFrame, null);
   }

   public void physicalSend(StompFrame frame) throws Exception {
      ActiveMQBuffer buffer = frame.toActiveMQBuffer();
      synchronized (sendLock) {
         getTransportConnection().write(buffer, false, false);
      }

      if (stompListener != null) {
         stompListener.replySent(frame);
      }

   }

   public VersionedStompFrameHandler getFrameHandler() {
      return this.frameHandler;
   }

   public boolean isEnableMessageID() {
      return enableMessageID;
   }

   public int getMinLargeMessageSize() {
      return minLargeMessageSize;
   }

   public StompProtocolManager getManager() {
      return manager;
   }

   @Override
   public void killMessage(SimpleString nodeID) {
      //unsupported
   }

   @Override
   public boolean isSupportsFlowControl() {
      return false;
   }

   @Override
   public Subject getSubject() {
      return null;
   }

   /**
    * Returns the name of the protocol for this Remoting Connection
    *
    * @return
    */
   @Override
   public String getProtocolName() {
      return StompProtocolManagerFactory.STOMP_PROTOCOL_NAME;
   }

   @Override
   public String getTransportLocalAddress() {
      // TODO Auto-generated method stub
      return getTransportConnection().getLocalAddress();
   }

}
