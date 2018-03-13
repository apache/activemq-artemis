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
package org.apache.activemq.artemis.protocol.amqp.proton;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPConnectionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.EventHandler;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ExtCapability;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ProtonHandler;
import org.apache.activemq.artemis.protocol.amqp.sasl.AnonymousServerSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASLFactory;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.VersionLoader;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.FAILOVER_SERVER_LIST;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.HOSTNAME;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.NETWORK_HOST;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.PORT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport.SCHEME;

public class AMQPConnectionContext extends ProtonInitializable implements EventHandler {

   private static final Logger log = Logger.getLogger(AMQPConnectionContext.class);

   public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
   public static final String AMQP_CONTAINER_ID = "amqp-container-id";

   protected final ProtonHandler handler;

   protected AMQPConnectionCallback connectionCallback;
   private final String containerId;
   private final boolean isIncomingConnection;
   private final ClientSASLFactory saslClientFactory;
   private final Map<Symbol, Object> connectionProperties = new HashMap<>();
   private final ScheduledExecutorService scheduledPool;

   private final Map<Session, AMQPSessionContext> sessions = new ConcurrentHashMap<>();

   private final ProtonProtocolManager protocolManager;

   private final boolean useCoreSubscriptionNaming;

   public AMQPConnectionContext(ProtonProtocolManager protocolManager,
                                AMQPConnectionCallback connectionSP,
                                String containerId,
                                int idleTimeout,
                                int maxFrameSize,
                                int channelMax,
                                boolean useCoreSubscriptionNaming,
                                ScheduledExecutorService scheduledPool,
                                boolean isIncomingConnection,
                                ClientSASLFactory saslClientFactory,
                                Map<Symbol, Object> connectionProperties) {

      this.protocolManager = protocolManager;
      this.connectionCallback = connectionSP;
      this.useCoreSubscriptionNaming = useCoreSubscriptionNaming;
      this.containerId = (containerId != null) ? containerId : UUID.randomUUID().toString();
      this.isIncomingConnection = isIncomingConnection;
      this.saslClientFactory = saslClientFactory;

      this.connectionProperties.put(AmqpSupport.PRODUCT, "apache-activemq-artemis");
      this.connectionProperties.put(AmqpSupport.VERSION, VersionLoader.getVersion().getFullVersion());

      if (connectionProperties != null) {
         this.connectionProperties.putAll(connectionProperties);
      }

      this.scheduledPool = scheduledPool;
      connectionCallback.setConnection(this);
      this.handler = new ProtonHandler(protocolManager.getServer().getExecutorFactory().getExecutor(), isIncomingConnection);
      handler.addEventHandler(this);
      Transport transport = handler.getTransport();
      transport.setEmitFlowEventOnSend(false);
      if (idleTimeout > 0) {
         transport.setIdleTimeout(idleTimeout);
      }
      transport.setChannelMax(channelMax);
      transport.setInitialRemoteMaxFrameSize(protocolManager.getInitialRemoteMaxFrameSize());
      transport.setMaxFrameSize(maxFrameSize);
      if (!isIncomingConnection && saslClientFactory != null) {
         handler.createClientSASL();
      }
   }

   public boolean isIncomingConnection() {
      return isIncomingConnection;
   }

   public ClientSASLFactory getSaslClientFactory() {
      return saslClientFactory;
   }

   protected AMQPSessionContext newSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionCallback sessionSPI = connectionCallback.createSessionCallback(this);
      AMQPSessionContext protonSession = new AMQPSessionContext(sessionSPI, this, realSession);

      return protonSession;
   }

   public SASLResult getSASLResult() {
      return handler.getSASLResult();
   }

   public void inputBuffer(ByteBuf buffer) {
      if (log.isTraceEnabled()) {
         ByteUtil.debugFrame(log, "Buffer Received ", buffer);
      }

      handler.inputBuffer(buffer);
   }

   public void destroy() {
      connectionCallback.close();
   }

   public boolean isSyncOnFlush() {
      return false;
   }

   public boolean tryLock(long time, TimeUnit timeUnit) {
      return handler.tryLock(time, timeUnit);
   }

   public void lock() {
      handler.lock();
   }

   public void unlock() {
      handler.unlock();
   }

   public int capacity() {
      return handler.capacity();
   }

   public void flush() {
      handler.flush();
   }

   public void close(ErrorCondition errorCondition) {
      handler.close(errorCondition);
   }

   protected AMQPSessionContext getSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionContext sessionExtension = sessions.get(realSession);
      if (sessionExtension == null) {
         // how this is possible? Log a warn here
         sessionExtension = newSessionExtension(realSession);
         realSession.setContext(sessionExtension);
         sessions.put(realSession, sessionExtension);
      }
      return sessionExtension;
   }

   protected boolean validateConnection(Connection connection) {
      return connectionCallback.validateConnection(connection, handler.getSASLResult());
   }

   public boolean checkDataReceived() {
      return handler.checkDataReceived();
   }

   public long getCreationTime() {
      return handler.getCreationTime();
   }

   public String getRemoteContainer() {
      return handler.getConnection().getRemoteContainer();
   }

   public String getPubSubPrefix() {
      return null;
   }

   protected void initInternal() throws Exception {
   }

   protected void remoteLinkOpened(Link link) throws Exception {

      AMQPSessionContext protonSession = getSessionExtension(link.getSession());

      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      if (link instanceof Receiver) {
         Receiver receiver = (Receiver) link;
         if (link.getRemoteTarget() instanceof Coordinator) {
            Coordinator coordinator = (Coordinator) link.getRemoteTarget();
            protonSession.addTransactionHandler(coordinator, receiver);
         } else {
            protonSession.addReceiver(receiver);
         }
      } else {
         Sender sender = (Sender) link;
         protonSession.addSender(sender);
      }
   }

   public Symbol[] getConnectionCapabilitiesOffered() {
      URI tc = connectionCallback.getFailoverList();
      if (tc != null) {
         Map<Symbol, Object> hostDetails = new HashMap<>();
         hostDetails.put(NETWORK_HOST, tc.getHost());
         boolean isSSL = tc.getQuery().contains(TransportConstants.SSL_ENABLED_PROP_NAME + "=true");
         if (isSSL) {
            hostDetails.put(SCHEME, "amqps");
         } else {
            hostDetails.put(SCHEME, "amqp");
         }
         hostDetails.put(HOSTNAME, tc.getHost());
         hostDetails.put(PORT, tc.getPort());

         connectionProperties.put(FAILOVER_SERVER_LIST, Arrays.asList(hostDetails));
      }
      return ExtCapability.getCapabilities();
   }

   public void open() {
      handler.open(containerId, connectionProperties);
   }

   public String getContainer() {
      return containerId;
   }

   public void addEventHandler(EventHandler eventHandler) {
      handler.addEventHandler(eventHandler);
   }

   public ProtonProtocolManager getProtocolManager() {
      return protocolManager;
   }

   public int getAmqpLowCredits() {
      if (protocolManager != null) {
         return protocolManager.getAmqpLowCredits();
      } else {
         // this is for tests only...
         return 30;
      }
   }

   public int getAmqpCredits() {
      if (protocolManager != null) {
         return protocolManager.getAmqpCredits();
      } else {
         // this is for tests only...
         return 100;
      }
   }

   public boolean isUseCoreSubscriptionNaming() {
      return useCoreSubscriptionNaming;
   }

   @Override
   public void onAuthInit(ProtonHandler handler, Connection connection, boolean sasl) {
      if (sasl) {
         // configured mech in decreasing order of preference
         String[] mechanisms = connectionCallback.getSaslMechanisms();
         if (mechanisms == null || mechanisms.length == 0) {
            mechanisms = AnonymousServerSASL.ANONYMOUS_MECH;
         }
         handler.createServerSASL(mechanisms);
      } else {
         if (!connectionCallback.isSupportsAnonymous()) {
            connectionCallback.sendSASLSupported();
            connectionCallback.close();
            handler.close(null);
         }
      }
   }

   @Override
   public void onSaslRemoteMechanismChosen(ProtonHandler handler, String mech) {
      handler.setChosenMechanism(connectionCallback.getServerSASL(mech));
   }

   @Override
   public void onSaslMechanismsOffered(final ProtonHandler handler, final String[] mechanisms) {
      if (saslClientFactory != null) {
         handler.setClientMechanism(saslClientFactory.chooseMechanism(mechanisms));
      }
   }

   @Override
   public void onAuthFailed(final ProtonHandler protonHandler, final Connection connection) {
      connectionCallback.close();
      handler.close(null);
   }

   @Override
   public void onAuthSuccess(final ProtonHandler protonHandler, final Connection connection) {
      connection.open();
   }

   @Override
   public void onTransport(Transport transport) {
      handler.flushBytes();
   }

   @Override
   public void pushBytes(ByteBuf bytes) {
      connectionCallback.onTransport(bytes, this);
   }

   @Override
   public boolean flowControl(ReadyListener readyListener) {
      return connectionCallback.isWritable(readyListener);
   }

   @Override
   public void onRemoteOpen(Connection connection) throws Exception {
      lock();
      try {
         try {
            initInternal();
         } catch (Exception e) {
            log.error("Error init connection", e);
         }
         if (!validateConnection(connection)) {
            connection.close();
         } else {
            connection.setContext(AMQPConnectionContext.this);
            connection.setContainer(containerId);
            connection.setProperties(connectionProperties);
            connection.setOfferedCapabilities(getConnectionCapabilitiesOffered());
            connection.open();
         }
      } finally {
         unlock();
      }
      initialise();

         /*
         * This can be null which is in effect an empty map, also we really don't need to check this for in bound connections
         * but its here in case we add support for outbound connections.
         * */
      if (connection.getRemoteProperties() == null || !connection.getRemoteProperties().containsKey(CONNECTION_OPEN_FAILED)) {
         long nextKeepAliveTime = handler.tick(true);
         if (nextKeepAliveTime != 0 && scheduledPool != null) {
            scheduledPool.schedule(new Runnable() {
               @Override
               public void run() {
                  long rescheduleAt = handler.tick(false);
                  if (rescheduleAt != 0) {
                     scheduledPool.schedule(this, rescheduleAt - TimeUnit.NANOSECONDS.toMillis(System.nanoTime()), TimeUnit.MILLISECONDS);
                  }
               }
            }, (nextKeepAliveTime - TimeUnit.NANOSECONDS.toMillis(System.nanoTime())), TimeUnit.MILLISECONDS);
         }
      }
   }

   @Override
   public void onRemoteClose(Connection connection) {
      lock();
      try {
         connection.close();
         connection.free();
      } finally {
         unlock();
      }

      for (AMQPSessionContext protonSession : sessions.values()) {
         protonSession.close();
      }
      sessions.clear();

      // We must force write the channel before we actually destroy the connection
      handler.flushBytes();
      destroy();
   }

   @Override
   public void onLocalOpen(Session session) throws Exception {
      getSessionExtension(session);
   }

   @Override
   public void onRemoteOpen(Session session) throws Exception {
      getSessionExtension(session).initialise();
      lock();
      try {
         session.open();
      } finally {
         unlock();
      }
   }

   @Override
   public void onRemoteClose(Session session) throws Exception {
      lock();
      try {
         session.close();
         session.free();
      } finally {
         unlock();
      }

      AMQPSessionContext sessionContext = (AMQPSessionContext) session.getContext();
      if (sessionContext != null) {
         sessionContext.close();
         sessions.remove(session);
         session.setContext(null);
      }
   }

   @Override
   public void onRemoteOpen(Link link) throws Exception {
      remoteLinkOpened(link);
   }

   @Override
   public void onFlow(Link link) throws Exception {
      if (link.getContext() != null) {
         ((ProtonDeliveryHandler) link.getContext()).onFlow(link.getCredit(), link.getDrain());
      }
   }

   @Override
   public void onRemoteClose(Link link) throws Exception {
      lock();
      try {
         link.close();
         link.free();
      } finally {
         unlock();
      }

      ProtonDeliveryHandler linkContext = (ProtonDeliveryHandler) link.getContext();
      if (linkContext != null) {
         linkContext.close(true);
      }
   }

   @Override
   public void onRemoteDetach(Link link) throws Exception {
      lock();
      try {
         link.detach();
         link.free();
      } finally {
         unlock();
      }
   }

   @Override
   public void onLocalDetach(Link link) throws Exception {
      Object context = link.getContext();
      if (context instanceof ProtonServerSenderContext) {
         ProtonServerSenderContext senderContext = (ProtonServerSenderContext) context;
         senderContext.close(false);
      }
   }

   @Override
   public void onDelivery(Delivery delivery) throws Exception {
      ProtonDeliveryHandler handler = (ProtonDeliveryHandler) delivery.getLink().getContext();
      if (handler != null) {
         handler.onMessage(delivery);
      } else {
         log.warn("Handler is null, can't delivery " + delivery, new Exception("tracing location"));
      }
   }
}
