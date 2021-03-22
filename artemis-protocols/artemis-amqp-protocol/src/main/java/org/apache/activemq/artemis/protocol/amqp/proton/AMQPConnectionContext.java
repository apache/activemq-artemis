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
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPConnectionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolLogger;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.EventHandler;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ExecutorNettyAdapter;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ExtCapability;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ProtonHandler;
import org.apache.activemq.artemis.protocol.amqp.sasl.AnonymousServerSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASLFactory;
import org.apache.activemq.artemis.protocol.amqp.sasl.PlainSASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.VersionLoader;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
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
   private static final FutureTask<Void> VOID_FUTURE = new FutureTask<>(() -> { }, null);

   protected final ProtonHandler handler;

   private AMQPConnectionCallback connectionCallback;
   private final String containerId;
   private final boolean isIncomingConnection;
   private final ClientSASLFactory saslClientFactory;
   private final Map<Symbol, Object> connectionProperties = new HashMap<>();
   private final ScheduledExecutorService scheduledPool;

   private final Map<Session, AMQPSessionContext> sessions = new ConcurrentHashMap<>();

   private final ProtonProtocolManager protocolManager;

   private final boolean useCoreSubscriptionNaming;

   /** Outgoing means created by the AMQP Bridge */
   private final boolean bridgeConnection;

   private final ScheduleOperator scheduleOp = new ScheduleOperator(new ScheduleRunnable());
   private final AtomicReference<Future<?>> scheduledFutureRef = new AtomicReference(VOID_FUTURE);

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
      this(protocolManager, connectionSP, containerId, idleTimeout, maxFrameSize, channelMax, useCoreSubscriptionNaming, scheduledPool, isIncomingConnection, saslClientFactory, connectionProperties, false);

   }
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
                                Map<Symbol, Object> connectionProperties,
                                boolean bridgeConnection) {

      this.protocolManager = protocolManager;
      this.bridgeConnection = bridgeConnection;
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
      EventLoop nettyExecutor;
      if (connectionCallback.getTransportConnection() instanceof NettyConnection) {
         nettyExecutor = ((NettyConnection) connectionCallback.getTransportConnection()).getNettyChannel().eventLoop();
      } else {
         nettyExecutor = new ExecutorNettyAdapter(protocolManager.getServer().getExecutorFactory().getExecutor());
      }
      this.handler = new ProtonHandler(nettyExecutor, protocolManager.getServer().getExecutorFactory().getExecutor(), isIncomingConnection && saslClientFactory == null);
      handler.addEventHandler(this);
      Transport transport = handler.getTransport();
      transport.setEmitFlowEventOnSend(false);
      if (idleTimeout > 0) {
         transport.setIdleTimeout(idleTimeout);
      }
      transport.setChannelMax(channelMax);
      transport.setInitialRemoteMaxFrameSize(protocolManager.getInitialRemoteMaxFrameSize());
      transport.setMaxFrameSize(maxFrameSize);
      transport.setOutboundFrameSizeLimit(maxFrameSize);
      if (saslClientFactory != null) {
         handler.createClientSASL();
      }
   }

   public boolean isBridgeConnection() {
      return bridgeConnection;
   }

   public void requireInHandler() {
      handler.requireHandler();
   }

   public boolean isHandler() {
      return handler.isHandler();
   }

   public void scheduledFlush() {
      handler.scheduledFlush();
   }

   public boolean isIncomingConnection() {
      return isIncomingConnection;
   }

   public ClientSASLFactory getSaslClientFactory() {
      return saslClientFactory;
   }

   protected AMQPSessionContext newSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionCallback sessionSPI = connectionCallback.createSessionCallback(this);
      AMQPSessionContext protonSession = new AMQPSessionContext(sessionSPI, this, realSession, protocolManager.getServer());

      return protonSession;
   }

   public SecurityAuth getSecurityAuth() {
      return new LocalSecurity();
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

   public ProtonHandler getHandler() {
      return handler;
   }

   public void destroy() {
      handler.runLater(() -> connectionCallback.close());
   }

   public boolean isSyncOnFlush() {
      return false;
   }

   public void instantFlush() {
      handler.instantFlush();
   }
   public void flush() {
      handler.flush();
   }

   public void afterFlush(Runnable runnable) {
      handler.afterFlush(runnable);
   }

   public void close(ErrorCondition errorCondition) {
      Future<?> scheduledFuture = scheduledFutureRef.getAndSet(null);

      if (scheduledPool instanceof ThreadPoolExecutor && scheduledFuture != null &&
         scheduledFuture != VOID_FUTURE && scheduledFuture instanceof Runnable) {
         if (!((ThreadPoolExecutor) scheduledPool).remove((Runnable) scheduledFuture) &&
            !scheduledFuture.isCancelled() && !scheduledFuture.isDone()) {
            ActiveMQAMQPProtocolLogger.LOGGER.cantRemovingScheduledTask();
         }
      }

      handler.close(errorCondition, this);
   }

   public AMQPSessionContext getSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionContext sessionExtension = sessions.get(realSession);
      if (sessionExtension == null) {
         // how this is possible? Log a warn here
         sessionExtension = newSessionExtension(realSession);
         realSession.setContext(sessionExtension);
         sessions.put(realSession, sessionExtension);
      }
      return sessionExtension;
   }

   public void runOnPool(Runnable run) {
      handler.runOnPool(run);
   }

   public void runNow(Runnable run) {
      handler.runNow(run);
   }

   public void runLater(Runnable run) {
      handler.runLater(run);
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

   public AMQPConnectionCallback getConnectionCallback() {
      return connectionCallback;
   }

   protected void remoteLinkOpened(Link link) throws Exception {

      AMQPSessionContext protonSession = getSessionExtension(link.getSession());

      if (link.getLocalState() ==  EndpointState.ACTIVE) { // if already active it's probably from the AMQP bridge and hence we just ignore it
         return;
      }

      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      if (link instanceof Receiver) {
         Receiver receiver = (Receiver) link;
         if (link.getRemoteTarget() instanceof Coordinator) {
            Coordinator coordinator = (Coordinator) link.getRemoteTarget();
            protonSession.addTransactionHandler(coordinator, receiver);
         } else {
            if (isReplicaTarget(receiver)) {
               try {
                  protonSession.getSessionSPI().check(SimpleString.toSimpleString(ProtonProtocolManager.MIRROR_ADDRESS), CheckType.SEND, getSecurityAuth());
               } catch (ActiveMQSecurityException e) {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingProducer(e.getMessage());
               }
               protonSession.addReplicaTarget(receiver);
            } else {
               protonSession.addReceiver(receiver);
            }
         }
      } else {
         Sender sender = (Sender) link;
         protonSession.addSender(sender);
      }
   }

   private boolean isReplicaTarget(Link link) {
      return link != null && link.getTarget() != null && link.getTarget().getAddress() != null && link.getTarget().getAddress().equals(ProtonProtocolManager.MIRROR_ADDRESS);
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
         return AmqpSupport.AMQP_LOW_CREDITS_DEFAULT;
      }
   }

   public int getAmqpCredits() {
      if (protocolManager != null) {
         return protocolManager.getAmqpCredits();
      } else {
         // this is for tests only...
         return AmqpSupport.AMQP_CREDITS_DEFAULT;
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
            handler.close(null, this);
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
      handler.close(null, this);
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
   public String getRemoteAddress() {
      return connectionCallback.getTransportConnection().getRemoteAddress();
   }

   @Override
   public void onRemoteOpen(Connection connection) throws Exception {
      handler.requireHandler();
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
      initialize();

      /*
       * This can be null which is in effect an empty map, also we really don't need to check this for in bound connections
       * but its here in case we add support for outbound connections.
       * */
      if (connection.getRemoteProperties() == null || !connection.getRemoteProperties().containsKey(CONNECTION_OPEN_FAILED)) {
         long nextKeepAliveTime = handler.tick(true);

         if (nextKeepAliveTime != 0 && scheduledPool != null) {
            scheduleOp.setDelay(nextKeepAliveTime - TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));

            scheduledFutureRef.getAndUpdate(scheduleOp);
         }
      }
   }

   class ScheduleOperator implements UnaryOperator<Future<?>> {

      private long delay;
      final ScheduleRunnable scheduleRunnable;

      ScheduleOperator(ScheduleRunnable scheduleRunnable) {
         this.scheduleRunnable = scheduleRunnable;
      }

      @Override
      public Future<?> apply(Future<?> future) {
         return (future != null) ? scheduledPool.schedule(scheduleRunnable, delay, TimeUnit.MILLISECONDS) : null;
      }

      public void setDelay(long delay) {
         this.delay = delay;
      }
   }


   class TickerRunnable implements Runnable {

      @Override
      public void run() {
         Long rescheduleAt = handler.tick(false);

         if (rescheduleAt == null) {
            // this mean tick could not acquire a lock, we will just retry in 10 milliseconds.
            scheduleOp.setDelay(10);

            scheduledFutureRef.getAndUpdate(scheduleOp);
         } else if (rescheduleAt != 0) {
            scheduleOp.setDelay(rescheduleAt - TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));

            scheduledFutureRef.getAndUpdate(scheduleOp);
         }
      }
   }

   class ScheduleRunnable implements Runnable {

      final TickerRunnable tickerRunnable = new TickerRunnable();

      @Override
      public void run() {

         // The actual tick has to happen within a Netty Worker, to avoid requiring a lock
         // this will also be used to flush the data directly into netty connection's executor
         handler.runLater(tickerRunnable);
      }
   }

   @Override
   public void onRemoteClose(Connection connection) {
      handler.requireHandler();
      connection.close();
      connection.free();

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
      AMQPSessionContext sessionContext = getSessionExtension(session);

      if (bridgeConnection) {
         sessionContext.initialize();
      }
   }

   @Override
   public void onRemoteOpen(Session session) throws Exception {
      handler.requireHandler();
      getSessionExtension(session).initialize();
      session.open();
   }

   @Override
   public void onRemoteClose(Session session) throws Exception {
      handler.runLater(() -> {
         session.close();
         session.free();

         AMQPSessionContext sessionContext = (AMQPSessionContext) session.getContext();
         if (sessionContext != null) {
            sessionContext.close();
            sessions.remove(session);
            session.setContext(null);
         }
      });
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
      handler.requireHandler();

      // We scheduled it for later, as that will work through anything that's pending on the current deliveries.
      runNow(() -> {

         ProtonDeliveryHandler linkContext = (ProtonDeliveryHandler) link.getContext();
         if (linkContext != null) {
            try {
               linkContext.close(true);
            } catch (Exception e) {
               log.error(e.getMessage(), e);
            }
         }

         /// we have to perform the link.close after the linkContext.close is finished.
         // linkeContext.close will perform a few executions on the netty loop,
         // this has to come next
         runLater(() -> {
            link.close();
            link.free();
            flush();
         });

      });
   }

   @Override
   public void onRemoteDetach(Link link) throws Exception {
      handler.requireHandler();
      boolean handleAsClose = link.getSource() != null && ((Source) link.getSource()).getExpiryPolicy() == TerminusExpiryPolicy.LINK_DETACH;

      if (handleAsClose) {
         onRemoteClose(link);
      } else {
         link.detach();
         link.free();
      }
   }

   @Override
   public void onLocalDetach(Link link) throws Exception {
      handler.requireHandler();
      Object context = link.getContext();
      if (context instanceof ProtonServerSenderContext) {
         ProtonServerSenderContext senderContext = (ProtonServerSenderContext) context;
         senderContext.close(false);
      }
   }

   @Override
   public void onDelivery(Delivery delivery) throws Exception {
      handler.requireHandler();
      ProtonDeliveryHandler handler = (ProtonDeliveryHandler) delivery.getLink().getContext();
      if (handler != null) {
         handler.onMessage(delivery);
      } else {
         log.warn("Handler is null, can't delivery " + delivery, new Exception("tracing location"));
      }
   }


   private class LocalSecurity implements SecurityAuth {
      @Override
      public String getUsername() {
         String username = null;
         SASLResult saslResult = getSASLResult();
         if (saslResult != null) {
            username = saslResult.getUser();
         }

         return username;
      }

      @Override
      public String getPassword() {
         String password = null;
         SASLResult saslResult = getSASLResult();
         if (saslResult != null) {
            if (saslResult instanceof PlainSASLResult) {
               password = ((PlainSASLResult) saslResult).getPassword();
            }
         }

         return password;
      }

      @Override
      public RemotingConnection getRemotingConnection() {
         return connectionCallback.getProtonConnectionDelegate();
      }

      @Override
      public String getSecurityDomain() {
         return getProtocolManager().getSecurityDomain();
      }
   }

}
