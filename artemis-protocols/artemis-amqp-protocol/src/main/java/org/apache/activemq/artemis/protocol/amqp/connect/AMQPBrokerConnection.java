/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.connect;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.remoting.CertificateUtil;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.BrokerConnection;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.mirror.MirrorController;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerQueuePlugin;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerAggregation;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolLogger;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.activemq.artemis.protocol.amqp.proton.SenderController;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASLFactory;
import org.apache.activemq.artemis.protocol.amqp.sasl.scram.SCRAMClientSASL;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.remoting.ClientConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.security.scram.SCRAM;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.jboss.logging.Logger;

public class AMQPBrokerConnection implements ClientConnectionLifeCycleListener, ActiveMQServerQueuePlugin, BrokerConnection {

   private static final Logger logger = Logger.getLogger(AMQPBrokerConnection.class);

   private final AMQPBrokerConnectConfiguration brokerConnectConfiguration;
   private final ProtonProtocolManager protonProtocolManager;
   private final ActiveMQServer server;
   private final NettyConnector bridgesConnector;
   private NettyConnection connection;
   private Session session;
   private AMQPSessionContext sessionContext;
   private ActiveMQProtonRemotingConnection protonRemotingConnection;
   private volatile boolean started = false;
   private final AMQPBrokerConnectionManager bridgeManager;
   private int retryCounter = 0;
   private boolean connecting = false;
   private volatile ScheduledFuture reconnectFuture;
   private final Set<Queue> senders = new HashSet<>();
   private final Set<Queue> receivers = new HashSet<>();

   final Executor connectExecutor;
   final ScheduledExecutorService scheduledExecutorService;

   /** This is just for logging.
    *  the actual connection will come from the amqpConnection configuration*/
   String host;

   /** This is just for logging.
    *  the actual connection will come from the amqpConnection configuration*/
   int port;

   public AMQPBrokerConnection(AMQPBrokerConnectionManager bridgeManager, AMQPBrokerConnectConfiguration brokerConnectConfiguration,
                               ProtonProtocolManager protonProtocolManager,
                               ActiveMQServer server,
                               NettyConnector bridgesConnector) {
      this.bridgeManager = bridgeManager;
      this.brokerConnectConfiguration = brokerConnectConfiguration;
      this.protonProtocolManager = protonProtocolManager;
      this.server = server;
      this.bridgesConnector = bridgesConnector;
      connectExecutor = server.getExecutorFactory().getExecutor();
      scheduledExecutorService = server.getScheduledPool();
   }

   @Override
   public String getName() {
      return brokerConnectConfiguration.getName();
   }

   @Override
   public String getProtocol() {
      return "AMQP";
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public void stop() {
      if (!started) return;
      started = false;
      if (protonRemotingConnection != null) {
         protonRemotingConnection.fail(new ActiveMQException("Stopping Broker Connection"));
         protonRemotingConnection = null;
         connection = null;
      }
      ScheduledFuture scheduledFuture = reconnectFuture;
      reconnectFuture = null;
      if (scheduledFuture != null) {
         scheduledFuture.cancel(true);
      }
   }

   @Override
   public void start() throws Exception {
      if (started) return;
      started = true;
      server.getConfiguration().registerBrokerPlugin(this);
      try {

         if (brokerConnectConfiguration != null && brokerConnectConfiguration.getConnectionElements() != null) {
            for (AMQPBrokerConnectionElement connectionElement : brokerConnectConfiguration.getConnectionElements()) {
               if (connectionElement.getType() == AMQPBrokerConnectionAddressType.MIRROR) {
                  installMirrorController(this, (AMQPMirrorBrokerConnectionElement) connectionElement, server);
               }
            }
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         return;
      }
      connectExecutor.execute(() -> doConnect());
   }

   public NettyConnection getConnection() {
      return connection;
   }

   @Override
   public void afterCreateQueue(Queue queue) {
      connectExecutor.execute(() -> {
         for (AMQPBrokerConnectionElement connectionElement : brokerConnectConfiguration.getConnectionElements()) {
            validateMatching(queue, connectionElement);
         }
      });
   }

   public void validateMatching(Queue queue, AMQPBrokerConnectionElement connectionElement) {
      if (connectionElement.getType() != AMQPBrokerConnectionAddressType.MIRROR) {
         if (connectionElement.getQueueName() != null) {
            if (queue.getName().equals(connectionElement.getQueueName())) {
               createLink(queue, connectionElement);
            }
         } else if (connectionElement.match(queue.getAddress(), server.getConfiguration().getWildcardConfiguration())) {
            createLink(queue, connectionElement);
         }
      }
   }

   public void createLink(Queue queue, AMQPBrokerConnectionElement connectionElement) {
      if (connectionElement.getType() == AMQPBrokerConnectionAddressType.PEER) {
         connectSender(queue, queue.getAddress().toString(), null, Symbol.valueOf("qd.waypoint"));
         connectReceiver(protonRemotingConnection, session, sessionContext, queue, Symbol.valueOf("qd.waypoint"));
      } else {
         if (connectionElement.getType() == AMQPBrokerConnectionAddressType.SENDER) {
            connectSender(queue, queue.getAddress().toString(), null);
         }
         if (connectionElement.getType() == AMQPBrokerConnectionAddressType.RECEIVER) {
            connectReceiver(protonRemotingConnection, session, sessionContext, queue);
         }
      }
   }

   private void doConnect() {
      try {
         connecting = true;

         List<TransportConfiguration> configurationList = brokerConnectConfiguration.getTransportConfigurations();

         TransportConfiguration tpConfig = configurationList.get(0);

         String hostOnParameter = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, tpConfig.getParams());
         int portOnParameter = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, tpConfig.getParams());
         this.host = hostOnParameter;
         this.port = portOnParameter;
         connection = bridgesConnector.createConnection(null, hostOnParameter, portOnParameter);

         if (connection == null) {
            retryConnection();
            return;
         }

         int currentRetryCounter = retryCounter;
         reconnectFuture = null;
         retryCounter = 0;

         // before we retry the connection we need to remove any previous links
         // as they will need to be recreated
         senders.clear();
         receivers.clear();

         ClientSASLFactory saslFactory = new SaslFactory(connection, brokerConnectConfiguration);

         ConnectionEntry entry = protonProtocolManager.createOutgoingConnectionEntry(connection, saslFactory);
         server.getRemotingService().addConnectionEntry(connection, entry);
         protonRemotingConnection = (ActiveMQProtonRemotingConnection) entry.connection;

         connection.getChannel().pipeline().addLast(new AMQPBrokerConnectionChannelHandler(bridgesConnector.getChannelGroup(), protonRemotingConnection.getAmqpConnection().getHandler(), this, server.getExecutorFactory().getExecutor()));

         session = protonRemotingConnection.getAmqpConnection().getHandler().getConnection().session();
         sessionContext = protonRemotingConnection.getAmqpConnection().getSessionExtension(session);

         protonRemotingConnection.getAmqpConnection().runLater(() -> {
            protonRemotingConnection.getAmqpConnection().open();
            session.open();
            protonRemotingConnection.getAmqpConnection().flush();
         });

         if (brokerConnectConfiguration.getConnectionElements() != null) {
            Stream<Binding> bindingStream = server.getPostOffice().getAllBindings();

            bindingStream.forEach(binding -> {
               if (binding instanceof QueueBinding) {
                  Queue queue = ((QueueBinding) binding).getQueue();
                  for (AMQPBrokerConnectionElement connectionElement : brokerConnectConfiguration.getConnectionElements()) {
                     validateMatching(queue, connectionElement);
                  }
               }
            });

            for (AMQPBrokerConnectionElement connectionElement : brokerConnectConfiguration.getConnectionElements()) {
               if (connectionElement.getType() == AMQPBrokerConnectionAddressType.MIRROR) {
                  AMQPMirrorBrokerConnectionElement replica = (AMQPMirrorBrokerConnectionElement)connectionElement;
                  Queue queue = server.locateQueue(replica.getSourceMirrorAddress());

                  connectSender(queue, ProtonProtocolManager.MIRROR_ADDRESS, (r) -> AMQPMirrorControllerSource.validateProtocolData(r, replica.getSourceMirrorAddress()));
               }
            }
         }

         protonRemotingConnection.getAmqpConnection().flush();

         bridgeManager.connected(connection, this);

         ActiveMQAMQPProtocolLogger.LOGGER.successReconnect(brokerConnectConfiguration.getName(), host + ":" + port, currentRetryCounter);

         connecting = false;
      } catch (Throwable e) {
         error(e);
      }
   }

   public void retryConnection() {
      if (bridgeManager.isStarted() && started) {
         if (brokerConnectConfiguration.getReconnectAttempts() < 0 || retryCounter < brokerConnectConfiguration.getReconnectAttempts()) {
            retryCounter++;
            ActiveMQAMQPProtocolLogger.LOGGER.retryConnection(brokerConnectConfiguration.getName(), host + ":" + port, retryCounter, brokerConnectConfiguration.getReconnectAttempts());
            if (logger.isDebugEnabled()) {
               logger.debug("Reconnecting in " + brokerConnectConfiguration.getRetryInterval() + ", this is the " + retryCounter + " of " + brokerConnectConfiguration.getReconnectAttempts());
            }
            reconnectFuture = scheduledExecutorService.schedule(() -> connectExecutor.execute(() -> doConnect()), brokerConnectConfiguration.getRetryInterval(), TimeUnit.MILLISECONDS);
         } else {
            connecting = false;
            ActiveMQAMQPProtocolLogger.LOGGER.retryConnectionFailed(brokerConnectConfiguration.getName(), host + ":" +  port, retryCounter);
            if (logger.isDebugEnabled()) {
               logger.debug("no more reconnections as the retry counter reached " + retryCounter + " out of " + brokerConnectConfiguration.getReconnectAttempts());
            }
         }
      }
   }

   private static void uninstallMirrorController(AMQPMirrorBrokerConnectionElement replicaConfig, ActiveMQServer server) {
      // TODO implement this as part of https://issues.apache.org/jira/browse/ARTEMIS-2965
   }

   /** The reason this method is static is the following:
    *
    *  It is returning the snfQueue to the replica, and I needed isolation from the actual instance.
    *  During development I had a mistake where I used a property from the Object,
    *  so, I needed this isolation for my organization and making sure nothing would be shared. */
   private static Queue installMirrorController(AMQPBrokerConnection brokerConnection, AMQPMirrorBrokerConnectionElement replicaConfig, ActiveMQServer server) throws Exception {

      MirrorController currentMirrorController = server.getMirrorController();

      // This following block is to avoid a duplicate on mirror controller
      if (currentMirrorController != null && currentMirrorController instanceof AMQPMirrorControllerSource) {
         Queue queue = checkCurrentMirror(brokerConnection, (AMQPMirrorControllerSource)currentMirrorController);
         // on this case we already had a mirror installed before, we won't duplicate it
         if (queue != null) {
            return queue;
         }
      } else if (currentMirrorController != null && currentMirrorController instanceof AMQPMirrorControllerAggregation) {
         AMQPMirrorControllerAggregation aggregation = (AMQPMirrorControllerAggregation) currentMirrorController;

         for (AMQPMirrorControllerSource source : aggregation.getPartitions()) {
            Queue queue = checkCurrentMirror(brokerConnection, source);
            // on this case we already had a mirror installed before, we won't duplicate it
            if (queue != null) {
               return queue;
            }
         }
      }

      AddressInfo addressInfo = server.getAddressInfo(replicaConfig.getSourceMirrorAddress());
      if (addressInfo == null) {
         addressInfo = new AddressInfo(replicaConfig.getSourceMirrorAddress()).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false).setTemporary(!replicaConfig.isDurable());
         server.addAddressInfo(addressInfo);
      }

      if (addressInfo.getRoutingType() != RoutingType.ANYCAST) {
         throw new IllegalArgumentException("sourceMirrorAddress is not ANYCAST");
      }

      Queue mirrorControlQueue = server.locateQueue(replicaConfig.getSourceMirrorAddress());

      if (mirrorControlQueue == null) {
         mirrorControlQueue = server.createQueue(new QueueConfiguration(replicaConfig.getSourceMirrorAddress()).setAddress(replicaConfig.getSourceMirrorAddress()).setRoutingType(RoutingType.ANYCAST).setDurable(replicaConfig.isDurable()), true);
      }

      mirrorControlQueue.setMirrorController(true);

      QueueBinding snfReplicaQueueBinding = (QueueBinding)server.getPostOffice().getBinding(replicaConfig.getSourceMirrorAddress());
      if (snfReplicaQueueBinding == null) {
         logger.warn("Queue does not exist even after creation! " + replicaConfig);
         throw new IllegalAccessException("Cannot start replica");
      }

      Queue snfQueue = snfReplicaQueueBinding.getQueue();

      if (!snfQueue.getAddress().equals(replicaConfig.getSourceMirrorAddress())) {
         logger.warn("Queue " + snfQueue + " belong to a different address (" + snfQueue.getAddress() + "), while we expected it to be " + addressInfo.getName());
         throw new IllegalAccessException("Cannot start replica");
      }

      AMQPMirrorControllerSource newPartition = new AMQPMirrorControllerSource(snfQueue, server, replicaConfig.isMessageAcknowledgements(), replicaConfig.isQueueCreation(), replicaConfig.isQueueRemoval(), brokerConnection);

      server.scanAddresses(newPartition);


      if (currentMirrorController == null) {
         server.installMirrorController(newPartition);
      } else {
         // Replace a standard implementation by an aggregated supporting multiple targets
         if (currentMirrorController instanceof AMQPMirrorControllerSource) {
            // replacing the simple mirror control for an aggregator
            AMQPMirrorControllerAggregation remoteAggregation = new AMQPMirrorControllerAggregation();
            remoteAggregation.addPartition((AMQPMirrorControllerSource) currentMirrorController);
            currentMirrorController = remoteAggregation;
            server.installMirrorController(remoteAggregation);
         }
         ((AMQPMirrorControllerAggregation) currentMirrorController).addPartition(newPartition);
      }

      return snfQueue;
   }

   private static Queue checkCurrentMirror(AMQPBrokerConnection brokerConnection,
                                             AMQPMirrorControllerSource currentMirrorController) {
      AMQPMirrorControllerSource source = currentMirrorController;
      if (source.getBrokerConnection() == brokerConnection) {
         return source.getSnfQueue();
      }

      return null;
   }

   private void connectReceiver(ActiveMQProtonRemotingConnection protonRemotingConnection,
                                Session session,
                                AMQPSessionContext sessionContext,
                                Queue queue,
                                Symbol... capabilities) {
      if (logger.isDebugEnabled()) {
         logger.debug("Connecting inbound for " + queue);
      }

      if (session == null) {
         logger.debug("session is null");
         return;
      }

      protonRemotingConnection.getAmqpConnection().runLater(() -> {

         if (receivers.contains(queue)) {
            logger.debug("Receiver for queue " + queue + " already exists, just giving up");
            return;
         }
         receivers.add(queue);
         Receiver receiver = session.receiver(queue.getAddress().toString() + ":" + UUIDGenerator.getInstance().generateStringUUID());
         receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
         receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
         Target target = new Target();
         target.setAddress(queue.getAddress().toString());
         receiver.setTarget(target);

         Source source = new Source();
         source.setAddress(queue.getAddress().toString());
         receiver.setSource(source);

         if (capabilities != null) {
            source.setCapabilities(capabilities);
         }

         receiver.open();
         protonRemotingConnection.getAmqpConnection().flush();
         try {
            sessionContext.addReceiver(receiver);
         } catch (Exception e) {
            error(e);
         }
      });
   }

   private void connectSender(Queue queue,
                              String targetName,
                              java.util.function.Consumer<? super MessageReference> beforeDeliver,
                              Symbol... capabilities) {
      if (logger.isDebugEnabled()) {
         logger.debug("Connecting outbound for " + queue);
      }


      if (session == null) {
         logger.debug("Session is null");
         return;
      }

      protonRemotingConnection.getAmqpConnection().runLater(() -> {
         try {
            if (senders.contains(queue)) {
               logger.debug("Sender for queue " + queue + " already exists, just giving up");
               return;
            }
            senders.add(queue);
            Sender sender = session.sender(targetName + ":" + UUIDGenerator.getInstance().generateStringUUID());
            sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
            Target target = new Target();
            target.setAddress(targetName);
            if (capabilities != null) {
               target.setCapabilities(capabilities);
            }
            sender.setTarget(target);

            Source source = new Source();
            source.setAddress(queue.getAddress().toString());
            sender.setSource(source);

            AMQPOutgoingController outgoingInitializer = new AMQPOutgoingController(queue, sender, sessionContext.getSessionSPI());

            ProtonServerSenderContext senderContext = new ProtonServerSenderContext(protonRemotingConnection.getAmqpConnection(), sender, sessionContext, sessionContext.getSessionSPI(), outgoingInitializer).setBeforeDelivery(beforeDeliver);

            sessionContext.addSender(sender, senderContext);
         } catch (Exception e) {
            error(e);
         }
         protonRemotingConnection.getAmqpConnection().flush();
      });
   }

   protected void error(Throwable e) {
      connecting = false;
      logger.warn(e.getMessage(), e);
      redoConnection();
   }

   private class AMQPOutgoingController implements SenderController {

      final Queue queue;
      final Sender sender;
      final AMQPSessionCallback sessionSPI;

      AMQPOutgoingController(Queue queue, Sender sender, AMQPSessionCallback sessionSPI) {
         this.queue = queue;
         this.sessionSPI = sessionSPI;
         this.sender = sender;
      }

      @Override
      public Consumer init(ProtonServerSenderContext senderContext) throws Exception {
         SimpleString queueName = queue.getName();
         return (Consumer) sessionSPI.createSender(senderContext, queueName, null, false);
      }

      @Override
      public void close() throws Exception {
      }
   }

   public void disconnect() throws Exception {
      redoConnection();
   }

   @Override
   public void connectionCreated(ActiveMQComponent component, Connection connection, ClientProtocolManager protocol) {
   }

   @Override
   public void connectionDestroyed(Object connectionID) {
      server.getRemotingService().removeConnection(connectionID);
      redoConnection();
   }

   @Override
   public void connectionException(Object connectionID, ActiveMQException me) {
      redoConnection();
   }

   private void redoConnection() {

      // we need to use the connectExecutor to initiate a redoConnection
      // otherwise we would need to add synchronized blocks along this class
      // to control when connecting becomes true and when it becomes false
      // keeping a single executor thread to this purpose would simplify things
      connectExecutor.execute(() -> {
         if (connecting) {
            if (logger.isDebugEnabled()) {
               logger.debug("Broker connection " + this.getName() + " was already in retry mode, exception or retry not captured");
            }
            return;
         }
         connecting = true;

         try {
            if (protonRemotingConnection != null) {
               protonRemotingConnection.fail(new ActiveMQException("Connection being recreated"));
               connection = null;
               protonRemotingConnection = null;
            }
         } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
         }

         retryConnection();
      });
   }

   @Override
   public void connectionReadyForWrites(Object connectionID, boolean ready) {
      protonRemotingConnection.flush();
   }

   private static final String EXTERNAL = "EXTERNAL";
   private static final String PLAIN = "PLAIN";
   private static final String ANONYMOUS = "ANONYMOUS";
   private static final byte[] EMPTY = new byte[0];

   private static class PlainSASLMechanism implements ClientSASL {

      private final byte[] initialResponse;

      PlainSASLMechanism(String username, String password) {
         byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
         byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
         byte[] encoded = new byte[usernameBytes.length + passwordBytes.length + 2];
         System.arraycopy(usernameBytes, 0, encoded, 1, usernameBytes.length);
         System.arraycopy(passwordBytes, 0, encoded, usernameBytes.length + 2, passwordBytes.length);
         initialResponse = encoded;
      }

      @Override
      public String getName() {
         return PLAIN;
      }

      @Override
      public byte[] getInitialResponse() {
         return initialResponse;
      }

      @Override
      public byte[] getResponse(byte[] challenge) {
         return EMPTY;
      }

      public static boolean isApplicable(final String username, final String password) {
         return username != null && username.length() > 0 && password != null && password.length() > 0;
      }
   }

   private static class AnonymousSASLMechanism implements ClientSASL {

      @Override
      public String getName() {
         return ANONYMOUS;
      }

      @Override
      public byte[] getInitialResponse() {
         return EMPTY;
      }

      @Override
      public byte[] getResponse(byte[] challenge) {
         return EMPTY;
      }
   }

   private static class ExternalSASLMechanism implements ClientSASL {

      @Override
      public String getName() {
         return EXTERNAL;
      }

      @Override
      public byte[] getInitialResponse() {
         return EMPTY;
      }

      @Override
      public byte[] getResponse(byte[] challenge) {
         return EMPTY;
      }

      public static boolean isApplicable(final NettyConnection connection) {
         return CertificateUtil.getLocalPrincipalFromConnection(connection) != null;
      }
   }

   private static final class SaslFactory implements ClientSASLFactory {

      private final NettyConnection connection;
      private final AMQPBrokerConnectConfiguration brokerConnectConfiguration;

      SaslFactory(NettyConnection connection, AMQPBrokerConnectConfiguration brokerConnectConfiguration) {
         this.connection = connection;
         this.brokerConnectConfiguration = brokerConnectConfiguration;
      }

      @Override
      public ClientSASL chooseMechanism(String[] offeredMechanims) {
         List<String> availableMechanisms = offeredMechanims == null ? Collections.emptyList() : Arrays.asList(offeredMechanims);

         if (availableMechanisms.contains(EXTERNAL) && ExternalSASLMechanism.isApplicable(connection)) {
            return new ExternalSASLMechanism();
         }
         if (SCRAMClientSASL.isApplicable(brokerConnectConfiguration.getUser(),
                                          brokerConnectConfiguration.getPassword())) {
            for (SCRAM scram : SCRAM.values()) {
               if (availableMechanisms.contains(scram.getName())) {
                  return new SCRAMClientSASL(scram, brokerConnectConfiguration.getUser(),
                                             brokerConnectConfiguration.getPassword());
               }
            }
         }
         if (availableMechanisms.contains(PLAIN) && PlainSASLMechanism.isApplicable(brokerConnectConfiguration.getUser(), brokerConnectConfiguration.getPassword())) {
            return new PlainSASLMechanism(brokerConnectConfiguration.getUser(), brokerConnectConfiguration.getPassword());
         }

         if (availableMechanisms.contains(ANONYMOUS)) {
            return new AnonymousSASLMechanism();
         }

         return null;
      }
   }

}
