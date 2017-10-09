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
package org.apache.activemq.artemis.protocol.amqp.broker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConstants;
import org.apache.activemq.artemis.protocol.amqp.sasl.MechanismFinder;
import org.apache.activemq.artemis.spi.core.protocol.AbstractProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;

/**
 * A proton protocol manager, basically reads the Proton Input and maps proton resources to ActiveMQ Artemis resources
 */
public class ProtonProtocolManager extends AbstractProtocolManager<AMQPMessage, AmqpInterceptor, ActiveMQProtonRemotingConnection> implements NotificationListener {

   private static final List<String> websocketRegistryNames = Arrays.asList("amqp");

   private final List<AmqpInterceptor> incomingInterceptors = new ArrayList<>();
   private final List<AmqpInterceptor> outgoingInterceptors = new ArrayList<>();

   private final ActiveMQServer server;

   private final ProtonProtocolManagerFactory factory;

   private final Map<SimpleString, RoutingType> prefixes = new HashMap<>();

   private int amqpCredits = 100;

   private int amqpLowCredits = 30;

   private int initialRemoteMaxFrameSize = 4 * 1024;

   private String[] saslMechanisms = MechanismFinder.getKnownMechanisms();

   private String saslLoginConfigScope = "amqp-sasl-gssapi";

   /*
   * used when you want to treat senders as a subscription on an address rather than consuming from the actual queue for
   * the address. This can be changed on the acceptor.
   * */
   // TODO fix this
   private String pubSubPrefix = ActiveMQDestination.TOPIC_QUALIFIED_PREFIX;

   private int maxFrameSize = AMQPConstants.Connection.DEFAULT_MAX_FRAME_SIZE;

   public ProtonProtocolManager(ProtonProtocolManagerFactory factory, ActiveMQServer server, List<BaseInterceptor> incomingInterceptors, List<BaseInterceptor> outgoingInterceptors) {
      this.factory = factory;
      this.server = server;
      this.updateInterceptors(incomingInterceptors, outgoingInterceptors);
   }

   public ActiveMQServer getServer() {
      return server;
   }

   @Override
   public void onNotification(Notification notification) {

   }

   @Override
   public ProtocolManagerFactory<AmqpInterceptor> getFactory() {
      return factory;
   }

   @Override
   public void updateInterceptors(List incoming, List outgoing) {
      this.incomingInterceptors.clear();
      this.incomingInterceptors.addAll(getFactory().filterInterceptors(incoming));

      this.outgoingInterceptors.clear();
      this.outgoingInterceptors.addAll(getFactory().filterInterceptors(outgoing));
   }

   @Override
   public boolean acceptsNoHandshake() {
      return false;
   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection remotingConnection) {
      AMQPConnectionCallback connectionCallback = new AMQPConnectionCallback(this, remotingConnection, server.getExecutorFactory().getExecutor(), server);
      long ttl = ActiveMQClient.DEFAULT_CONNECTION_TTL;

      if (server.getConfiguration().getConnectionTTLOverride() != -1) {
         ttl = server.getConfiguration().getConnectionTTLOverride();
      }

      String id = server.getConfiguration().getName();
      boolean useCoreSubscriptionNaming = server.getConfiguration().isAmqpUseCoreSubscriptionNaming();
      AMQPConnectionContext amqpConnection = new AMQPConnectionContext(this, connectionCallback, id, (int) ttl, getMaxFrameSize(), AMQPConstants.Connection.DEFAULT_CHANNEL_MAX, useCoreSubscriptionNaming, server.getScheduledPool(), true, null, null);

      Executor executor = server.getExecutorFactory().getExecutor();

      ActiveMQProtonRemotingConnection delegate = new ActiveMQProtonRemotingConnection(this, amqpConnection, remotingConnection, executor);
      delegate.addFailureListener(connectionCallback);
      delegate.addCloseListener(connectionCallback);

      connectionCallback.setProtonConnectionDelegate(delegate);

      ConnectionEntry entry = new ConnectionEntry(delegate, executor, System.currentTimeMillis(), ttl);

      return entry;
   }

   @Override
   public void removeHandler(String name) {

   }

   @Override
   public void handleBuffer(RemotingConnection connection, ActiveMQBuffer buffer) {
      ActiveMQProtonRemotingConnection protonConnection = (ActiveMQProtonRemotingConnection) connection;

      protonConnection.bufferReceived(protonConnection.getID(), buffer);
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline) {

   }

   public int getAmqpCredits() {
      return amqpCredits;
   }

   public ProtonProtocolManager setAmqpCredits(int amqpCredits) {
      this.amqpCredits = amqpCredits;
      return this;
   }

   public int getAmqpLowCredits() {
      return amqpLowCredits;
   }

   public ProtonProtocolManager setAmqpLowCredits(int amqpLowCredits) {
      this.amqpLowCredits = amqpLowCredits;
      return this;
   }

   @Override
   public boolean isProtocol(byte[] array) {
      return array.length >= 4 && array[0] == (byte) 'A' && array[1] == (byte) 'M' && array[2] == (byte) 'Q' && array[3] == (byte) 'P';
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer) {
   }

   @Override
   public List<String> websocketSubprotocolIdentifiers() {
      return websocketRegistryNames;
   }

   public String getPubSubPrefix() {
      return pubSubPrefix;
   }

   public void setPubSubPrefix(String pubSubPrefix) {
      this.pubSubPrefix = pubSubPrefix;
   }

   public int getMaxFrameSize() {
      return maxFrameSize;
   }

   public void setMaxFrameSize(int maxFrameSize) {
      this.maxFrameSize = maxFrameSize;
   }

   public String[] getSaslMechanisms() {
      return saslMechanisms;
   }

   public void setSaslMechanisms(String[] saslMechanisms) {
      this.saslMechanisms = saslMechanisms;
   }

   public String getSaslLoginConfigScope() {
      return saslLoginConfigScope;
   }

   public void setSaslLoginConfigScope(String saslLoginConfigScope) {
      this.saslLoginConfigScope = saslLoginConfigScope;
   }


   @Override
   public void setAnycastPrefix(String anycastPrefix) {
      for (String prefix : anycastPrefix.split(",")) {
         prefixes.put(SimpleString.toSimpleString(prefix), RoutingType.ANYCAST);
      }
   }

   @Override
   public void setMulticastPrefix(String multicastPrefix) {
      for (String prefix : multicastPrefix.split(",")) {
         prefixes.put(SimpleString.toSimpleString(prefix), RoutingType.MULTICAST);
      }
   }

   @Override
   public Map<SimpleString, RoutingType> getPrefixes() {
      return prefixes;
   }

   public void invokeIncoming(AMQPMessage message, ActiveMQProtonRemotingConnection connection) {
      super.invokeInterceptors(this.incomingInterceptors, message, connection);
   }

   public void invokeOutgoing(AMQPMessage message, ActiveMQProtonRemotingConnection connection) {
      super.invokeInterceptors(this.outgoingInterceptors, message, connection);
   }

   public int getInitialRemoteMaxFrameSize() {
      return initialRemoteMaxFrameSize;
   }

   public void setInitialRemoteMaxFrameSize(int initialRemoteMaxFrameSize) {
      this.initialRemoteMaxFrameSize = initialRemoteMaxFrameSize;
   }

}
