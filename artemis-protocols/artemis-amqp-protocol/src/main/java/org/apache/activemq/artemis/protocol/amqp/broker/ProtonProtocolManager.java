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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.protocol.amqp.converter.ProtonMessageConverter;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConstants;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;

/**
 * A proton protocol manager, basically reads the Proton Input and maps proton resources to ActiveMQ Artemis resources
 */
public class ProtonProtocolManager implements ProtocolManager<Interceptor>, NotificationListener {

   private static final List<String> websocketRegistryNames = Arrays.asList("amqp");

   private final ActiveMQServer server;

   private MessageConverter protonConverter;

   private final ProtonProtocolManagerFactory factory;

   /*
   * used when you want to treat senders as a subscription on an address rather than consuming from the actual queue for
   * the address. This can be changed on the acceptor.
   * */
   private String pubSubPrefix = ActiveMQDestination.JMS_TOPIC_ADDRESS_PREFIX;

   private int maxFrameSize = AMQPConstants.Connection.DEFAULT_MAX_FRAME_SIZE;

   public ProtonProtocolManager(ProtonProtocolManagerFactory factory, ActiveMQServer server) {
      this.factory = factory;
      this.server = server;
      this.protonConverter = new ProtonMessageConverter(server.getStorageManager());
   }

   public ActiveMQServer getServer() {
      return server;
   }

   @Override
   public MessageConverter getConverter() {
      return protonConverter;
   }

   @Override
   public void onNotification(Notification notification) {

   }

   @Override
   public ProtocolManagerFactory<Interceptor> getFactory() {
      return factory;
   }

   @Override
   public void updateInterceptors(List<BaseInterceptor> incomingInterceptors,
                                  List<BaseInterceptor> outgoingInterceptors) {
      // no op
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
      AMQPConnectionContext amqpConnection = new AMQPConnectionContext(connectionCallback, id, (int) ttl, getMaxFrameSize(), AMQPConstants.Connection.DEFAULT_CHANNEL_MAX, server.getExecutorFactory().getExecutor(), server.getScheduledPool());

      Executor executor = server.getExecutorFactory().getExecutor();

      ActiveMQProtonRemotingConnection delegate = new ActiveMQProtonRemotingConnection(this, amqpConnection, remotingConnection, executor);

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
}
