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
package org.apache.activemq.artemis.core.protocol.proton;

import java.util.List;
import java.util.concurrent.Executor;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.protocol.proton.converter.ProtonMessageConverter;
import org.apache.activemq.artemis.core.protocol.proton.plug.ActiveMQProtonConnectionCallback;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.proton.plug.AMQPServerConnectionContext;
import org.proton.plug.context.server.ProtonServerConnectionContextFactory;

import static org.proton.plug.context.AMQPConstants.Connection.DEFAULT_CHANNEL_MAX;
import static org.proton.plug.context.AMQPConstants.Connection.DEFAULT_MAX_FRAME_SIZE;

/**
 * A proton protocol manager, basically reads the Proton Input and maps proton resources to ActiveMQ Artemis resources
 */
public class ProtonProtocolManager implements ProtocolManager<Interceptor>, NotificationListener {

   private final ActiveMQServer server;

   private MessageConverter protonConverter;

   private final ProtonProtocolManagerFactory factory;

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
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection remotingConnection) {
      ActiveMQProtonConnectionCallback connectionCallback = new ActiveMQProtonConnectionCallback(this, remotingConnection);
      long ttl = ActiveMQClient.DEFAULT_CONNECTION_TTL;

      if (server.getConfiguration().getConnectionTTLOverride() != -1) {
         ttl = server.getConfiguration().getConnectionTTLOverride();
      }
      AMQPServerConnectionContext amqpConnection = ProtonServerConnectionContextFactory.getFactory().createConnection(connectionCallback, (int) ttl, DEFAULT_MAX_FRAME_SIZE, DEFAULT_CHANNEL_MAX);

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

}
