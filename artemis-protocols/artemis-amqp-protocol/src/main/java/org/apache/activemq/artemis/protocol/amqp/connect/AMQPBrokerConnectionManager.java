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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.ClientConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.SessionContext;
import org.apache.activemq.artemis.spi.core.remoting.TopologyResponseHandler;
import org.jboss.logging.Logger;

public class AMQPBrokerConnectionManager implements ActiveMQComponent, ClientConnectionLifeCycleListener {
   private static final Logger logger = Logger.getLogger(AMQPBrokerConnectionManager.class);

   private final ProtonProtocolManagerFactory protonProtocolManagerFactory;
   private final ActiveMQServer server;
   private volatile boolean started = false;

   List<AMQPBrokerConnectConfiguration> amqpConnectionsConfig;
   List<AMQPBrokerConnection> amqpBrokerConnectionList;
   ProtonProtocolManager protonProtocolManager;

   public AMQPBrokerConnectionManager(ProtonProtocolManagerFactory factory, List<AMQPBrokerConnectConfiguration> amqpConnectionsConfig, ActiveMQServer server) {
      this.amqpConnectionsConfig = amqpConnectionsConfig;
      this.server = server;
      this.protonProtocolManagerFactory = factory;
   }

   @Override
   public void start() throws Exception {
      if (!started) {
         started = true;
      }

      amqpBrokerConnectionList = new ArrayList<>();


      for (AMQPBrokerConnectConfiguration config : amqpConnectionsConfig) {
         NettyConnectorFactory factory = new NettyConnectorFactory().setServerConnector(true);
         protonProtocolManager = (ProtonProtocolManager)protonProtocolManagerFactory.createProtocolManager(server, config.getTransportConfigurations().get(0).getExtraParams(), null, null);
         NettyConnector bridgesConnector = (NettyConnector)factory.createConnector(config.getTransportConfigurations().get(0).getParams(), null, this, server.getExecutorFactory().getExecutor(), server.getThreadPool(), server.getScheduledPool(), new ClientProtocolManagerWithAMQP(protonProtocolManager));
         bridgesConnector.start();

         if (logger.isDebugEnabled()) {
            logger.debug("Connecting " + config);
         }
         AMQPBrokerConnection amqpBrokerConnection = new AMQPBrokerConnection(this, config, protonProtocolManager, server, bridgesConnector);
         amqpBrokerConnectionList.add(amqpBrokerConnection);
         server.registerBrokerConnection(amqpBrokerConnection);
         if (config.isAutostart()) {
            amqpBrokerConnection.start();
         }
      }
   }

   public void connected(NettyConnection nettyConnection, AMQPBrokerConnection bridgeConnection) {
   }

   @Override
   public void stop() throws Exception {
      if (started) {
         started = false;
         for (AMQPBrokerConnection connection : amqpBrokerConnectionList) {
            connection.stop();
         }
      }
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public void connectionCreated(ActiveMQComponent component, Connection connection, ClientProtocolManager protocol) {
   }


   @Override
   public void connectionDestroyed(Object connectionID) {
      for (AMQPBrokerConnection connection : amqpBrokerConnectionList) {
         if (connection.getConnection() != null && connectionID.equals(connection.getConnection().getID())) {
            connection.connectionDestroyed(connectionID);
         }
      }
   }

   @Override
   public void connectionException(Object connectionID, ActiveMQException me) {
      for (AMQPBrokerConnection connection : amqpBrokerConnectionList) {
         if (connection.getConnection() != null && connectionID.equals(connection.getConnection().getID())) {
            connection.connectionException(connectionID, me);
         }
      }

   }

   @Override
   public void connectionReadyForWrites(Object connectionID, boolean ready) {
      for (AMQPBrokerConnection connection : amqpBrokerConnectionList) {
         if (connection.getConnection().getID().equals(connectionID)) {
            connection.connectionReadyForWrites(connectionID, ready);
         }
      }
   }

   /** The Client Protocol Manager is used for Core Clients.
    *  As we are reusing the NettyConnector the API requires a ClientProtocolManager.
    *  This is to give us the reference for the AMQPConnection used. */
   public static class ClientProtocolManagerWithAMQP implements ClientProtocolManager {
      public final ProtonProtocolManager protonPM;

      public ClientProtocolManagerWithAMQP(ProtonProtocolManager protonPM) {
         this.protonPM = protonPM;
      }

      public ProtonProtocolManager getProtonPM() {
         return protonPM;
      }

      @Override
      public ClientProtocolManager setExecutor(Executor executor) {
         return null;
      }

      @Override
      public RemotingConnection connect(Connection transportConnection,
                                        long callTimeout,
                                        long callFailoverTimeout,
                                        List<Interceptor> incomingInterceptors,
                                        List<Interceptor> outgoingInterceptors,
                                        TopologyResponseHandler topologyResponseHandler) {
         return null;
      }

      @Override
      public RemotingConnection getCurrentConnection() {
         return null;
      }

      @Override
      public Lock lockSessionCreation() {
         return null;
      }

      @Override
      public boolean waitOnLatch(long milliseconds) throws InterruptedException {
         return false;
      }

      @Override
      public void stop() {

      }

      @Override
      public boolean isAlive() {
         return false;
      }

      @Override
      public void addChannelHandlers(ChannelPipeline pipeline) {

      }

      @Override
      public void sendSubscribeTopology(boolean isServer) {

      }

      @Override
      public void ping(long connectionTTL) {

      }

      @Override
      public SessionContext createSessionContext(String name,
                                                 String username,
                                                 String password,
                                                 boolean xa,
                                                 boolean autoCommitSends,
                                                 boolean autoCommitAcks,
                                                 boolean preAcknowledge,
                                                 int minLargeMessageSize,
                                                 int confirmationWindowSize) throws ActiveMQException {
         return null;
      }

      @Override
      public boolean cleanupBeforeFailover(ActiveMQException cause) {
         return false;
      }

      @Override
      public boolean checkForFailover(String liveNodeID) throws ActiveMQException {
         return false;
      }

      @Override
      public void setSessionFactory(ClientSessionFactory factory) {

      }

      @Override
      public ClientSessionFactory getSessionFactory() {
         return null;
      }

      @Override
      public String getName() {
         return null;
      }
   }


}
