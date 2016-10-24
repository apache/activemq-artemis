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
package org.apache.activemq.artemis.protocol.amqp.client;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPConnectionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConstants;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.remoting.BaseConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Manages the lifecycle of a proton client connection.
 */
public class ProtonClientConnectionLifeCycleListener implements BaseConnectionLifeCycleListener<ProtonProtocolManager>, BufferHandler {
   private final Map<Object, ConnectionEntry> connectionMap = new ConcurrentHashMap<>();
   private final ActiveMQServer server;
   private final int ttl;
   private static final Logger log = Logger.getLogger(ProtonClientConnectionLifeCycleListener.class);

   public ProtonClientConnectionLifeCycleListener(ActiveMQServer server, int ttl) {
      this.server = server;
      this.ttl = ttl;
   }

   @Override
   public void connectionCreated(ActiveMQComponent component, Connection connection, ProtonProtocolManager protocolManager) {
      AMQPConnectionCallback connectionCallback = new AMQPConnectionCallback(protocolManager, connection, server.getExecutorFactory().getExecutor(), server);

      String id = server.getConfiguration().getName();
      AMQPConnectionContext amqpConnection = new AMQPConnectionContext(connectionCallback, id, ttl, protocolManager.getMaxFrameSize(), AMQPConstants.Connection.DEFAULT_CHANNEL_MAX, server.getExecutorFactory().getExecutor(), server.getScheduledPool());
      Executor executor = server.getExecutorFactory().getExecutor();
      amqpConnection.open();

      ActiveMQProtonRemotingConnection delegate = new ActiveMQProtonRemotingConnection(protocolManager, amqpConnection, connection, executor);

      connectionCallback.setProtonConnectionDelegate(delegate);

      ConnectionEntry connectionEntry = new ConnectionEntry(delegate, executor, System.currentTimeMillis(), ttl);
      connectionMap.put(connection.getID(), connectionEntry);
      log.info("Connection " + connection.getRemoteAddress() + " created");
   }

   @Override
   public void connectionDestroyed(Object connectionID) {
      ConnectionEntry connection = connectionMap.remove(connectionID);
      if (connection != null) {
         log.info("Connection " + connection.connection.getRemoteAddress() + " destroyed");
         connection.connection.disconnect(false);
      }
   }

   @Override
   public void connectionException(Object connectionID, ActiveMQException me) {
      ConnectionEntry connection = connectionMap.get(connectionID);
      if (connection != null) {
         log.info("Connection " + connection.connection.getRemoteAddress() + " exception: " + me.getMessage());
         connection.connection.fail(me);
      }
   }

   @Override
   public void connectionReadyForWrites(Object connectionID, boolean ready) {
      ConnectionEntry connection = connectionMap.get(connectionID);
      if (connection != null) {
         log.info("Connection " + connection.connection.getRemoteAddress() + " ready");
         connection.connection.getTransportConnection().fireReady(true);
      }
   }

   public void stop() {
      for (ConnectionEntry entry : connectionMap.values()) {
         entry.connection.disconnect(false);
      }
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
      ConnectionEntry entry = connectionMap.get(connectionID);
      if (entry != null) {
         entry.connection.bufferReceived(connectionID, buffer);
      }
   }
}
