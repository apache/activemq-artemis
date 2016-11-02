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

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPConnectionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConstants;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.EventHandler;
import org.apache.activemq.artemis.spi.core.remoting.Connection;

import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * Connection factory for outgoing AMQP connections.
 */
public class AMQPClientConnectionFactory {

   private final ActiveMQServer server;
   private final String containerId;
   private final int ttl;

   public AMQPClientConnectionFactory(ActiveMQServer server, String containerId, int ttl) {
      this.server = server;
      this.containerId = containerId;
      this.ttl = ttl;
   }

   public ActiveMQProtonRemotingConnection createConnection(ProtonProtocolManager protocolManager, Connection connection, Optional<EventHandler> eventHandler) {
      AMQPConnectionCallback connectionCallback = new AMQPConnectionCallback(protocolManager, connection, server.getExecutorFactory().getExecutor(), server);

      Executor executor = server.getExecutorFactory().getExecutor();

      AMQPConnectionContext amqpConnection = new AMQPConnectionContext(connectionCallback, containerId, ttl, protocolManager.getMaxFrameSize(), AMQPConstants.Connection.DEFAULT_CHANNEL_MAX, executor, server.getScheduledPool());
      eventHandler.ifPresent(amqpConnection::addEventHandler);

      ActiveMQProtonRemotingConnection delegate = new ActiveMQProtonRemotingConnection(protocolManager, amqpConnection, connection, executor);

      connectionCallback.setProtonConnectionDelegate(delegate);

      amqpConnection.open();
      return delegate;
   }
}
