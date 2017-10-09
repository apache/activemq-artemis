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
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.EventHandler;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASLFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.BaseConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle of a proton client connection.
 */
public class ProtonClientConnectionManager implements BaseConnectionLifeCycleListener<ProtonProtocolManager>, BufferHandler {
   private final Map<Object, ActiveMQProtonRemotingConnection> connectionMap = new ConcurrentHashMap<>();
   private static final Logger log = Logger.getLogger(ProtonClientConnectionManager.class);
   private final AMQPClientConnectionFactory connectionFactory;
   private final Optional<EventHandler> eventHandler;
   private final ClientSASLFactory clientSASLFactory;

   public ProtonClientConnectionManager(AMQPClientConnectionFactory connectionFactory, Optional<EventHandler> eventHandler, ClientSASLFactory clientSASLFactory) {
      this.connectionFactory = connectionFactory;
      this.eventHandler = eventHandler;
      this.clientSASLFactory = clientSASLFactory;
   }

   @Override
   public void connectionCreated(ActiveMQComponent component, Connection connection, ProtonProtocolManager protocolManager) {
      ActiveMQProtonRemotingConnection amqpConnection = connectionFactory.createConnection(protocolManager, connection, eventHandler, clientSASLFactory);
      connectionMap.put(connection.getID(), amqpConnection);
      amqpConnection.open();

      log.info("Connection " + amqpConnection.getRemoteAddress() + " created");
   }

   @Override
   public void connectionDestroyed(Object connectionID) {
      RemotingConnection connection = connectionMap.remove(connectionID);
      if (connection != null) {
         log.info("Connection " + connection.getRemoteAddress() + " destroyed");
         connection.disconnect(false);
      } else {
         log.error("Connection with id " + connectionID + " not found in connectionDestroyed");
      }
   }

   @Override
   public void connectionException(Object connectionID, ActiveMQException me) {
      RemotingConnection connection = connectionMap.get(connectionID);
      if (connection != null) {
         log.info("Connection " + connection.getRemoteAddress() + " exception: " + me.getMessage());
         connection.fail(me);
      } else {
         log.error("Connection with id " + connectionID + " not found in connectionException");
      }
   }

   @Override
   public void connectionReadyForWrites(Object connectionID, boolean ready) {
      RemotingConnection connection = connectionMap.get(connectionID);
      if (connection != null) {
         log.info("Connection " + connection.getRemoteAddress() + " ready");
         connection.getTransportConnection().fireReady(true);
      } else {
         log.error("Connection with id " + connectionID + " not found in connectionReadyForWrites()!");
      }
   }

   public void stop() {
      for (RemotingConnection connection : connectionMap.values()) {
         connection.disconnect(false);
      }
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
      RemotingConnection connection = connectionMap.get(connectionID);
      if (connection != null) {
         connection.bufferReceived(connectionID, buffer);
      } else {
         log.error("Connection with id " + connectionID + " not found in bufferReceived()!");
      }
   }
}
