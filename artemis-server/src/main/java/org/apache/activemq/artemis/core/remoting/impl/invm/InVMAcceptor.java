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
package org.apache.activemq.artemis.core.remoting.impl.invm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.core.security.ActiveMQPrincipal;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationService;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.TypedProperties;

public final class InVMAcceptor implements Acceptor {

   private final int id;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();

   private volatile boolean started;

   private final ExecutorFactory executorFactory;

   private final ClusterConnection clusterConnection;

   private boolean paused;

   private NotificationService notificationService;

   private final Map<String, Object> configuration;

   private ActiveMQPrincipal defaultActiveMQPrincipal;

   private final long connectionsAllowed;

   private final String name;

   public InVMAcceptor(final String name,
                       final ClusterConnection clusterConnection,
                       final Map<String, Object> configuration,
                       final BufferHandler handler,
                       final ConnectionLifeCycleListener listener,
                       final Executor threadPool) {

      this.name = name;

      this.clusterConnection = clusterConnection;

      this.configuration = configuration;

      this.handler = handler;

      this.listener = listener;

      id = ConfigurationHelper.getIntProperty(TransportConstants.SERVER_ID_PROP_NAME, 0, configuration);

      executorFactory = new OrderedExecutorFactory(threadPool);

      connectionsAllowed = ConfigurationHelper.getLongProperty(TransportConstants.CONNECTIONS_ALLOWED, TransportConstants.DEFAULT_CONNECTIONS_ALLOWED, configuration);
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public Map<String, Object> getConfiguration() {
      return configuration;
   }

   @Override
   public ClusterConnection getClusterConnection() {
      return clusterConnection;
   }

   public long getConnectionsAllowed() {
      return connectionsAllowed;
   }

   public int getConnectionCount() {
      return connections.size();
   }

   @Override
   public synchronized void start() throws Exception {
      if (started) {
         return;
      }

      InVMRegistry.instance.registerAcceptor(id, this);

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("factory"), new SimpleString(InVMAcceptorFactory.class.getName()));
         props.putIntProperty(new SimpleString("id"), id);
         Notification notification = new Notification(null, CoreNotificationType.ACCEPTOR_STARTED, props);
         notificationService.sendNotification(notification);
      }

      started = true;

      paused = false;
   }

   @Override
   public synchronized void stop() {
      if (!started) {
         return;
      }

      if (!paused) {
         InVMRegistry.instance.unregisterAcceptor(id);
      }

      for (Connection connection : connections.values()) {
         listener.connectionDestroyed(connection.getID());
      }

      connections.clear();

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("factory"), new SimpleString(InVMAcceptorFactory.class.getName()));
         props.putIntProperty(new SimpleString("id"), id);
         Notification notification = new Notification(null, CoreNotificationType.ACCEPTOR_STOPPED, props);
         try {
            notificationService.sendNotification(notification);
         }
         catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }

      started = false;

      paused = false;
   }

   @Override
   public synchronized boolean isStarted() {
      return started;
   }

   /*
    * Stop accepting new connections
    */
   @Override
   public synchronized void pause() {
      if (!started || paused) {
         return;
      }

      InVMRegistry.instance.unregisterAcceptor(id);

      paused = true;
   }

   @Override
   public synchronized void setNotificationService(final NotificationService notificationService) {
      this.notificationService = notificationService;
   }

   public BufferHandler getHandler() {
      if (!started) {
         throw new IllegalStateException("Acceptor is not started");
      }

      return handler;
   }

   public ExecutorFactory getExecutorFactory() {
      return executorFactory;
   }

   public void connect(final String connectionID,
                       final BufferHandler remoteHandler,
                       final InVMConnector connector,
                       final Executor clientExecutor) {
      if (!started) {
         throw new IllegalStateException("Acceptor is not started");
      }

      Listener connectionListener = new Listener(connector);

      InVMConnection inVMConnection = new InVMConnection(id, connectionID, remoteHandler, connectionListener, clientExecutor, defaultActiveMQPrincipal);

      connectionListener.connectionCreated(this, inVMConnection, ActiveMQClient.DEFAULT_CORE_PROTOCOL);
   }

   public void disconnect(final String connectionID) {
      if (!started) {
         return;
      }

      Connection conn = connections.get(connectionID);

      if (conn != null) {
         conn.close();
      }
   }

   /**
    * we are InVM so allow unsecure connections
    *
    * @return true
    */
   @Override
   public boolean isUnsecurable() {
      return true;
   }

   @Override
   public void setDefaultActiveMQPrincipal(ActiveMQPrincipal defaultActiveMQPrincipal) {
      this.defaultActiveMQPrincipal = defaultActiveMQPrincipal;
   }

   private class Listener implements ConnectionLifeCycleListener {
      //private static Listener instance = new Listener();

      private final InVMConnector connector;

      Listener(final InVMConnector connector) {
         this.connector = connector;
      }

      @Override
      public void connectionCreated(final ActiveMQComponent component,
                                    final Connection connection,
                                    final String protocol) {
         if (connections.putIfAbsent((String) connection.getID(), connection) != null) {
            throw ActiveMQMessageBundle.BUNDLE.connectionExists(connection.getID());
         }

         listener.connectionCreated(component, connection, protocol);
      }

      @Override
      public void connectionDestroyed(final Object connectionID) {
         InVMConnection connection = (InVMConnection) connections.remove(connectionID);

         if (connection != null) {

            listener.connectionDestroyed(connectionID);

            // Execute on different thread after all the packets are sent, to avoid deadlocks
            connection.getExecutor().execute(new Runnable() {
               @Override
               public void run() {
                  // Remove on the other side too
                  connector.disconnect((String) connectionID);
               }
            });
         }
      }

      @Override
      public void connectionException(final Object connectionID, final ActiveMQException me) {
         listener.connectionException(connectionID, me);
      }

      @Override
      public void connectionReadyForWrites(Object connectionID, boolean ready) {
      }
   }

}
