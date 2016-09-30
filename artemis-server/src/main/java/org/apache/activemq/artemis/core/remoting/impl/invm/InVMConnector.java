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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.spi.core.remoting.AbstractConnector;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.BaseConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.ClientConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ActiveMQThreadPoolExecutor;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.OrderedExecutorFactory;
import org.jboss.logging.Logger;

public class InVMConnector extends AbstractConnector {

   private static final Logger logger = Logger.getLogger(InVMConnector.class);

   public static final Map<String, Object> DEFAULT_CONFIG;

   static {
      Map<String, Object> config = new HashMap<>();
      config.put(TransportConstants.SERVER_ID_PROP_NAME, TransportConstants.DEFAULT_SERVER_ID);
      DEFAULT_CONFIG = Collections.unmodifiableMap(config);
   }

   // Used for testing failure only
   public static volatile boolean failOnCreateConnection;

   public static volatile int numberOfFailures = -1;

   private static volatile int failures;

   public static synchronized void resetFailures() {
      InVMConnector.failures = 0;
      InVMConnector.failOnCreateConnection = false;
      InVMConnector.numberOfFailures = -1;
   }

   private static synchronized void incFailures() {
      InVMConnector.failures++;
      if (InVMConnector.failures == InVMConnector.numberOfFailures) {
         InVMConnector.resetFailures();
      }
   }

   protected final int id;

   private final ClientProtocolManager protocolManager;

   private final BufferHandler handler;

   private final BaseConnectionLifeCycleListener listener;

   private final InVMAcceptor acceptor;

   private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<>();

   private volatile boolean started;

   protected final OrderedExecutorFactory executorFactory;

   private final Executor closeExecutor;

   private static ExecutorService threadPoolExecutor;

   public static synchronized void resetThreadPool() {
      if (threadPoolExecutor != null) {
         threadPoolExecutor.shutdown();
         threadPoolExecutor = null;
      }
   }

   private static synchronized ExecutorService getInVMExecutor() {
      if (threadPoolExecutor == null) {
         if (ActiveMQClient.getGlobalThreadPoolSize() <= -1) {
            threadPoolExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), ActiveMQThreadFactory.defaultThreadFactory());
         } else {
            threadPoolExecutor = new ActiveMQThreadPoolExecutor(0, ActiveMQClient.getGlobalThreadPoolSize(), 60L, TimeUnit.SECONDS, ActiveMQThreadFactory.defaultThreadFactory());
         }
      }
      return threadPoolExecutor;
   }

   public InVMConnector(final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final ClientConnectionLifeCycleListener listener,
                        final Executor closeExecutor,
                        final Executor threadPool,
                        ClientProtocolManager protocolManager) {
      super(configuration);
      this.listener = listener;

      id = ConfigurationHelper.getIntProperty(TransportConstants.SERVER_ID_PROP_NAME, 0, configuration);

      this.handler = handler;

      this.closeExecutor = closeExecutor;

      executorFactory = new OrderedExecutorFactory(getInVMExecutor());

      InVMRegistry registry = InVMRegistry.instance;

      acceptor = registry.getAcceptor(id);

      this.protocolManager = protocolManager;
   }

   public Acceptor getAcceptor() {
      return acceptor;
   }

   @Override
   public synchronized void close() {
      if (!started) {
         return;
      }

      for (Connection connection : connections.values()) {
         listener.connectionDestroyed(connection.getID());
      }

      started = false;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public Connection createConnection() {
      if (InVMConnector.failOnCreateConnection) {
         InVMConnector.incFailures();

         logger.debug("Returning null on InVMConnector for tests");
         // For testing only
         return null;
      }

      if (acceptor == null) {
         return null;
      }

      if (acceptor.getConnectionsAllowed() == -1 || acceptor.getConnectionCount() < acceptor.getConnectionsAllowed()) {
         Connection conn = internalCreateConnection(acceptor.getHandler(), new Listener(), acceptor.getExecutorFactory().getExecutor());

         acceptor.connect((String) conn.getID(), handler, this, executorFactory.getExecutor());
         return conn;
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug(new StringBuilder().append("Connection limit of ").append(acceptor.getConnectionsAllowed()).append(" reached. Refusing connection."));
         }
         return null;
      }
   }

   @Override
   public synchronized void start() {
      started = true;
   }

   public BufferHandler getHandler() {
      return handler;
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

   // This may be an injection point for mocks on tests
   protected Connection internalCreateConnection(final BufferHandler handler,
                                                 final ClientConnectionLifeCycleListener listener,
                                                 final Executor serverExecutor) {
      // No acceptor on a client connection
      InVMConnection inVMConnection = new InVMConnection(id, handler, listener, serverExecutor);
      listener.connectionCreated(null, inVMConnection, protocolManager);
      return inVMConnection;
   }

   @Override
   public boolean isEquivalent(Map<String, Object> configuration) {
      int serverId = ConfigurationHelper.getIntProperty(TransportConstants.SERVER_ID_PROP_NAME, 0, configuration);
      return id == serverId;
   }

   private class Listener implements ClientConnectionLifeCycleListener {

      @Override
      public void connectionCreated(final ActiveMQComponent component,
                                    final Connection connection,
                                    final ClientProtocolManager protocol) {
         if (connections.putIfAbsent((String) connection.getID(), connection) != null) {
            throw ActiveMQMessageBundle.BUNDLE.connectionExists(connection.getID());
         }

         if (listener instanceof ConnectionLifeCycleListener) {
            listener.connectionCreated(component, connection, protocol.getName());
         } else {
            listener.connectionCreated(component, connection, protocol);
         }

      }

      @Override
      public void connectionDestroyed(final Object connectionID) {
         if (connections.remove(connectionID) != null) {
            // Close the corresponding connection on the other side
            acceptor.disconnect((String) connectionID);

            // Execute on different thread to avoid deadlocks
            closeExecutor.execute(new Runnable() {
               @Override
               public void run() {
                  listener.connectionDestroyed(connectionID);
               }
            });
         }
      }

      @Override
      public void connectionException(final Object connectionID, final ActiveMQException me) {
         // Execute on different thread to avoid deadlocks
         closeExecutor.execute(new Runnable() {
            @Override
            public void run() {
               listener.connectionException(connectionID, me);
            }
         });
      }

      @Override
      public void connectionReadyForWrites(Object connectionID, boolean ready) {
      }
   }

}
