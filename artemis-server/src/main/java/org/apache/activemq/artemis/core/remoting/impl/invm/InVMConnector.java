/**
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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.spi.core.remoting.AbstractConnector;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.OrderedExecutorFactory;

public class InVMConnector extends AbstractConnector
{
   public static final Map<String, Object> DEFAULT_CONFIG;

   static
   {
      Map<String, Object> config = new HashMap<String , Object>();
      config.put(TransportConstants.SERVER_ID_PROP_NAME, TransportConstants.DEFAULT_SERVER_ID);
      DEFAULT_CONFIG = Collections.unmodifiableMap(config);
   }

   // Used for testing failure only
   public static volatile boolean failOnCreateConnection;

   public static volatile int numberOfFailures = -1;

   private static volatile int failures;

   public static synchronized void resetFailures()
   {
      InVMConnector.failures = 0;
      InVMConnector.failOnCreateConnection = false;
      InVMConnector.numberOfFailures = -1;
   }

   private static synchronized void incFailures()
   {
      InVMConnector.failures++;
      if (InVMConnector.failures == InVMConnector.numberOfFailures)
      {
         InVMConnector.resetFailures();
      }
   }

   protected final int id;

   private final ClientProtocolManager protocolManager;

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final InVMAcceptor acceptor;

   private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();

   private volatile boolean started;

   protected final OrderedExecutorFactory executorFactory;

   private final Executor closeExecutor;

   public InVMConnector(final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final ConnectionLifeCycleListener listener,
                        final Executor closeExecutor,
                        final Executor threadPool,
                        ClientProtocolManager protocolManager)
   {
      super(configuration);
      this.listener = listener;

      id = ConfigurationHelper.getIntProperty(TransportConstants.SERVER_ID_PROP_NAME, 0, configuration);

      this.handler = handler;

      this.closeExecutor = closeExecutor;

      executorFactory = new OrderedExecutorFactory(threadPool);

      InVMRegistry registry = InVMRegistry.instance;

      acceptor = registry.getAcceptor(id);

      this.protocolManager = protocolManager;
   }

   public Acceptor getAcceptor()
   {
      return acceptor;
   }

   public synchronized void close()
   {
      if (!started)
      {
         return;
      }

      for (Connection connection : connections.values())
      {
         listener.connectionDestroyed(connection.getID());
      }

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public Connection createConnection()
   {
      if (InVMConnector.failOnCreateConnection)
      {
         InVMConnector.incFailures();

         ActiveMQServerLogger.LOGGER.debug("Returning null on InVMConnector for tests");
         // For testing only
         return null;
      }

      if (acceptor == null)
      {
         return null;
      }

      if (acceptor.getConnectionsAllowed() == -1 || acceptor.getConnectionCount() < acceptor.getConnectionsAllowed())
      {
         Connection conn = internalCreateConnection(acceptor.getHandler(), new Listener(), acceptor.getExecutorFactory().getExecutor());

         acceptor.connect((String) conn.getID(), handler, this, executorFactory.getExecutor());
         return conn;
      }
      else
      {
         if (ActiveMQServerLogger.LOGGER.isDebugEnabled())
         {
            ActiveMQServerLogger.LOGGER.debug(new StringBuilder().append("Connection limit of ").append(acceptor.getConnectionsAllowed()).append(" reached. Refusing connection."));
         }
         return null;
      }
   }

   public synchronized void start()
   {
      started = true;
   }

   public BufferHandler getHandler()
   {
      return handler;
   }

   public void disconnect(final String connectionID)
   {
      if (!started)
      {
         return;
      }

      Connection conn = connections.get(connectionID);

      if (conn != null)
      {
         conn.close();
      }
   }

   // This may be an injection point for mocks on tests
   protected Connection internalCreateConnection(final BufferHandler handler,
                                                 final ConnectionLifeCycleListener listener,
                                                 final Executor serverExecutor)
   {
      // No acceptor on a client connection
      InVMConnection inVMConnection = new InVMConnection(id, handler, listener, serverExecutor);
      listener.connectionCreated(null, inVMConnection, protocolManager.getName());
      return inVMConnection;
   }

   public boolean isEquivalent(Map<String, Object> configuration)
   {
      int serverId = ConfigurationHelper.getIntProperty(TransportConstants.SERVER_ID_PROP_NAME,
                                            0,
                                            configuration);
      return id == serverId;
   }

   private class Listener implements ConnectionLifeCycleListener
   {
      public void connectionCreated(final ActiveMQComponent component, final Connection connection, final String protocol)
      {
         if (connections.putIfAbsent((String)connection.getID(), connection) != null)
         {
            throw ActiveMQMessageBundle.BUNDLE.connectionExists(connection.getID());
         }

         listener.connectionCreated(component, connection, protocol);
      }

      public void connectionDestroyed(final Object connectionID)
      {
         if (connections.remove(connectionID) != null)
         {
            // Close the corresponding connection on the other side
            acceptor.disconnect((String)connectionID);

            // Execute on different thread to avoid deadlocks
            closeExecutor.execute(new Runnable()
            {
               public void run()
               {
                  listener.connectionDestroyed(connectionID);
               }
            });
         }
      }

      public void connectionException(final Object connectionID, final ActiveMQException me)
      {
         // Execute on different thread to avoid deadlocks
         closeExecutor.execute(new Runnable()
         {
            public void run()
            {
               listener.connectionException(connectionID, me);
            }
         });
      }

      public void connectionReadyForWrites(Object connectionID, boolean ready)
      {
      }
   }

}
