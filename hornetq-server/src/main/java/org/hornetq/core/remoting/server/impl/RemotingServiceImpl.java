/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.remoting.server.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.impl.CoreProtocolManagerFactory;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.security.HornetQPrincipal;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.ProtocolManagerFactory;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.AcceptorFactory;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.utils.ClassloadingUtil;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.HornetQThreadFactory;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class RemotingServiceImpl implements RemotingService, ConnectionLifeCycleListener
{
   // Constants -----------------------------------------------------

   private static final boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   public static final long CONNECTION_TTL_CHECK_INTERVAL = 2000;

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   private final Set<TransportConfiguration> acceptorsConfig;

   private final List<Interceptor> incomingInterceptors = new CopyOnWriteArrayList<Interceptor>();

   private final List<Interceptor> outgoingInterceptors = new CopyOnWriteArrayList<Interceptor>();

   private final Map<String, Acceptor> acceptors = new HashMap<String, Acceptor>();

   private final Map<Object, ConnectionEntry> connections = new ConcurrentHashMap<Object, ConnectionEntry>();

   private final HornetQServer server;

   private final ManagementService managementService;

   private ExecutorService threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private FailureCheckAndFlushThread failureCheckAndFlushThread;

   private final ClusterManager clusterManager;

   private final Map<String, ProtocolManager> protocolMap = new ConcurrentHashMap();

   private HornetQPrincipal defaultInvmSecurityPrincipal;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingServiceImpl(final ClusterManager clusterManager,
                              final Configuration config,
                              final HornetQServer server,
                              final ManagementService managementService,
                              final ScheduledExecutorService scheduledThreadPool, List<ProtocolManagerFactory> protocolManagerFactories)
   {
      acceptorsConfig = config.getAcceptorConfigurations();

      this.server = server;

      this.clusterManager = clusterManager;

      for (String interceptorClass : config.getIncomingInterceptorClassNames())
      {
         try
         {
            incomingInterceptors.add((Interceptor) safeInitNewInstance(interceptorClass));
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorCreatingRemotingInterceptor(e, interceptorClass);
         }
      }

      for (String interceptorClass : config.getOutgoingInterceptorClassNames())
      {
         try
         {
            outgoingInterceptors.add((Interceptor) safeInitNewInstance(interceptorClass));
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorCreatingRemotingInterceptor(e, interceptorClass);
         }
      }
      this.managementService = managementService;

      this.scheduledThreadPool = scheduledThreadPool;

      CoreProtocolManagerFactory coreProtocolManagerFactory = new CoreProtocolManagerFactory();
      //i know there is only 1
      HornetQServerLogger.LOGGER.addingProtocolSupport(coreProtocolManagerFactory.getProtocols()[0]);
      this.protocolMap.put(coreProtocolManagerFactory.getProtocols()[0],
                           coreProtocolManagerFactory.createProtocolManager(server, incomingInterceptors, outgoingInterceptors));

      if (config.isResolveProtocols())
      {
         ServiceLoader<ProtocolManagerFactory> serviceLoader = ServiceLoader.load(ProtocolManagerFactory.class, this.getClass().getClassLoader());
         if (serviceLoader != null)
         {
            for (ProtocolManagerFactory next : serviceLoader)
            {
               String[] protocols = next.getProtocols();
               for (String protocol : protocols)
               {
                  HornetQServerLogger.LOGGER.addingProtocolSupport(protocol);
                  protocolMap.put(protocol, next.createProtocolManager(server, incomingInterceptors, outgoingInterceptors));
               }
            }
         }
      }

      if (protocolManagerFactories != null)
      {
         for (ProtocolManagerFactory protocolManagerFactory : protocolManagerFactories)
         {
            String[] protocols = protocolManagerFactory.getProtocols();
            for (String protocol : protocols)
            {
               HornetQServerLogger.LOGGER.addingProtocolSupport(protocol);
               protocolMap.put(protocol, protocolManagerFactory.createProtocolManager(server, incomingInterceptors, outgoingInterceptors));
            }
         }
      }
   }

   // RemotingService implementation -------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      ClassLoader tccl = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return Thread.currentThread().getContextClassLoader();
         }
      });

      // The remoting service maintains it's own thread pool for handling remoting traffic
      // If OIO each connection will have it's own thread
      // If NIO these are capped at nio-remoting-threads which defaults to num cores * 3
      // This needs to be a different thread pool to the main thread pool especially for OIO where we may need
      // to support many hundreds of connections, but the main thread pool must be kept small for better performance

      ThreadFactory tFactory = new HornetQThreadFactory("HornetQ-remoting-threads-" + server.toString() +
                                                           "-" +
                                                           System.identityHashCode(this), false, tccl);

      threadPool = Executors.newCachedThreadPool(tFactory);

      ClassLoader loader = Thread.currentThread().getContextClassLoader();

      for (TransportConfiguration info : acceptorsConfig)
      {
         try
         {
            Class<?> clazz = loader.loadClass(info.getFactoryClassName());

            AcceptorFactory factory = (AcceptorFactory) clazz.newInstance();

            // Check valid properties

            if (info.getParams() != null)
            {
               Set<String> invalid = ConfigurationHelper.checkKeys(factory.getAllowableProperties(), info.getParams()
                  .keySet());

               if (!invalid.isEmpty())
               {
                  HornetQServerLogger.LOGGER.invalidAcceptorKeys(ConfigurationHelper.stringSetToCommaListString(invalid));

                  continue;
               }
            }

            Map<String, ProtocolManager> supportedProtocols = new ConcurrentHashMap();

            String protocol = ConfigurationHelper.getStringProperty(TransportConstants.PROTOCOL_PROP_NAME, null,
                                                                    info.getParams());

            if (protocol != null)
            {
               HornetQServerLogger.LOGGER.warnDeprecatedProtocol();
               ProtocolManager protocolManager = protocolMap.get(protocol);

               if (protocolManager == null)
               {
                  throw HornetQMessageBundle.BUNDLE.noProtocolManagerFound(protocol);
               }
               else
               {
                  supportedProtocols.put(protocol, protocolManager);
               }
            }

            String protocols = ConfigurationHelper.getStringProperty(TransportConstants.PROTOCOLS_PROP_NAME, null,
                                                                     info.getParams());

            if (protocols != null)
            {
               String[] actualProtocols = protocols.split(",");

               if (actualProtocols != null)
               {
                  for (String actualProtocol : actualProtocols)
                  {
                     ProtocolManager protocolManager = protocolMap.get(actualProtocol);

                     if (protocolManager == null)
                     {
                        throw HornetQMessageBundle.BUNDLE.noProtocolManagerFound(actualProtocol);
                     }
                     else
                     {
                        supportedProtocols.put(actualProtocol, protocolManager);
                     }
                  }
               }
            }

            ClusterConnection clusterConnection = lookupClusterConnection(info);

            Acceptor acceptor = factory.createAcceptor(info.getName(),
                                                       clusterConnection,
                                                       info.getParams(),
                                                       new DelegatingBufferHandler(),
                                                       this,
                                                       threadPool,
                                                       scheduledThreadPool,
                                                       supportedProtocols.isEmpty() ? protocolMap : supportedProtocols);

            if (defaultInvmSecurityPrincipal != null && acceptor.isUnsecurable())
            {
               acceptor.setDefaultHornetQPrincipal(defaultInvmSecurityPrincipal);
            }

            acceptors.put(info.getName(), acceptor);

            if (managementService != null)
            {
               acceptor.setNotificationService(managementService);

               managementService.registerAcceptor(acceptor, info);
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorCreatingAcceptor(e, info.getFactoryClassName());
         }
      }

      for (Acceptor a : acceptors.values())
      {
         a.start();
      }

      // This thread checks connections that need to be closed, and also flushes confirmations
      failureCheckAndFlushThread = new FailureCheckAndFlushThread(RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL);

      failureCheckAndFlushThread.start();

      started = true;
   }

   public synchronized void allowInvmSecurityOverride(HornetQPrincipal principal)
   {
      defaultInvmSecurityPrincipal = principal;
      for (Acceptor acceptor : acceptors.values())
      {
         if (acceptor.isUnsecurable())
         {
            acceptor.setDefaultHornetQPrincipal(principal);
         }
      }
   }

   public synchronized void freeze(final String scaleDownNodeID, final CoreRemotingConnection connectionToKeepOpen)
   {
      if (!started)
         return;
      failureCheckAndFlushThread.close(false);

      for (Acceptor acceptor : acceptors.values())
      {
         try
         {
            acceptor.pause();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorStoppingAcceptor();
         }
      }
      HashMap<Object, ConnectionEntry> connectionEntries = new HashMap<Object, ConnectionEntry>(connections);

      // Now we ensure that no connections will process any more packets after this method is
      // complete then send a disconnect packet
      for (Entry<Object, ConnectionEntry> entry : connectionEntries.entrySet())
      {
         RemotingConnection conn = entry.getValue().connection;

         if (conn.equals(connectionToKeepOpen))
            continue;

         if (HornetQServerLogger.LOGGER.isTraceEnabled())
         {
            HornetQServerLogger.LOGGER.trace("Sending connection.disconnection packet to " + conn);
         }

         if (!conn.isClient())
         {
            conn.disconnect(scaleDownNodeID, false);
            connections.remove(entry.getKey());
         }
      }
   }

   public void stop(final boolean criticalError) throws Exception
   {
      if (!started)
      {
         return;
      }

      failureCheckAndFlushThread.close(criticalError);

      // We need to stop them accepting first so no new connections are accepted after we send the disconnect message
      for (Acceptor acceptor : acceptors.values())
      {
         if (HornetQServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQServerLogger.LOGGER.debug("Pausing acceptor " + acceptor);
         }
         acceptor.pause();
      }

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("Sending disconnect on live connections");
      }

      HashSet<ConnectionEntry> connectionEntries = new HashSet<ConnectionEntry>(connections.values());

      // Now we ensure that no connections will process any more packets after this method is complete
      // then send a disconnect packet
      for (ConnectionEntry entry : connectionEntries)
      {
         RemotingConnection conn = entry.connection;

         if (HornetQServerLogger.LOGGER.isTraceEnabled())
         {
            HornetQServerLogger.LOGGER.trace("Sending connection.disconnection packet to " + conn);
         }

         conn.disconnect(criticalError);
      }

      for (Acceptor acceptor : acceptors.values())
      {
         acceptor.stop();
      }

      acceptors.clear();

      connections.clear();

      if (managementService != null)
      {
         managementService.unregisterAcceptors();
      }

      threadPool.shutdown();

      if (!criticalError)
      {
         boolean ok = threadPool.awaitTermination(10000, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            HornetQServerLogger.LOGGER.timeoutRemotingThreadPool();
         }
      }

      started = false;

   }

   @Override
   public Acceptor getAcceptor(String name)
   {
      return acceptors.get(name);
   }

   public boolean isStarted()
   {
      return started;
   }

   public RemotingConnection removeConnection(final Object remotingConnectionID)
   {
      ConnectionEntry entry = connections.remove(remotingConnectionID);

      if (entry != null)
      {
         return entry.connection;
      }
      else
      {
         HornetQServerLogger.LOGGER.errorRemovingConnection();

         return null;
      }
   }

   public synchronized Set<RemotingConnection> getConnections()
   {
      Set<RemotingConnection> conns = new HashSet<RemotingConnection>(connections.size());

      for (ConnectionEntry entry : connections.values())
      {
         conns.add(entry.connection);
      }

      return conns;
   }

   // ConnectionLifeCycleListener implementation -----------------------------------

   private ProtocolManager getProtocolManager(String protocol)
   {
      return protocolMap.get(protocol);
   }

   public void connectionCreated(final HornetQComponent component, final Connection connection, final String protocol)
   {
      if (server == null)
      {
         throw new IllegalStateException("Unable to create connection, server hasn't finished starting up");
      }

      ProtocolManager pmgr = this.getProtocolManager(protocol.toString());

      if (pmgr == null)
      {
         throw HornetQMessageBundle.BUNDLE.unknownProtocol(protocol);
      }

      ConnectionEntry entry = pmgr.createConnectionEntry((Acceptor) component, connection);

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("Connection created " + connection);
      }

      connections.put(connection.getID(), entry);
   }

   public void connectionDestroyed(final Object connectionID)
   {

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("Connection removed " + connectionID + " from server " + this.server, new Exception("trace"));
      }

      ConnectionEntry conn = connections.get(connectionID);

      if (conn != null)
      {
         // Bit of a hack - find a better way to do this

         List<FailureListener> failureListeners = conn.connection.getFailureListeners();

         boolean empty = true;

         for (FailureListener listener : failureListeners)
         {
            if (listener instanceof ServerSessionImpl)
            {
               empty = false;

               break;
            }
         }

         // We only destroy the connection if the connection has no sessions attached to it
         // Otherwise it means the connection has died without the sessions being closed first
         // so we need to keep them for ttl, in case re-attachment occurs
         if (empty)
         {
            connections.remove(connectionID);

            conn.connection.destroy();
         }
      }
   }

   public void connectionException(final Object connectionID, final HornetQException me)
   {
      // We DO NOT call fail on connection exception, otherwise in event of real connection failure, the
      // connection will be failed, the session will be closed and won't be able to reconnect

      // E.g. if live server fails, then this handler wil be called on backup server for the server
      // side replicating connection.
      // If the connection fail() is called then the sessions on the backup will get closed.

      // Connections should only fail when TTL is exceeded
   }

   public void connectionReadyForWrites(final Object connectionID, final boolean ready)
   {
   }

   @Override
   public void addIncomingInterceptor(final Interceptor interceptor)
   {
      incomingInterceptors.add(interceptor);
   }

   @Override
   public boolean removeIncomingInterceptor(final Interceptor interceptor)
   {
      return incomingInterceptors.remove(interceptor);
   }

   @Override
   public void addOutgoingInterceptor(final Interceptor interceptor)
   {
      outgoingInterceptors.add(interceptor);
   }

   @Override
   public boolean removeOutgoingInterceptor(final Interceptor interceptor)
   {
      return outgoingInterceptors.remove(interceptor);
   }

   private ClusterConnection lookupClusterConnection(TransportConfiguration acceptorConfig)
   {
      String clusterConnectionName = (String) acceptorConfig.getParams().get(org.hornetq.core.remoting.impl.netty.TransportConstants.CLUSTER_CONNECTION);

      ClusterConnection clusterConnection = null;
      if (clusterConnectionName != null)
      {
         clusterConnection = clusterManager.getClusterConnection(clusterConnectionName);
      }

      // if not found we will still use the default name, even if a name was provided
      if (clusterConnection == null)
      {
         clusterConnection = clusterManager.getDefaultConnection(acceptorConfig);
      }

      return clusterConnection;
   }

   // Inner classes -------------------------------------------------

   private final class DelegatingBufferHandler implements BufferHandler
   {
      public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
      {
         ConnectionEntry conn = connections.get(connectionID);

         if (conn != null)
         {
            conn.connection.bufferReceived(connectionID, buffer);
         }
         else
         {
            if (HornetQServerLogger.LOGGER.isTraceEnabled())
            {
               HornetQServerLogger.LOGGER.trace("ConnectionID = " + connectionID + " was already closed, so ignoring packet");
            }
         }
      }
   }

   private final class FailureCheckAndFlushThread extends Thread
   {
      private final long pauseInterval;

      private volatile boolean closed;
      private final CountDownLatch latch = new CountDownLatch(1);

      FailureCheckAndFlushThread(final long pauseInterval)
      {
         super("hornetq-failure-check-thread");

         this.pauseInterval = pauseInterval;
      }

      public void close(final boolean criticalError)
      {
         closed = true;

         latch.countDown();

         if (!criticalError)
         {
            try
            {
               join();
            }
            catch (InterruptedException e)
            {
               throw new HornetQInterruptedException(e);
            }
         }
      }

      @Override
      public void run()
      {
         while (!closed)
         {
            try
            {
               long now = System.currentTimeMillis();

               Set<Object> idsToRemove = new HashSet<Object>();

               for (ConnectionEntry entry : connections.values())
               {
                  RemotingConnection conn = entry.connection;

                  boolean flush = true;

                  if (entry.ttl != -1)
                  {
                     if (!conn.checkDataReceived())
                     {
                        if (now >= entry.lastCheck + entry.ttl)
                        {
                           idsToRemove.add(conn.getID());

                           flush = false;
                        }
                     }
                     else
                     {
                        entry.lastCheck = now;
                     }
                  }

                  if (flush)
                  {
                     conn.flush();
                  }
               }

               for (Object id : idsToRemove)
               {
                  RemotingConnection conn = removeConnection(id);
                  if (conn != null)
                  {
                     conn.fail(HornetQMessageBundle.BUNDLE.clientExited(conn.getRemoteAddress()));
                  }
               }

               if (latch.await(pauseInterval, TimeUnit.MILLISECONDS))
                  return;
            }
            catch (Throwable e)
            {
               HornetQServerLogger.LOGGER.errorOnFailureCheck(e);
            }
         }
      }
   }

   private static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            return ClassloadingUtil.newInstanceFromClassLoader(className);
         }
      });
   }

}
