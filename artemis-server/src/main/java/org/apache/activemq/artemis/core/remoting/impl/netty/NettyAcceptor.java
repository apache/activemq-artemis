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
package org.apache.activemq.artemis.core.remoting.impl.netty;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jctools.maps.NonBlockingHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.protocol.ProtocolHandler;
import org.apache.activemq.artemis.core.remoting.impl.AbstractAcceptor;
import org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport;
import org.apache.activemq.artemis.core.security.ActiveMQPrincipal;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationService;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ServerConnectionLifeCycleListener;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.TypedProperties;
import org.jboss.logging.Logger;

/**
 * A Netty TCP Acceptor that is embedding Netty.
 */
public class NettyAcceptor extends AbstractAcceptor {

   static {
      // Disable default Netty leak detection if the Netty leak detection level system properties are not in use
      if ( System.getProperty("io.netty.leakDetectionLevel") == null && System.getProperty("io.netty.leakDetection.level") == null) {
         ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
      }
   }

   //just for debug
   private final String protocolsString;

   private final String name;

   private final ClusterConnection clusterConnection;

   private Class<? extends ServerChannel> channelClazz;
   private EventLoopGroup eventLoopGroup;

   private volatile ChannelGroup serverChannelGroup;

   private volatile ChannelGroup channelGroup;

   private ServerBootstrap bootstrap;

   private final BufferHandler handler;

   private final ServerConnectionLifeCycleListener listener;

   private final boolean sslEnabled;

   private final boolean useInvm;

   private final ProtocolHandler protocolHandler;

   private final String host;

   private final int port;

   private final String keyStoreProvider;

   // non-final for testing purposes
   private String keyStorePath;

   private final String keyStorePassword;

   private final String trustStoreProvider;

   private final String trustStorePath;

   private final String trustStorePassword;

   private final String enabledCipherSuites;

   private final String enabledProtocols;

   private final boolean needClientAuth;

   private final boolean verifyHost;

   private final boolean tcpNoDelay;

   private final int backlog;

   private final int tcpSendBufferSize;

   private final int tcpReceiveBufferSize;

   private final int nioRemotingThreads;

   private final ConcurrentMap<Object, NettyServerConnection> connections = new NonBlockingHashMap<>();

   private final Map<String, Object> configuration;

   private final ScheduledExecutorService scheduledThreadPool;

   private NotificationService notificationService;

   private boolean paused;

   private BatchFlusher flusher;

   private ScheduledFuture<?> batchFlusherFuture;

   private final long batchDelay;

   private final boolean directDeliver;

   private final boolean httpUpgradeEnabled;

   private final long connectionsAllowed;

   private Map<String, Object> extraConfigs;

   private static final Logger logger = Logger.getLogger(NettyAcceptor.class);

   public NettyAcceptor(final String name,
                        final ClusterConnection clusterConnection,
                        final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final ServerConnectionLifeCycleListener listener,
                        final ScheduledExecutorService scheduledThreadPool,
                        final Map<String, ProtocolManager> protocolMap) {
      super(protocolMap);

      this.name = name;

      this.clusterConnection = clusterConnection;

      this.configuration = configuration;

      this.handler = handler;

      this.listener = listener;

      sslEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME, TransportConstants.DEFAULT_SSL_ENABLED, configuration);

      nioRemotingThreads = ConfigurationHelper.getIntProperty(TransportConstants.NIO_REMOTING_THREADS_PROPNAME, -1, configuration);
      backlog = ConfigurationHelper.getIntProperty(TransportConstants.BACKLOG_PROP_NAME, -1, configuration);
      useInvm = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_INVM_PROP_NAME, TransportConstants.DEFAULT_USE_INVM, configuration);

      this.protocolHandler = new ProtocolHandler(protocolMap, this, configuration, scheduledThreadPool);

      this.protocolsString = getProtocols(protocolMap);

      host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, configuration);
      port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, configuration);
      if (sslEnabled) {
         keyStoreProvider = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PROVIDER, configuration);

         keyStorePath = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PATH, configuration);

         keyStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PASSWORD, configuration, ActiveMQDefaultConfiguration.getPropMaskPassword(), ActiveMQDefaultConfiguration.getPropMaskPassword());

         trustStoreProvider = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER, configuration);

         trustStorePath = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PATH, configuration);

         trustStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD, configuration, ActiveMQDefaultConfiguration.getPropMaskPassword(), ActiveMQDefaultConfiguration.getPropMaskPassword());

         enabledCipherSuites = ConfigurationHelper.getStringProperty(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, TransportConstants.DEFAULT_ENABLED_CIPHER_SUITES, configuration);

         enabledProtocols = ConfigurationHelper.getStringProperty(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, TransportConstants.DEFAULT_ENABLED_PROTOCOLS, configuration);

         needClientAuth = ConfigurationHelper.getBooleanProperty(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, TransportConstants.DEFAULT_NEED_CLIENT_AUTH, configuration);

         verifyHost = ConfigurationHelper.getBooleanProperty(TransportConstants.VERIFY_HOST_PROP_NAME, TransportConstants.DEFAULT_VERIFY_HOST, configuration);
      } else {
         keyStoreProvider = TransportConstants.DEFAULT_KEYSTORE_PROVIDER;
         keyStorePath = TransportConstants.DEFAULT_KEYSTORE_PATH;
         keyStorePassword = TransportConstants.DEFAULT_KEYSTORE_PASSWORD;
         trustStoreProvider = TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER;
         trustStorePath = TransportConstants.DEFAULT_TRUSTSTORE_PATH;
         trustStorePassword = TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD;
         enabledCipherSuites = TransportConstants.DEFAULT_ENABLED_CIPHER_SUITES;
         enabledProtocols = TransportConstants.DEFAULT_ENABLED_PROTOCOLS;
         needClientAuth = TransportConstants.DEFAULT_NEED_CLIENT_AUTH;
         verifyHost = TransportConstants.DEFAULT_VERIFY_HOST;
      }

      tcpNoDelay = ConfigurationHelper.getBooleanProperty(TransportConstants.TCP_NODELAY_PROPNAME, TransportConstants.DEFAULT_TCP_NODELAY, configuration);
      tcpSendBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_SENDBUFFER_SIZE, configuration);
      tcpReceiveBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_RECEIVEBUFFER_SIZE, configuration);

      this.scheduledThreadPool = scheduledThreadPool;

      batchDelay = ConfigurationHelper.getLongProperty(TransportConstants.BATCH_DELAY, TransportConstants.DEFAULT_BATCH_DELAY, configuration);

      directDeliver = ConfigurationHelper.getBooleanProperty(TransportConstants.DIRECT_DELIVER, TransportConstants.DEFAULT_DIRECT_DELIVER, configuration);

      httpUpgradeEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME, TransportConstants.DEFAULT_HTTP_UPGRADE_ENABLED, configuration);

      connectionsAllowed = ConfigurationHelper.getLongProperty(TransportConstants.CONNECTIONS_ALLOWED, TransportConstants.DEFAULT_CONNECTIONS_ALLOWED, configuration);
   }

   @Override
   public synchronized void start() throws Exception {
      if (channelClazz != null) {
         // Already started
         return;
      }

      if (useInvm) {
         channelClazz = LocalServerChannel.class;
         eventLoopGroup = new LocalEventLoopGroup();
      } else {
         int threadsToUse;

         if (nioRemotingThreads == -1) {
            // Default to number of cores * 3

            threadsToUse = Runtime.getRuntime().availableProcessors() * 3;
         } else {
            threadsToUse = this.nioRemotingThreads;
         }
         channelClazz = NioServerSocketChannel.class;
         eventLoopGroup = new NioEventLoopGroup(threadsToUse, AccessController.doPrivileged(new PrivilegedAction<ActiveMQThreadFactory>() {
            @Override
            public ActiveMQThreadFactory run() {
               return new ActiveMQThreadFactory("activemq-netty-threads", true, ClientSessionFactoryImpl.class.getClassLoader());
            }
         }));
      }

      bootstrap = new ServerBootstrap();
      bootstrap.group(eventLoopGroup);
      bootstrap.channel(channelClazz);

      ChannelInitializer<Channel> factory = new ChannelInitializer<Channel>() {
         @Override
         public void initChannel(Channel channel) throws Exception {
            ChannelPipeline pipeline = channel.pipeline();
            if (sslEnabled) {
               pipeline.addLast("ssl", getSslHandler());
               pipeline.addLast("sslHandshakeExceptionHandler", new SslHandshakeExceptionHandler());
            }
            pipeline.addLast(protocolHandler.getProtocolDecoder());
         }
      };
      bootstrap.childHandler(factory);

      // Bind
      bootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
      if (tcpReceiveBufferSize != -1) {
         bootstrap.childOption(ChannelOption.SO_RCVBUF, tcpReceiveBufferSize);
      }
      if (tcpSendBufferSize != -1) {
         bootstrap.childOption(ChannelOption.SO_SNDBUF, tcpSendBufferSize);
      }
      if (backlog != -1) {
         bootstrap.option(ChannelOption.SO_BACKLOG, backlog);
      }
      bootstrap.option(ChannelOption.SO_REUSEADDR, true);
      bootstrap.childOption(ChannelOption.SO_REUSEADDR, true);
      bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
      bootstrap.childOption(ChannelOption.ALLOCATOR, PartialPooledByteBufAllocator.INSTANCE);
      channelGroup = new DefaultChannelGroup("activemq-accepted-channels", GlobalEventExecutor.INSTANCE);

      serverChannelGroup = new DefaultChannelGroup("activemq-acceptor-channels", GlobalEventExecutor.INSTANCE);

      if (httpUpgradeEnabled) {
         // the channel will be bound by the Web container and hand over after the HTTP Upgrade
         // handshake is successful
      } else {
         startServerChannels();

         paused = false;

         if (notificationService != null) {
            TypedProperties props = new TypedProperties();
            props.putSimpleStringProperty(new SimpleString("factory"), new SimpleString(NettyAcceptorFactory.class.getName()));
            props.putSimpleStringProperty(new SimpleString("host"), new SimpleString(host));
            props.putIntProperty(new SimpleString("port"), port);
            Notification notification = new Notification(null, CoreNotificationType.ACCEPTOR_STARTED, props);
            notificationService.sendNotification(notification);
         }

         if (batchDelay > 0) {
            flusher = new BatchFlusher();

            batchFlusherFuture = scheduledThreadPool.scheduleWithFixedDelay(flusher, batchDelay, batchDelay, TimeUnit.MILLISECONDS);
         }

         ActiveMQServerLogger.LOGGER.startedAcceptor(host, port, protocolsString);
      }
   }

   @Override
   public String getName() {
      return name;
   }

   // only for testing purposes
   public void setKeyStorePath(String keyStorePath) {
      this.keyStorePath = keyStorePath;
   }

   /**
    * Transfers the Netty channel that has been created outside of this NettyAcceptor
    * to control it and configure it according to this NettyAcceptor setting.
    *
    * @param channel A Netty channel created outside this NettyAcceptor.
    */
   public void transfer(Channel channel) {
      if (paused || eventLoopGroup == null) {
         throw ActiveMQMessageBundle.BUNDLE.acceptorUnavailable();
      }
      channel.pipeline().addLast(protocolHandler.getProtocolDecoder());
   }

   @Override
   public void reload() {
      serverChannelGroup.disconnect();
      serverChannelGroup.clear();
      startServerChannels();
   }

   public synchronized SslHandler getSslHandler() throws Exception {
      final SSLContext context;
      try {
         if (keyStorePath == null && TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER.equals(keyStoreProvider))
            throw new IllegalArgumentException("If \"" + TransportConstants.SSL_ENABLED_PROP_NAME +
                                                  "\" is true then \"" + TransportConstants.KEYSTORE_PATH_PROP_NAME + "\" must be non-null " +
                                                  "unless an alternative \"" + TransportConstants.KEYSTORE_PROVIDER_PROP_NAME + "\" has been specified.");
         context = SSLSupport.createContext(keyStoreProvider, keyStorePath, keyStorePassword, trustStoreProvider, trustStorePath, trustStorePassword);
      } catch (Exception e) {
         IllegalStateException ise = new IllegalStateException("Unable to create NettyAcceptor for " + host + ":" + port);
         ise.initCause(e);
         throw ise;
      }
      SSLEngine engine;
      if (verifyHost) {
         engine = context.createSSLEngine(host, port);
      } else {
         engine = context.createSSLEngine();
      }

      engine.setUseClientMode(false);

      if (needClientAuth)
         engine.setNeedClientAuth(true);

      // setting the enabled cipher suites resets the enabled protocols so we need
      // to save the enabled protocols so that after the customer cipher suite is enabled
      // we can reset the enabled protocols if a customer protocol isn't specified
      String[] originalProtocols = engine.getEnabledProtocols();

      if (enabledCipherSuites != null) {
         try {
            engine.setEnabledCipherSuites(SSLSupport.parseCommaSeparatedListIntoArray(enabledCipherSuites));
         } catch (IllegalArgumentException e) {
            ActiveMQServerLogger.LOGGER.invalidCipherSuite(SSLSupport.parseArrayIntoCommandSeparatedList(engine.getSupportedCipherSuites()));
            throw e;
         }
      }

      if (enabledProtocols != null) {
         try {
            engine.setEnabledProtocols(SSLSupport.parseCommaSeparatedListIntoArray(enabledProtocols));
         } catch (IllegalArgumentException e) {
            ActiveMQServerLogger.LOGGER.invalidProtocol(SSLSupport.parseArrayIntoCommandSeparatedList(engine.getSupportedProtocols()));
            throw e;
         }
      } else {
         engine.setEnabledProtocols(originalProtocols);
      }

      // Strip "SSLv3" from the current enabled protocols to address the POODLE exploit.
      // This recommendation came from http://www.oracle.com/technetwork/java/javase/documentation/cve-2014-3566-2342133.html
      String[] protocols = engine.getEnabledProtocols();
      Set<String> set = new HashSet<>();
      for (String s : protocols) {
         if (s.equalsIgnoreCase("SSLv3") || s.equals("SSLv2Hello")) {
            ActiveMQServerLogger.LOGGER.disallowedProtocol(s, name);
            continue;
         }
         set.add(s);
      }

      engine.setEnabledProtocols(set.toArray(new String[set.size()]));

      if (verifyHost) {
         SSLParameters sslParameters = engine.getSSLParameters();
         sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
         engine.setSSLParameters(sslParameters);
      }

      return new SslHandler(engine);
   }

   private void startServerChannels() {
      String[] hosts = TransportConfiguration.splitHosts(host);
      for (String h : hosts) {
         SocketAddress address;
         if (useInvm) {
            address = new LocalAddress(h);
         } else {
            address = new InetSocketAddress(h, port);
         }
         Channel serverChannel = bootstrap.bind(address).syncUninterruptibly().channel();
         serverChannelGroup.add(serverChannel);
      }
   }

   @Override
   public Map<String, Object> getConfiguration() {
      return this.configuration;
   }

   @Override
   public synchronized void stop() {
      if (channelClazz == null) {
         return;
      }

      if (protocolHandler != null) {
         protocolHandler.close();
      }

      if (batchFlusherFuture != null) {
         batchFlusherFuture.cancel(false);

         flusher.cancel();

         flusher = null;

         batchFlusherFuture = null;
      }

      // serverChannelGroup has been unbound in pause()
      if (serverChannelGroup != null) {
         serverChannelGroup.close().awaitUninterruptibly();
      }

      if (channelGroup != null) {
         ChannelGroupFuture future = channelGroup.close().awaitUninterruptibly();

         if (!future.isSuccess()) {
            ActiveMQServerLogger.LOGGER.nettyChannelGroupError();
            for (Channel channel : future.group()) {
               if (channel.isActive()) {
                  ActiveMQServerLogger.LOGGER.nettyChannelStillOpen(channel, channel.remoteAddress());
               }
            }
         }
      }

      // Shutdown the EventLoopGroup if no new task was added for 100ms or if
      // 3000ms elapsed.
      eventLoopGroup.shutdownGracefully(100, 3000, TimeUnit.MILLISECONDS);
      eventLoopGroup = null;

      channelClazz = null;

      for (Connection connection : connections.values()) {
         listener.connectionDestroyed(connection.getID());
      }

      connections.clear();

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("factory"), new SimpleString(NettyAcceptorFactory.class.getName()));
         props.putSimpleStringProperty(new SimpleString("host"), new SimpleString(host));
         props.putIntProperty(new SimpleString("port"), port);
         Notification notification = new Notification(null, CoreNotificationType.ACCEPTOR_STOPPED, props);
         try {
            notificationService.sendNotification(notification);
         } catch (Exception e) {
            logger.warn("failed to send notification", e.getMessage(), e);
         }
      }

      paused = false;
   }

   @Override
   public boolean isStarted() {
      return channelClazz != null;
   }

   @Override
   public synchronized void pause() {
      if (paused) {
         return;
      }

      if (channelClazz == null) {
         return;
      }

      // We *pause* the acceptor so no new connections are made
      if (serverChannelGroup != null) {
         ChannelGroupFuture future = serverChannelGroup.close().awaitUninterruptibly();
         if (!future.isSuccess()) {
            ActiveMQServerLogger.LOGGER.nettyChannelGroupBindError();
            for (Channel channel : future.group()) {
               if (channel.isActive()) {
                  ActiveMQServerLogger.LOGGER.nettyChannelStillBound(channel, channel.remoteAddress());
               }
            }
         }
      }
      paused = true;
   }

   @Override
   public void setNotificationService(final NotificationService notificationService) {
      this.notificationService = notificationService;
   }

   /**
    * not allowed
    *
    * @param defaultActiveMQPrincipal
    */
   @Override
   public void setDefaultActiveMQPrincipal(ActiveMQPrincipal defaultActiveMQPrincipal) {
      throw new IllegalStateException("unsecure connections not allowed");
   }

   /**
    * only InVM acceptors should allow this
    *
    * @return
    */
   @Override
   public boolean isUnsecurable() {
      return false;
   }

   @Override
   public ClusterConnection getClusterConnection() {
      return clusterConnection;
   }

   public ConnectionCreator createConnectionCreator() {
      return new ActiveMQServerChannelHandler(channelGroup, handler, new Listener());
   }

   private static String getProtocols(Map<String, ProtocolManager> protocolManager) {
      StringBuilder sb = new StringBuilder();
      if (protocolManager != null) {
         Set<String> strings = protocolManager.keySet();
         for (String string : strings) {
            if (sb.length() > 0) {
               sb.append(",");
            }
            sb.append(string);
         }
      }
      return sb.toString();
   }

   // Inner classes -----------------------------------------------------------------------------

   private final class ActiveMQServerChannelHandler extends ActiveMQChannelHandler implements ConnectionCreator {

      ActiveMQServerChannelHandler(final ChannelGroup group,
                                   final BufferHandler handler,
                                   final ServerConnectionLifeCycleListener listener) {
         super(group, handler, listener);
      }

      @Override
      public NettyServerConnection createConnection(final ChannelHandlerContext ctx,
                                                    String protocol,
                                                    boolean httpEnabled) throws Exception {
         if (connectionsAllowed == -1 || connections.size() < connectionsAllowed) {
            super.channelActive(ctx);
            Listener connectionListener = new Listener();

            NettyServerConnection nc = new NettyServerConnection(configuration, ctx.channel(), connectionListener, !httpEnabled && batchDelay > 0, directDeliver);

            connectionListener.connectionCreated(NettyAcceptor.this, nc, protocolHandler.getProtocol(protocol));

            SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
            if (sslHandler != null) {
               sslHandler.handshakeFuture().addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Channel>>() {
                  @Override
                  public void operationComplete(final io.netty.util.concurrent.Future<Channel> future) throws Exception {
                     if (future.isSuccess()) {
                        active = true;
                     } else {
                        future.getNow().close();
                     }
                  }
               });
            } else {
               active = true;
            }
            return nc;
         } else {
            ActiveMQServerLogger.LOGGER.connectionLimitReached(connectionsAllowed, ctx.channel().remoteAddress().toString());
            ctx.channel().close();
            return null;
         }
      }
   }

   private class Listener implements ServerConnectionLifeCycleListener {

      @Override
      public void connectionCreated(final ActiveMQComponent component,
                                    final Connection connection,
                                    final ProtocolManager protocol) {
         if (connections.putIfAbsent(connection.getID(), (NettyServerConnection) connection) != null) {
            throw ActiveMQMessageBundle.BUNDLE.connectionExists(connection.getID());
         }

         listener.connectionCreated(component, connection, protocol);
      }

      @Override
      public void connectionDestroyed(final Object connectionID) {
         if (connections.remove(connectionID) != null) {
            listener.connectionDestroyed(connectionID);
         }
      }

      @Override
      public void connectionException(final Object connectionID, final ActiveMQException me) {
         // Execute on different thread to avoid deadlocks
         new Thread() {
            @Override
            public void run() {
               listener.connectionException(connectionID, me);
            }
         }.start();

      }

      @Override
      public void connectionReadyForWrites(final Object connectionID, boolean ready) {
         NettyServerConnection conn = connections.get(connectionID);

         if (conn != null) {
            conn.fireReady(ready);
         }

         listener.connectionReadyForWrites(connectionID, ready);
      }
   }

   private class BatchFlusher implements Runnable {

      private boolean cancelled;

      @Override
      public synchronized void run() {
         if (!cancelled) {
            for (Connection connection : connections.values()) {
               connection.checkFlushBatchBuffer();
            }
         }
      }

      public synchronized void cancel() {
         cancelled = true;
      }
   }

   /**
    * Deal with SSL handshake exceptions which otherwise would not be handled and would result in a lengthy stack-trace
    * in the log.
    */
   private class SslHandshakeExceptionHandler implements ChannelHandler {

      @Override
      public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

      }

      @Override
      public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {

      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
         if (cause.getMessage() != null && cause.getMessage().startsWith(SSLHandshakeException.class.getName())) {
            Throwable rootCause = getRootCause(cause);
            String errorMessage = rootCause.getClass().getName() + ": " + rootCause.getMessage();

            ActiveMQServerLogger.LOGGER.sslHandshakeFailed(ctx.channel().remoteAddress().toString(), errorMessage);

            if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
               ActiveMQServerLogger.LOGGER.debug("SSL handshake failed", cause);
            }
         }
      }

      private Throwable getRootCause(Throwable throwable) {
         List<Throwable> list = new ArrayList<>();
         while (throwable != null && list.contains(throwable) == false) {
            list.add(throwable);
            throwable = throwable.getCause();
         }
         return (list.size() < 2 ? throwable : list.get(list.size() - 1));
      }
   }
}
