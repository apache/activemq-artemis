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
import java.io.IOException;
import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.Cookie;
import io.netty.handler.codec.http.CookieDecoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ClientCookieEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQClientProtocolManager;
import org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.spi.core.remoting.AbstractConnector;
import org.apache.activemq.artemis.spi.core.remoting.BaseConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;
import org.apache.activemq.artemis.spi.core.remoting.ClientConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.FutureLatch;

import static org.apache.activemq.artemis.utils.Base64.encodeBytes;

public class NettyConnector extends AbstractConnector {

   // Constants -----------------------------------------------------
   public static final String JAVAX_KEYSTORE_PATH_PROP_NAME = "javax.net.ssl.keyStore";
   public static final String JAVAX_KEYSTORE_PASSWORD_PROP_NAME = "javax.net.ssl.keyStorePassword";
   public static final String JAVAX_TRUSTSTORE_PATH_PROP_NAME = "javax.net.ssl.trustStore";
   public static final String JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME = "javax.net.ssl.trustStorePassword";
   public static final String ACTIVEMQ_KEYSTORE_PROVIDER_PROP_NAME = "org.apache.activemq.ssl.keyStoreProvider";
   public static final String ACTIVEMQ_KEYSTORE_PATH_PROP_NAME = "org.apache.activemq.ssl.keyStore";
   public static final String ACTIVEMQ_KEYSTORE_PASSWORD_PROP_NAME = "org.apache.activemq.ssl.keyStorePassword";
   public static final String ACTIVEMQ_TRUSTSTORE_PROVIDER_PROP_NAME = "org.apache.activemq.ssl.trustStoreProvider";
   public static final String ACTIVEMQ_TRUSTSTORE_PATH_PROP_NAME = "org.apache.activemq.ssl.trustStore";
   public static final String ACTIVEMQ_TRUSTSTORE_PASSWORD_PROP_NAME = "org.apache.activemq.ssl.trustStorePassword";

   // Constants for HTTP upgrade
   // These constants are exposed publicly as they are used on the server-side to fetch
   // headers from the HTTP request, compute some values and fill the HTTP response
   public static final String MAGIC_NUMBER = "CF70DEB8-70F9-4FBA-8B4F-DFC3E723B4CD";
   public static final String SEC_ACTIVEMQ_REMOTING_KEY = "Sec-ActiveMQRemoting-Key";
   public static final String SEC_ACTIVEMQ_REMOTING_ACCEPT = "Sec-ActiveMQRemoting-Accept";
   public static final String ACTIVEMQ_REMOTING = "activemq-remoting";

   private static final AttributeKey<String> REMOTING_KEY = AttributeKey.valueOf(SEC_ACTIVEMQ_REMOTING_KEY);

   // Default Configuration
   public static final Map<String, Object> DEFAULT_CONFIG;

   static {
      // Disable resource leak detection for performance reasons by default
      ResourceLeakDetector.setEnabled(false);

      // Set default Configuration
      Map<String, Object> config = new HashMap<>();
      config.put(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST);
      config.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT);
      DEFAULT_CONFIG = Collections.unmodifiableMap(config);
   }

   // Attributes ----------------------------------------------------

   private Class<? extends Channel> channelClazz;

   private Bootstrap bootstrap;

   private ChannelGroup channelGroup;

   private final BufferHandler handler;

   private final BaseConnectionLifeCycleListener listener;

   private boolean sslEnabled = TransportConstants.DEFAULT_SSL_ENABLED;

   private boolean httpEnabled;

   private long httpMaxClientIdleTime;

   private long httpClientIdleScanPeriod;

   private boolean httpRequiresSessionId;

   // if true, after the connection, the connector will send
   // a HTTP GET request (+ Upgrade: activemq-remoting) that
   // will be handled by the server's http server.
   private boolean httpUpgradeEnabled;

   private boolean useServlet;

   private String host;

   private int port;

   private String localAddress;

   private int localPort;

   private String keyStoreProvider;

   private String keyStorePath;

   private String keyStorePassword;

   private String trustStoreProvider;

   private String trustStorePath;

   private String trustStorePassword;

   private String enabledCipherSuites;

   private String enabledProtocols;

   private boolean tcpNoDelay;

   private int tcpSendBufferSize;

   private int tcpReceiveBufferSize;

   private long batchDelay;

   private ConcurrentMap<Object, Connection> connections = new ConcurrentHashMap<>();

   private String servletPath;

   private int nioRemotingThreads;

   private boolean useNioGlobalWorkerPool;

   private ScheduledExecutorService scheduledThreadPool;

   private Executor closeExecutor;

   private BatchFlusher flusher;

   private ScheduledFuture<?> batchFlusherFuture;

   private EventLoopGroup group;

   private int connectTimeoutMillis;

   private final ClientProtocolManager protocolManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   public NettyConnector(final Map<String, Object> configuration,
                         final BufferHandler handler,
                         final BaseConnectionLifeCycleListener listener,
                         final Executor closeExecutor,
                         final Executor threadPool,
                         final ScheduledExecutorService scheduledThreadPool) {
      this(configuration, handler, listener, closeExecutor, threadPool, scheduledThreadPool, new ActiveMQClientProtocolManager());
   }

   public NettyConnector(final Map<String, Object> configuration,
                         final BufferHandler handler,
                         final BaseConnectionLifeCycleListener listener,
                         final Executor closeExecutor,
                         final Executor threadPool,
                         final ScheduledExecutorService scheduledThreadPool,
                         final ClientProtocolManager protocolManager) {
      super(configuration);

      this.protocolManager = protocolManager;

      if (listener == null) {
         throw ActiveMQClientMessageBundle.BUNDLE.nullListener();
      }

      if (handler == null) {
         throw ActiveMQClientMessageBundle.BUNDLE.nullHandler();
      }

      this.listener = listener;

      this.handler = handler;

      sslEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.SSL_ENABLED_PROP_NAME, TransportConstants.DEFAULT_SSL_ENABLED, configuration);
      httpEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_ENABLED_PROP_NAME, TransportConstants.DEFAULT_HTTP_ENABLED, configuration);
      servletPath = ConfigurationHelper.getStringProperty(TransportConstants.SERVLET_PATH, TransportConstants.DEFAULT_SERVLET_PATH, configuration);
      if (httpEnabled) {
         httpMaxClientIdleTime = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME, TransportConstants.DEFAULT_HTTP_CLIENT_IDLE_TIME, configuration);
         httpClientIdleScanPeriod = ConfigurationHelper.getLongProperty(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD, TransportConstants.DEFAULT_HTTP_CLIENT_SCAN_PERIOD, configuration);
         httpRequiresSessionId = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_REQUIRES_SESSION_ID, TransportConstants.DEFAULT_HTTP_REQUIRES_SESSION_ID, configuration);
      }
      else {
         httpMaxClientIdleTime = 0;
         httpClientIdleScanPeriod = -1;
         httpRequiresSessionId = false;
      }

      httpUpgradeEnabled = ConfigurationHelper.getBooleanProperty(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME, TransportConstants.DEFAULT_HTTP_UPGRADE_ENABLED, configuration);

      nioRemotingThreads = ConfigurationHelper.getIntProperty(TransportConstants.NIO_REMOTING_THREADS_PROPNAME, -1, configuration);

      useNioGlobalWorkerPool = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_NIO_GLOBAL_WORKER_POOL_PROP_NAME, TransportConstants.DEFAULT_USE_NIO_GLOBAL_WORKER_POOL, configuration);

      useServlet = ConfigurationHelper.getBooleanProperty(TransportConstants.USE_SERVLET_PROP_NAME, TransportConstants.DEFAULT_USE_SERVLET, configuration);
      host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, configuration);
      port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, configuration);
      localAddress = ConfigurationHelper.getStringProperty(TransportConstants.LOCAL_ADDRESS_PROP_NAME, TransportConstants.DEFAULT_LOCAL_ADDRESS, configuration);

      localPort = ConfigurationHelper.getIntProperty(TransportConstants.LOCAL_PORT_PROP_NAME, TransportConstants.DEFAULT_LOCAL_PORT, configuration);
      if (sslEnabled) {
         keyStoreProvider = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PROVIDER, configuration);

         keyStorePath = ConfigurationHelper.getStringProperty(TransportConstants.KEYSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PATH, configuration);

         keyStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PASSWORD, configuration, ActiveMQDefaultConfiguration.getPropMaskPassword(), ActiveMQDefaultConfiguration.getPropMaskPassword());

         trustStoreProvider = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER, configuration);

         trustStorePath = ConfigurationHelper.getStringProperty(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PATH, configuration);

         trustStorePassword = ConfigurationHelper.getPasswordProperty(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD, configuration, ActiveMQDefaultConfiguration.getPropMaskPassword(), ActiveMQDefaultConfiguration.getPropMaskPassword());

         enabledCipherSuites = ConfigurationHelper.getStringProperty(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, TransportConstants.DEFAULT_ENABLED_CIPHER_SUITES, configuration);

         enabledProtocols = ConfigurationHelper.getStringProperty(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, TransportConstants.DEFAULT_ENABLED_PROTOCOLS, configuration);
      }
      else {
         keyStoreProvider = TransportConstants.DEFAULT_KEYSTORE_PROVIDER;
         keyStorePath = TransportConstants.DEFAULT_KEYSTORE_PATH;
         keyStorePassword = TransportConstants.DEFAULT_KEYSTORE_PASSWORD;
         trustStoreProvider = TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER;
         trustStorePath = TransportConstants.DEFAULT_TRUSTSTORE_PATH;
         trustStorePassword = TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD;
         enabledCipherSuites = TransportConstants.DEFAULT_ENABLED_CIPHER_SUITES;
         enabledProtocols = TransportConstants.DEFAULT_ENABLED_PROTOCOLS;
      }

      tcpNoDelay = ConfigurationHelper.getBooleanProperty(TransportConstants.TCP_NODELAY_PROPNAME, TransportConstants.DEFAULT_TCP_NODELAY, configuration);
      tcpSendBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_SENDBUFFER_SIZE, configuration);
      tcpReceiveBufferSize = ConfigurationHelper.getIntProperty(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME, TransportConstants.DEFAULT_TCP_RECEIVEBUFFER_SIZE, configuration);

      batchDelay = ConfigurationHelper.getLongProperty(TransportConstants.BATCH_DELAY, TransportConstants.DEFAULT_BATCH_DELAY, configuration);

      connectTimeoutMillis = ConfigurationHelper.getIntProperty(TransportConstants.NETTY_CONNECT_TIMEOUT, TransportConstants.DEFAULT_NETTY_CONNECT_TIMEOUT, configuration);
      this.closeExecutor = closeExecutor;
      this.scheduledThreadPool = scheduledThreadPool;
   }

   @Override
   public String toString() {
      return "NettyConnector [host=" + host +
         ", port=" +
         port +
         ", httpEnabled=" +
         httpEnabled +
         ", httpUpgradeEnabled=" +
         httpUpgradeEnabled +
         ", useServlet=" +
         useServlet +
         ", servletPath=" +
         servletPath +
         ", sslEnabled=" +
         sslEnabled +
         ", useNio=" +
         true +
         "]";
   }

   @Override
   public synchronized void start() {
      if (channelClazz != null) {
         return;
      }

      int threadsToUse;

      if (nioRemotingThreads == -1) {
         // Default to number of cores * 3

         threadsToUse = Runtime.getRuntime().availableProcessors() * 3;
      }
      else {
         threadsToUse = this.nioRemotingThreads;
      }

      if (useNioGlobalWorkerPool) {
         channelClazz = NioSocketChannel.class;
         group = SharedNioEventLoopGroup.getInstance(threadsToUse);
      }
      else {
         channelClazz = NioSocketChannel.class;
         group = new NioEventLoopGroup(threadsToUse);
      }
      // if we are a servlet wrap the socketChannelFactory

      bootstrap = new Bootstrap();
      bootstrap.channel(channelClazz);
      bootstrap.group(group);

      bootstrap.option(ChannelOption.TCP_NODELAY, tcpNoDelay);

      if (connectTimeoutMillis != -1) {
         bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis);
      }
      if (tcpReceiveBufferSize != -1) {
         bootstrap.option(ChannelOption.SO_RCVBUF, tcpReceiveBufferSize);
      }
      if (tcpSendBufferSize != -1) {
         bootstrap.option(ChannelOption.SO_SNDBUF, tcpSendBufferSize);
      }
      bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
      bootstrap.option(ChannelOption.SO_REUSEADDR, true);
      bootstrap.option(ChannelOption.ALLOCATOR, PartialPooledByteBufAllocator.INSTANCE);
      channelGroup = new DefaultChannelGroup("activemq-connector", GlobalEventExecutor.INSTANCE);

      final SSLContext context;
      if (sslEnabled) {
         try {
            // HORNETQ-680 - override the server-side config if client-side system properties are set
            String realKeyStorePath = keyStorePath;
            String realKeyStoreProvider = keyStoreProvider;
            String realKeyStorePassword = keyStorePassword;
            if (System.getProperty(JAVAX_KEYSTORE_PATH_PROP_NAME) != null) {
               realKeyStorePath = System.getProperty(JAVAX_KEYSTORE_PATH_PROP_NAME);
            }
            if (System.getProperty(JAVAX_KEYSTORE_PASSWORD_PROP_NAME) != null) {
               realKeyStorePassword = System.getProperty(JAVAX_KEYSTORE_PASSWORD_PROP_NAME);
            }

            if (System.getProperty(ACTIVEMQ_KEYSTORE_PROVIDER_PROP_NAME) != null) {
               realKeyStoreProvider = System.getProperty(ACTIVEMQ_KEYSTORE_PROVIDER_PROP_NAME);
            }
            if (System.getProperty(ACTIVEMQ_KEYSTORE_PATH_PROP_NAME) != null) {
               realKeyStorePath = System.getProperty(ACTIVEMQ_KEYSTORE_PATH_PROP_NAME);
            }
            if (System.getProperty(ACTIVEMQ_KEYSTORE_PASSWORD_PROP_NAME) != null) {
               realKeyStorePassword = System.getProperty(ACTIVEMQ_KEYSTORE_PASSWORD_PROP_NAME);
            }

            String realTrustStorePath = trustStorePath;
            String realTrustStoreProvider = trustStoreProvider;
            String realTrustStorePassword = trustStorePassword;
            if (System.getProperty(JAVAX_TRUSTSTORE_PATH_PROP_NAME) != null) {
               realTrustStorePath = System.getProperty(JAVAX_TRUSTSTORE_PATH_PROP_NAME);
            }
            if (System.getProperty(JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME) != null) {
               realTrustStorePassword = System.getProperty(JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME);
            }

            if (System.getProperty(ACTIVEMQ_TRUSTSTORE_PROVIDER_PROP_NAME) != null) {
               realTrustStoreProvider = System.getProperty(ACTIVEMQ_TRUSTSTORE_PROVIDER_PROP_NAME);
            }
            if (System.getProperty(ACTIVEMQ_TRUSTSTORE_PATH_PROP_NAME) != null) {
               realTrustStorePath = System.getProperty(ACTIVEMQ_TRUSTSTORE_PATH_PROP_NAME);
            }
            if (System.getProperty(ACTIVEMQ_TRUSTSTORE_PASSWORD_PROP_NAME) != null) {
               realTrustStorePassword = System.getProperty(ACTIVEMQ_TRUSTSTORE_PASSWORD_PROP_NAME);
            }
            context = SSLSupport.createContext(realKeyStoreProvider, realKeyStorePath, realKeyStorePassword, realTrustStoreProvider, realTrustStorePath, realTrustStorePassword);
         }
         catch (Exception e) {
            close();
            IllegalStateException ise = new IllegalStateException("Unable to create NettyConnector for " + host + ":" + port);
            ise.initCause(e);
            throw ise;
         }
      }
      else {
         context = null; // Unused
      }

      if (context != null && useServlet) {
         // TODO: Fix me
         //bootstrap.setOption("sslContext", context);
      }

      bootstrap.handler(new ChannelInitializer<Channel>() {
         @Override
         public void initChannel(Channel channel) throws Exception {
            final ChannelPipeline pipeline = channel.pipeline();
            if (sslEnabled && !useServlet) {
               SSLEngine engine = context.createSSLEngine();

               engine.setUseClientMode(true);

               engine.setWantClientAuth(true);

               // setting the enabled cipher suites resets the enabled protocols so we need
               // to save the enabled protocols so that after the customer cipher suite is enabled
               // we can reset the enabled protocols if a customer protocol isn't specified
               String[] originalProtocols = engine.getEnabledProtocols();

               if (enabledCipherSuites != null) {
                  try {
                     engine.setEnabledCipherSuites(SSLSupport.parseCommaSeparatedListIntoArray(enabledCipherSuites));
                  }
                  catch (IllegalArgumentException e) {
                     ActiveMQClientLogger.LOGGER.invalidCipherSuite(SSLSupport.parseArrayIntoCommandSeparatedList(engine.getSupportedCipherSuites()));
                     throw e;
                  }
               }

               if (enabledProtocols != null) {
                  try {
                     engine.setEnabledProtocols(SSLSupport.parseCommaSeparatedListIntoArray(enabledProtocols));
                  }
                  catch (IllegalArgumentException e) {
                     ActiveMQClientLogger.LOGGER.invalidProtocol(SSLSupport.parseArrayIntoCommandSeparatedList(engine.getSupportedProtocols()));
                     throw e;
                  }
               }
               else {
                  engine.setEnabledProtocols(originalProtocols);
               }

               SslHandler handler = new SslHandler(engine);

               pipeline.addLast(handler);
            }

            if (httpEnabled) {
               pipeline.addLast(new HttpRequestEncoder());

               pipeline.addLast(new HttpResponseDecoder());

               pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));

               pipeline.addLast(new HttpHandler());
            }

            if (httpUpgradeEnabled) {
               // prepare to handle a HTTP 101 response to upgrade the protocol.
               final HttpClientCodec httpClientCodec = new HttpClientCodec();
               pipeline.addLast(httpClientCodec);
               pipeline.addLast("http-upgrade", new HttpUpgradeHandler(pipeline, httpClientCodec));
            }

            protocolManager.addChannelHandlers(pipeline);

            pipeline.addLast(new ActiveMQClientChannelHandler(channelGroup, handler, new Listener()));
         }
      });

      if (batchDelay > 0) {
         flusher = new BatchFlusher();

         batchFlusherFuture = scheduledThreadPool.scheduleWithFixedDelay(flusher, batchDelay, batchDelay, TimeUnit.MILLISECONDS);
      }

      ActiveMQClientLogger.LOGGER.debug("Started Netty Connector version " + TransportConstants.NETTY_VERSION);
   }

   @Override
   public synchronized void close() {
      if (channelClazz == null) {
         return;
      }

      if (batchFlusherFuture != null) {
         batchFlusherFuture.cancel(false);

         flusher.cancel();

         flusher = null;

         batchFlusherFuture = null;
      }

      bootstrap = null;
      channelGroup.close().awaitUninterruptibly();

      // Shutdown the EventLoopGroup if no new task was added for 100ms or if
      // 3000ms elapsed.
      group.shutdownGracefully(100, 3000, TimeUnit.MILLISECONDS);

      channelClazz = null;

      for (Connection connection : connections.values()) {
         listener.connectionDestroyed(connection.getID());
      }

      connections.clear();
   }

   @Override
   public boolean isStarted() {
      return channelClazz != null;
   }

   @Override
   public Connection createConnection() {
      if (channelClazz == null) {
         return null;
      }

      // HORNETQ-907 - strip off IPv6 scope-id (if necessary)
      SocketAddress remoteDestination = new InetSocketAddress(host, port);
      InetAddress inetAddress = ((InetSocketAddress) remoteDestination).getAddress();
      if (inetAddress instanceof Inet6Address) {
         Inet6Address inet6Address = (Inet6Address) inetAddress;
         if (inet6Address.getScopeId() != 0) {
            try {
               remoteDestination = new InetSocketAddress(InetAddress.getByAddress(inet6Address.getAddress()), ((InetSocketAddress) remoteDestination).getPort());
            }
            catch (UnknownHostException e) {
               throw new IllegalArgumentException(e.getMessage());
            }
         }
      }

      ActiveMQClientLogger.LOGGER.debug("Remote destination: " + remoteDestination);

      ChannelFuture future;
      //port 0 does not work so only use local address if set
      if (localPort != 0) {
         SocketAddress localDestination;
         if (localAddress != null) {
            localDestination = new InetSocketAddress(localAddress, localPort);
         }
         else {
            localDestination = new InetSocketAddress(localPort);
         }
         future = bootstrap.connect(remoteDestination, localDestination);
      }
      else {
         future = bootstrap.connect(remoteDestination);
      }

      future.awaitUninterruptibly();

      if (future.isSuccess()) {
         final Channel ch = future.channel();
         SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
         if (sslHandler != null) {
            Future<Channel> handshakeFuture = sslHandler.handshakeFuture();
            if (handshakeFuture.awaitUninterruptibly(30000)) {
               if (handshakeFuture.isSuccess()) {
                  ChannelPipeline channelPipeline = ch.pipeline();
                  ActiveMQChannelHandler channelHandler = channelPipeline.get(ActiveMQChannelHandler.class);
                  channelHandler.active = true;
               }
               else {
                  ch.close().awaitUninterruptibly();
                  ActiveMQClientLogger.LOGGER.errorCreatingNettyConnection(handshakeFuture.cause());
                  return null;
               }
            }
            else {
               //handshakeFuture.setFailure(new SSLException("Handshake was not completed in 30 seconds"));
               ch.close().awaitUninterruptibly();
               return null;
            }

         }
         if (httpUpgradeEnabled) {
            // Send a HTTP GET + Upgrade request that will be handled by the http-upgrade handler.
            try {
               //get this first incase it removes itself
               HttpUpgradeHandler httpUpgradeHandler = (HttpUpgradeHandler) ch.pipeline().get("http-upgrade");
               String scheme = "http";
               if (sslEnabled) {
                  scheme = "https";
               }
               URI uri = new URI(scheme, null, host, port, null, null, null);
               HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
               request.headers().set(HttpHeaders.Names.HOST, host);
               request.headers().set(HttpHeaders.Names.UPGRADE, ACTIVEMQ_REMOTING);
               request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.UPGRADE);

               final String endpoint = ConfigurationHelper.getStringProperty(TransportConstants.HTTP_UPGRADE_ENDPOINT_PROP_NAME, null, configuration);
               if (endpoint != null) {
                  request.headers().set(TransportConstants.HTTP_UPGRADE_ENDPOINT_PROP_NAME, endpoint);
               }

               // Get 16 bit nonce and base 64 encode it
               byte[] nonce = randomBytes(16);
               String key = base64(nonce);
               request.headers().set(SEC_ACTIVEMQ_REMOTING_KEY, key);
               ch.attr(REMOTING_KEY).set(key);

               ActiveMQClientLogger.LOGGER.debugf("Sending HTTP request %s", request);

               // Send the HTTP request.
               ch.writeAndFlush(request);

               if (!httpUpgradeHandler.awaitHandshake()) {
                  return null;
               }
            }
            catch (URISyntaxException e) {
               ActiveMQClientLogger.LOGGER.errorCreatingNettyConnection(e);
               return null;
            }
         }
         else {
            ChannelPipeline channelPipeline = ch.pipeline();
            ActiveMQChannelHandler channelHandler = channelPipeline.get(ActiveMQChannelHandler.class);
            channelHandler.active = true;
         }

         // No acceptor on a client connection
         Listener connectionListener = new Listener();
         NettyConnection conn = new NettyConnection(configuration, ch, connectionListener, !httpEnabled && batchDelay > 0, false);
         connectionListener.connectionCreated(null, conn, protocolManager);
         return conn;
      }
      else {
         Throwable t = future.cause();

         if (t != null && !(t instanceof ConnectException)) {
            ActiveMQClientLogger.LOGGER.errorCreatingNettyConnection(future.cause());
         }

         return null;
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static final class ActiveMQClientChannelHandler extends ActiveMQChannelHandler {

      ActiveMQClientChannelHandler(final ChannelGroup group,
                                   final BufferHandler handler,
                                   final ClientConnectionLifeCycleListener listener) {
         super(group, handler, listener);
      }
   }

   private static class HttpUpgradeHandler extends SimpleChannelInboundHandler<HttpObject> {

      private final ChannelPipeline pipeline;
      private final HttpClientCodec httpClientCodec;
      private final CountDownLatch latch = new CountDownLatch(1);
      private boolean handshakeComplete = false;

      public HttpUpgradeHandler(ChannelPipeline pipeline, HttpClientCodec httpClientCodec) {
         this.pipeline = pipeline;
         this.httpClientCodec = httpClientCodec;
      }

      @Override
      public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
         if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            if (response.getStatus().code() == HttpResponseStatus.SWITCHING_PROTOCOLS.code() && response.headers().get(HttpHeaders.Names.UPGRADE).equals(ACTIVEMQ_REMOTING)) {
               String accept = response.headers().get(SEC_ACTIVEMQ_REMOTING_ACCEPT);
               String expectedResponse = createExpectedResponse(MAGIC_NUMBER, ctx.channel().attr(REMOTING_KEY).get());

               if (expectedResponse.equals(accept)) {
                  // remove the http handlers and flag the activemq channel handler as active
                  pipeline.remove(httpClientCodec);
                  pipeline.remove(this);
                  handshakeComplete = true;
                  ActiveMQChannelHandler channelHandler = pipeline.get(ActiveMQChannelHandler.class);
                  channelHandler.active = true;
               }
               else {
                  ActiveMQClientLogger.LOGGER.httpHandshakeFailed(accept, expectedResponse);
                  ctx.close();
               }
            }
            else if (response.getStatus().code() == HttpResponseStatus.FORBIDDEN.code()) {
               ActiveMQClientLogger.LOGGER.httpUpgradeNotSupportedByRemoteAcceptor();
               ctx.close();
            }
            latch.countDown();
         }
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
         ActiveMQClientLogger.LOGGER.errorCreatingNettyConnection(cause);
         ctx.close();
      }

      public boolean awaitHandshake() {
         try {
            if (!latch.await(30000, TimeUnit.MILLISECONDS)) {
               return false;
            }
         }
         catch (InterruptedException e) {
            return false;
         }
         return handshakeComplete;
      }
   }

   class HttpHandler extends ChannelDuplexHandler {

      private Channel channel;

      private long lastSendTime = 0;

      private boolean waitingGet = false;

      private HttpIdleTimer task;

      private final String url;

      private final FutureLatch handShakeFuture = new FutureLatch();

      private boolean active = false;

      private boolean handshaking = false;

      private String cookie;

      public HttpHandler() throws Exception {
         url = new URI("http", null, host, port, servletPath, null, null).toString();
      }

      @Override
      public void channelActive(final ChannelHandlerContext ctx) throws Exception {
         super.channelActive(ctx);
         channel = ctx.channel();
         if (httpClientIdleScanPeriod > 0) {
            task = new HttpIdleTimer();
            java.util.concurrent.Future<?> future = scheduledThreadPool.scheduleAtFixedRate(task, httpClientIdleScanPeriod, httpClientIdleScanPeriod, TimeUnit.MILLISECONDS);
            task.setFuture(future);
         }
      }

      @Override
      public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
         if (task != null) {
            task.close();
         }

         super.channelInactive(ctx);
      }

      @Override
      public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
         FullHttpResponse response = (FullHttpResponse) msg;
         if (httpRequiresSessionId && !active) {
            Set<Cookie> cookieMap = CookieDecoder.decode(response.headers().get(HttpHeaders.Names.SET_COOKIE));
            for (Cookie cookie : cookieMap) {
               if (cookie.getName().equals("JSESSIONID")) {
                  this.cookie = ClientCookieEncoder.LAX.encode(cookie);
               }
            }
            active = true;
            handShakeFuture.run();
         }
         waitingGet = false;
         ctx.fireChannelRead(response.content());
      }

      @Override
      public void write(final ChannelHandlerContext ctx, final Object msg, ChannelPromise promise) throws Exception {
         if (msg instanceof ByteBuf) {
            if (httpRequiresSessionId && !active) {
               if (handshaking) {
                  handshaking = true;
               }
               else {
                  if (!handShakeFuture.await(5000)) {
                     throw new RuntimeException("Handshake failed after timeout");
                  }
               }
            }

            ByteBuf buf = (ByteBuf) msg;
            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, url, buf);
            httpRequest.headers().add(HttpHeaders.Names.HOST, NettyConnector.this.host);
            if (cookie != null) {
               httpRequest.headers().add(HttpHeaders.Names.COOKIE, cookie);
            }
            httpRequest.headers().add(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.readableBytes()));
            ctx.write(httpRequest, promise);
            lastSendTime = System.currentTimeMillis();
         }
         else {
            ctx.write(msg, promise);
            lastSendTime = System.currentTimeMillis();
         }
      }

      private class HttpIdleTimer implements Runnable {

         private boolean closed = false;

         private java.util.concurrent.Future<?> future;

         @Override
         public synchronized void run() {
            if (closed) {
               return;
            }

            if (!waitingGet && System.currentTimeMillis() > lastSendTime + httpMaxClientIdleTime) {
               FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
               httpRequest.headers().add(HttpHeaders.Names.HOST, NettyConnector.this.host);
               waitingGet = true;
               channel.writeAndFlush(httpRequest);
            }
         }

         public synchronized void setFuture(final java.util.concurrent.Future<?> future) {
            this.future = future;
         }

         public void close() {
            if (future != null) {
               future.cancel(false);
            }

            closed = true;
         }
      }
   }

   private class Listener implements ClientConnectionLifeCycleListener {

      @Override
      public void connectionCreated(final ActiveMQComponent component,
                                    final Connection connection,
                                    final ClientProtocolManager protocol) {
         if (connections.putIfAbsent(connection.getID(), connection) != null) {
            throw ActiveMQClientMessageBundle.BUNDLE.connectionExists(connection.getID());
         }
      }

      @Override
      public void connectionDestroyed(final Object connectionID) {
         if (connections.remove(connectionID) != null) {
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
         NettyConnection connection = (NettyConnection)connections.get(connectionID);
         if (connection != null) {
            connection.fireReady(ready);
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

   @Override
   public boolean isEquivalent(Map<String, Object> configuration) {
      //here we only check host and port because these two parameters
      //is sufficient to determine the target host
      String host = ConfigurationHelper.getStringProperty(TransportConstants.HOST_PROP_NAME, TransportConstants.DEFAULT_HOST, configuration);
      Integer port = ConfigurationHelper.getIntProperty(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT, configuration);

      if (!port.equals(this.port))
         return false;

      if (host.equals(this.host))
         return true;

      //The host may be an alias. We need to compare raw IP address.
      boolean result = false;
      try {
         InetAddress inetAddr1 = InetAddress.getByName(host);
         InetAddress inetAddr2 = InetAddress.getByName(this.host);
         String ip1 = inetAddr1.getHostAddress();
         String ip2 = inetAddr2.getHostAddress();
         ActiveMQClientLogger.LOGGER.debug(this + " host 1: " + host + " ip address: " + ip1 + " host 2: " + this.host + " ip address: " + ip2);

         result = ip1.equals(ip2);
      }
      catch (UnknownHostException e) {
         ActiveMQClientLogger.LOGGER.error("Cannot resolve host", e);
      }

      return result;
   }

   @Override
   public void finalize() throws Throwable {
      close();
      super.finalize();
   }

   //for test purpose only
   public Bootstrap getBootStrap() {
      return bootstrap;
   }

   public static void clearThreadPools() {
      SharedNioEventLoopGroup.forceShutdown();
   }

   private static ClassLoader getThisClassLoader() {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
         @Override
         public ClassLoader run() {
            return ClientSessionFactoryImpl.class.getClassLoader();
         }
      });

   }

   private static String base64(byte[] data) {
      ByteBuf encodedData = Unpooled.wrappedBuffer(data);
      ByteBuf encoded = Base64.encode(encodedData);
      String encodedString = encoded.toString(StandardCharsets.UTF_8);
      encoded.release();
      return encodedString;
   }

   /**
    * Creates an arbitrary number of random bytes
    *
    * @param size the number of random bytes to create
    * @return An array of random bytes
    */
   private static byte[] randomBytes(int size) {
      byte[] bytes = new byte[size];

      for (int index = 0; index < size; index++) {
         bytes[index] = (byte) randomNumber(0, 255);
      }

      return bytes;
   }

   private static int randomNumber(int minimum, int maximum) {
      return (int) (Math.random() * maximum + minimum);
   }

   public static String createExpectedResponse(final String magicNumber, final String secretKey) throws IOException {
      try {
         final String concat = secretKey + magicNumber;
         final MessageDigest digest = MessageDigest.getInstance("SHA1");

         digest.update(concat.getBytes(StandardCharsets.UTF_8));
         final byte[] bytes = digest.digest();
         return encodeBytes(bytes);
      }
      catch (NoSuchAlgorithmException e) {
         throw new IOException(e);
      }
   }
}

