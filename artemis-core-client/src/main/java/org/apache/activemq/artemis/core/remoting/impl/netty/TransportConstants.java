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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import io.netty.util.Version;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;

public class TransportConstants {

   public static final String SSL_ENABLED_PROP_NAME = "sslEnabled";

   public static final String SSL_KRB5_CONFIG_PROP_NAME = "sslKrb5Config";

   public static final String HTTP_ENABLED_PROP_NAME = "httpEnabled";

   public static final String HTTP_CLIENT_IDLE_PROP_NAME = "httpClientIdleTime";

   public static final String HTTP_CLIENT_IDLE_SCAN_PERIOD = "httpClientIdleScanPeriod";

   public static final String HTTP_RESPONSE_TIME_PROP_NAME = "httpResponseTime";

   public static final String HTTP_SERVER_SCAN_PERIOD_PROP_NAME = "httpServerScanPeriod";

   public static final String HTTP_REQUIRES_SESSION_ID = "httpRequiresSessionId";

   public static final String HTTP_UPGRADE_ENABLED_PROP_NAME = "httpUpgradeEnabled";

   public static final String HTTP_UPGRADE_ENDPOINT_PROP_NAME = "httpUpgradeEndpoint";

   public static final String USE_SERVLET_PROP_NAME = "useServlet";

   public static final String SERVLET_PATH = "servletPath";

   public static final String USE_NIO_PROP_NAME = "useNio";

   public static final String USE_EPOLL_PROP_NAME = "useEpoll";

   public static final String USE_KQUEUE_PROP_NAME = "useKQueue";

   @Deprecated
   /**
    * @deprecated Use USE_GLOBAL_WORKER_POOL_PROP_NAME
    */
   public static final String USE_NIO_GLOBAL_WORKER_POOL_PROP_NAME = "useNioGlobalWorkerPool";

   public static final String USE_GLOBAL_WORKER_POOL_PROP_NAME = "useGlobalWorkerPool";

   public static final String USE_INVM_PROP_NAME = "useInvm";

   public static final String ACTIVEMQ_SERVER_NAME = "activemqServerName";

   /**
    * @deprecated use PROTOCOLS_PROP_NAME
    */
   @Deprecated
   public static final String PROTOCOL_PROP_NAME = "protocol";

   public static final String PROTOCOLS_PROP_NAME = "protocols";

   public static final String HOST_PROP_NAME = "host";

   public static final String PORT_PROP_NAME = "port";

   public static final String LOCAL_ADDRESS_PROP_NAME = "localAddress";

   public static final String LOCAL_PORT_PROP_NAME = "localPort";

   public static final String KEYSTORE_PROVIDER_PROP_NAME = "keyStoreProvider";

   public static final String KEYSTORE_PATH_PROP_NAME = "keyStorePath";

   public static final String KEYSTORE_PASSWORD_PROP_NAME = "keyStorePassword";

   public static final String TRUSTSTORE_PROVIDER_PROP_NAME = "trustStoreProvider";

   public static final String TRUSTSTORE_PATH_PROP_NAME = "trustStorePath";

   public static final String TRUSTSTORE_PASSWORD_PROP_NAME = "trustStorePassword";

   public static final String CRL_PATH_PROP_NAME = "crlPath";

   public static final String ENABLED_CIPHER_SUITES_PROP_NAME = "enabledCipherSuites";

   public static final String ENABLED_PROTOCOLS_PROP_NAME = "enabledProtocols";

   public static final String NEED_CLIENT_AUTH_PROP_NAME = "needClientAuth";

   public static final String WANT_CLIENT_AUTH_PROP_NAME = "wantClientAuth";

   public static final String VERIFY_HOST_PROP_NAME = "verifyHost";

   public static final String TRUST_ALL_PROP_NAME = "trustAll";

   public static final String SNIHOST_PROP_NAME = "sniHost";

   public static final String BACKLOG_PROP_NAME = "backlog";

   public static final String USE_DEFAULT_SSL_CONTEXT_PROP_NAME = "useDefaultSslContext";

   public static final String SSL_PROVIDER = "sslProvider";

   public static final String NETTY_VERSION;

   /**
    * Disable Nagle's algorithm.<br>
    * Valid for (client) Sockets.
    *
    * @see <a
    * href="http://design.jboss.org/jbossorg/branding/Javadocs/doc/api/org/jboss/netty/channel/socket/SocketChannelConfig.html#setTcpNoDelay%28boolean%29">
    * Netty note on this option</a>
    * @see <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/net/socketOpt.html">Oracle
    * doc on tcpNoDelay</a>
    */
   public static final String TCP_NODELAY_PROPNAME = "tcpNoDelay";

   public static final String TCP_SENDBUFFER_SIZE_PROPNAME = "tcpSendBufferSize";

   public static final String TCP_RECEIVEBUFFER_SIZE_PROPNAME = "tcpReceiveBufferSize";

   @Deprecated
   /**
    * @deprecated Use REMOTING_THREADS_PROPNAME
    */
   public static final String NIO_REMOTING_THREADS_PROPNAME = "nioRemotingThreads";

   public static final String WRITE_BUFFER_LOW_WATER_MARK_PROPNAME = "writeBufferLowWaterMark";

   public static final String WRITE_BUFFER_HIGH_WATER_MARK_PROPNAME = "writeBufferHighWaterMark";

   public static final String REMOTING_THREADS_PROPNAME = "remotingThreads";

   public static final String BATCH_DELAY = "batchDelay";

   public static final String DIRECT_DELIVER = "directDeliver";

   public static final String CLUSTER_CONNECTION = "clusterConnection";

   public static final String STOMP_CONSUMERS_CREDIT = "stompConsumerCredits";

   public static final int STOMP_DEFAULT_CONSUMERS_CREDIT = 10 * 1024; // 10K

   public static final boolean DEFAULT_SSL_ENABLED = false;

   public static final String DEFAULT_SSL_KRB5_CONFIG = null;

   public static final String DEFAULT_SNIHOST_CONFIG = null;

   public static final boolean DEFAULT_USE_GLOBAL_WORKER_POOL = true;

   public static final boolean DEFAULT_USE_EPOLL = true;

   public static final boolean DEFAULT_USE_KQUEUE = true;

   public static final boolean DEFAULT_USE_INVM = false;

   public static final boolean DEFAULT_USE_SERVLET = false;

   public static final String DEFAULT_HOST = "localhost";

   public static final int DEFAULT_PORT = 61616;

   public static final String DEFAULT_LOCAL_ADDRESS = null;

   public static final int DEFAULT_LOCAL_PORT = 0;

   public static final int DEFAULT_STOMP_PORT = 61613;

   public static final String DEFAULT_KEYSTORE_PROVIDER = "JKS";

   public static final String DEFAULT_KEYSTORE_PATH = null;

   public static final String DEFAULT_KEYSTORE_PASSWORD = null;

   public static final String DEFAULT_TRUSTSTORE_PROVIDER = "JKS";

   public static final String DEFAULT_TRUSTSTORE_PATH = null;

   public static final String DEFAULT_TRUSTSTORE_PASSWORD = null;

   public static final String DEFAULT_CRL_PATH = null;

   public static final String DEFAULT_ENABLED_CIPHER_SUITES = null;

   public static final String DEFAULT_ENABLED_PROTOCOLS = null;

   public static final boolean DEFAULT_NEED_CLIENT_AUTH = false;

   public static final boolean DEFAULT_WANT_CLIENT_AUTH = false;

   public static final boolean DEFAULT_VERIFY_HOST = false;

   public static final String DEFAULT_SSL_PROVIDER = "JDK";

   public static final String OPENSSL_PROVIDER = "OPENSSL";

   public static final boolean DEFAULT_TRUST_ALL = false;

   public static final boolean DEFAULT_USE_DEFAULT_SSL_CONTEXT = false;

   public static final boolean DEFAULT_TCP_NODELAY = true;

   public static final int DEFAULT_TCP_SENDBUFFER_SIZE = 1024 * 1024;

   public static final int DEFAULT_TCP_RECEIVEBUFFER_SIZE = 1024 * 1024;

   public static final int DEFAULT_WRITE_BUFFER_LOW_WATER_MARK = 32 * 1024;

   public static final int DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK = 128 * 1024;

   public static final boolean DEFAULT_HTTP_ENABLED = false;

   public static final long DEFAULT_HTTP_CLIENT_IDLE_TIME = 500;

   public static final long DEFAULT_HTTP_CLIENT_SCAN_PERIOD = 500;

   public static final long DEFAULT_HTTP_RESPONSE_TIME = 10000;

   public static final long DEFAULT_HTTP_SERVER_SCAN_PERIOD = 5000;

   public static final boolean DEFAULT_HTTP_REQUIRES_SESSION_ID = false;

   public static final boolean DEFAULT_HTTP_UPGRADE_ENABLED = false;

   public static final String DEFAULT_SERVLET_PATH = "/messaging/ActiveMQServlet";

   public static final long DEFAULT_BATCH_DELAY = 0;

   public static final boolean DEFAULT_DIRECT_DELIVER = true;

   public static final Set<String> ALLOWABLE_CONNECTOR_KEYS;

   public static final Set<String> ALLOWABLE_ACCEPTOR_KEYS;

   public static final String CONNECTION_TTL = "connectionTtl";

   public static final String CONNECTION_TTL_MAX = "connectionTtlMax";

   public static final String CONNECTION_TTL_MIN = "connectionTtlMin";

   public static final String HEART_BEAT_TO_CONNECTION_TTL_MODIFIER = "heartBeatToConnectionTtlModifier";

   public static final String STOMP_ENABLE_MESSAGE_ID = "stomp-enable-message-id";

   public static final String STOMP_MIN_LARGE_MESSAGE_SIZE = "stomp-min-large-message-size";

   public static final String NETTY_CONNECT_TIMEOUT = "connect-timeout-millis";

   public static final int DEFAULT_NETTY_CONNECT_TIMEOUT = -1;

   public static final String CONNECTIONS_ALLOWED = "connectionsAllowed";

   public static final long DEFAULT_CONNECTIONS_ALLOWED = -1L;

   public static final String STOMP_MAX_FRAME_PAYLOAD_LENGTH = "stompMaxFramePayloadLength";

   public static final int DEFAULT_STOMP_MAX_FRAME_PAYLOAD_LENGTH = 65536;

   public static final String HANDSHAKE_TIMEOUT = "handshake-timeout";

   public static final int DEFAULT_HANDSHAKE_TIMEOUT = 10;

   static {
      Set<String> allowableAcceptorKeys = new HashSet<>();
      allowableAcceptorKeys.add(TransportConstants.SSL_ENABLED_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HTTP_RESPONSE_TIME_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HTTP_SERVER_SCAN_PERIOD_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.USE_NIO_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.USE_EPOLL_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.USE_KQUEUE_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.USE_INVM_PROP_NAME);
      //noinspection deprecation
      allowableAcceptorKeys.add(TransportConstants.PROTOCOL_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.PROTOCOLS_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HOST_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.PORT_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.KEYSTORE_PATH_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.TRUSTSTORE_PATH_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.WANT_CLIENT_AUTH_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.VERIFY_HOST_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.TCP_NODELAY_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.WRITE_BUFFER_HIGH_WATER_MARK_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.WRITE_BUFFER_LOW_WATER_MARK_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.NIO_REMOTING_THREADS_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.REMOTING_THREADS_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.BATCH_DELAY);
      allowableAcceptorKeys.add(TransportConstants.DIRECT_DELIVER);
      allowableAcceptorKeys.add(TransportConstants.CLUSTER_CONNECTION);
      allowableAcceptorKeys.add(TransportConstants.STOMP_CONSUMERS_CREDIT);
      allowableAcceptorKeys.add(TransportConstants.STOMP_MIN_LARGE_MESSAGE_SIZE);
      allowableAcceptorKeys.add(TransportConstants.CONNECTION_TTL);
      allowableAcceptorKeys.add(TransportConstants.CONNECTION_TTL_MAX);
      allowableAcceptorKeys.add(TransportConstants.CONNECTION_TTL_MIN);
      allowableAcceptorKeys.add(TransportConstants.HEART_BEAT_TO_CONNECTION_TTL_MODIFIER);
      allowableAcceptorKeys.add(TransportConstants.STOMP_ENABLE_MESSAGE_ID);
      allowableAcceptorKeys.add(TransportConstants.CONNECTIONS_ALLOWED);
      allowableAcceptorKeys.add(TransportConstants.STOMP_MAX_FRAME_PAYLOAD_LENGTH);
      allowableAcceptorKeys.add(ActiveMQDefaultConfiguration.getPropMaskPassword());
      allowableAcceptorKeys.add(ActiveMQDefaultConfiguration.getPropPasswordCodec());
      allowableAcceptorKeys.add(TransportConstants.BACKLOG_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.CRL_PATH_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HANDSHAKE_TIMEOUT);
      allowableAcceptorKeys.add(TransportConstants.SSL_PROVIDER);

      ALLOWABLE_ACCEPTOR_KEYS = Collections.unmodifiableSet(allowableAcceptorKeys);

      Set<String> allowableConnectorKeys = new HashSet<>();
      allowableConnectorKeys.add(TransportConstants.ACTIVEMQ_SERVER_NAME);
      allowableConnectorKeys.add(TransportConstants.SSL_ENABLED_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HTTP_ENABLED_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD);
      allowableConnectorKeys.add(TransportConstants.HTTP_REQUIRES_SESSION_ID);
      allowableConnectorKeys.add(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HTTP_UPGRADE_ENDPOINT_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.USE_SERVLET_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.SERVLET_PATH);
      allowableConnectorKeys.add(TransportConstants.USE_NIO_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.USE_NIO_GLOBAL_WORKER_POOL_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.USE_EPOLL_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.USE_KQUEUE_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.USE_GLOBAL_WORKER_POOL_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HOST_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.PORT_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.LOCAL_ADDRESS_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.LOCAL_PORT_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.KEYSTORE_PATH_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.TRUSTSTORE_PATH_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.VERIFY_HOST_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.TRUST_ALL_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.TCP_NODELAY_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.WRITE_BUFFER_HIGH_WATER_MARK_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.WRITE_BUFFER_LOW_WATER_MARK_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.NIO_REMOTING_THREADS_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.REMOTING_THREADS_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.BATCH_DELAY);
      allowableConnectorKeys.add(ActiveMQDefaultConfiguration.getPropMaskPassword());
      allowableConnectorKeys.add(ActiveMQDefaultConfiguration.getPropPasswordCodec());
      allowableConnectorKeys.add(TransportConstants.NETTY_CONNECT_TIMEOUT);
      allowableConnectorKeys.add(TransportConstants.USE_DEFAULT_SSL_CONTEXT_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.SSL_PROVIDER);
      allowableConnectorKeys.add(TransportConstants.HANDSHAKE_TIMEOUT);
      allowableConnectorKeys.add(TransportConstants.CRL_PATH_PROP_NAME);

      ALLOWABLE_CONNECTOR_KEYS = Collections.unmodifiableSet(allowableConnectorKeys);

      String version;
      Version v = Version.identify().get("netty-transport");
      if (v == null) {
         version = "unknown";
      } else {
         version = v.artifactVersion();
      }
      NETTY_VERSION = version;
   }

}
