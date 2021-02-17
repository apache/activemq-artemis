/*
 * Copyright 2021 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.remoting.impl.ssl;

import java.util.Map;

import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.spi.core.remoting.ssl.OpenSSLContextFactory;

import io.netty.handler.ssl.SslContext;

/**
 * Default {@link OpenSSLContextFactory} for use in {@link NettyConnector} and NettyAcceptor.
 */
public class DefaultOpenSSLContextFactory implements OpenSSLContextFactory {

   protected SSLSupport createSSLSupport(final Map<String, Object> config,
      final String keystoreProvider, final String keystorePath, final String keystorePassword,
      final String truststoreProvider, final String truststorePath, final String truststorePassword,
      final String crlPath, final String trustManagerFactoryPlugin, final boolean trustAll
   ) {
      if (log.isDebugEnabled()) {
         final String sep = System.getProperty("line.separator") + "  ";
         log.debug("Creating io.netty.handler.ssl.SslContext with configuration:" + sep +
            TransportConstants.SSL_PROVIDER + '=' + TransportConstants.OPENSSL_PROVIDER + sep +
            TransportConstants.SSL_CONTEXT_PROP_NAME + '=' + config.get(TransportConstants.SSL_CONTEXT_PROP_NAME) + sep +
            TransportConstants.KEYSTORE_PROVIDER_PROP_NAME + '=' + keystoreProvider + sep +
            TransportConstants.KEYSTORE_PATH_PROP_NAME + '=' + keystorePath + sep +
            TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME + '=' + truststoreProvider + sep +
            TransportConstants.TRUSTSTORE_PATH_PROP_NAME + '=' + truststorePath + sep +
            TransportConstants.CRL_PATH_PROP_NAME + '=' + crlPath + sep +
            TransportConstants.TRUST_MANAGER_FACTORY_PLUGIN_PROP_NAME + '=' + trustManagerFactoryPlugin + sep +
            TransportConstants.TRUST_ALL_PROP_NAME + '=' + trustAll
         );
      }
      return new SSLSupport()
         .setSslProvider(TransportConstants.OPENSSL_PROVIDER)
         .setKeystoreProvider(keystoreProvider)
         .setKeystorePath(keystorePath)
         .setKeystorePassword(keystorePassword)
         .setTruststoreProvider(truststoreProvider)
         .setTruststorePath(truststorePath)
         .setTruststorePassword(truststorePassword)
         .setTrustAll(trustAll)
         .setCrlPath(crlPath)
         .setTrustManagerFactoryPlugin(trustManagerFactoryPlugin);
   }

   @Override
   public SslContext getClientSslContext(final Map<String, Object> config,
      final String keystoreProvider, final String keystorePath, final String keystorePassword,
      final String truststoreProvider, final String truststorePath, final String truststorePassword,
      final String crlPath, final String trustManagerFactoryPlugin, final boolean trustAll
   ) throws Exception {

      return createSSLSupport(config,
         keystoreProvider, keystorePath, keystorePassword,
         truststoreProvider, truststorePath, truststorePassword,
         crlPath, trustManagerFactoryPlugin, trustAll
      ).createNettyClientContext();
   }

   @Override
   public SslContext getServerSslContext(final Map<String, Object> config,
      final String keystoreProvider, final String keystorePath, final String keystorePassword,
      final String truststoreProvider, final String truststorePath, final String truststorePassword,
      final String crlPath, final String trustManagerFactoryPlugin, final boolean trustAll
   ) throws Exception {

      return createSSLSupport(config,
         keystoreProvider, keystorePath, keystorePassword,
         truststoreProvider, truststorePath, truststorePassword,
         crlPath, trustManagerFactoryPlugin, trustAll
      ).createNettyContext();
   }

   @Override
   public int getPriority() {
      return 5;
   }
}
