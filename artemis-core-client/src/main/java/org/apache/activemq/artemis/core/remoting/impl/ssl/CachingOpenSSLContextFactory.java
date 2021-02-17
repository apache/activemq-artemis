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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.spi.core.remoting.ssl.OpenSSLContextFactory;
import org.apache.activemq.artemis.utils.ConfigurationHelper;

import io.netty.handler.ssl.SslContext;

/**
 * {@link OpenSSLContextFactory} providing a cache of {@link SslContext}.
 * Since {@link SslContext} should be reused instead of recreated and are thread safe.
 * To activate it you need to allow this Service to be discovered by having a
 * <code>META-INF/services/org.apache.activemq.artemis.spi.core.remoting.ssl.OpenSSLContextFactory</code>
 * file with <code>org.apache.activemq.artemis.core.remoting.impl.ssl.CachingOpenSSLContextFactory</code>
 * as value.
 */
public class CachingOpenSSLContextFactory extends DefaultOpenSSLContextFactory {

   private final ConcurrentMap<Object, SslContext> sslContexts = new ConcurrentHashMap<>();

   private static final class CompositeCacheKey {
      final boolean isForServer;
      final String keystorePath;
      final String keystoreProvider;
      final String truststorePath;
      final String truststoreProvider;
      final String trustManagerFactoryPlugin;
      final String crlPath;
      final boolean trustAll;
      final int hashCode;

      CompositeCacheKey(final boolean isForServer,
         final String keystorePath, final String keystoreProvider,
         final String truststorePath, final String truststoreProvider,
         final String crlPath, final String trustManagerFactoryPlugin, final boolean trustAll
      ) {
         this.isForServer = isForServer;
         this.keystorePath = keystorePath;
         this.keystoreProvider = keystoreProvider;
         this.truststorePath = truststorePath;
         this.truststoreProvider = truststoreProvider;
         this.trustManagerFactoryPlugin = trustManagerFactoryPlugin;
         this.crlPath = crlPath;
         this.trustAll = trustAll;
         hashCode = Objects.hash(isForServer,
            keystorePath, keystoreProvider,
            truststorePath, truststoreProvider,
            crlPath, trustManagerFactoryPlugin, trustAll
         );
      }

      @Override
      public int hashCode() {
         return hashCode;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null || getClass() != obj.getClass())
            return false;
         final CompositeCacheKey other = (CompositeCacheKey) obj;
         return isForServer == other.isForServer
            && Objects.equals(keystorePath, other.keystorePath)
            && Objects.equals(keystoreProvider, other.keystoreProvider)
            && Objects.equals(truststorePath, other.truststorePath)
            && Objects.equals(truststoreProvider, other.truststoreProvider)
            && Objects.equals(crlPath, other.crlPath)
            && Objects.equals(trustManagerFactoryPlugin, other.trustManagerFactoryPlugin)
            && trustAll == other.trustAll;
      }
   }

   @Override
   public void clearSslContexts() {
      sslContexts.clear();
   }

   @Override
   public SslContext getClientSslContext(final Map<String, Object> config,
      final String keystoreProvider, final String keystorePath, final String keystorePassword,
      final String truststoreProvider, final String truststorePath, final String truststorePassword,
      final String crlPath, final String trustManagerFactoryPlugin, final boolean trustAll
   ) throws Exception {

      final Object cachKey = getCacheKey(true, config,
         keystoreProvider, keystorePath,
         truststoreProvider, truststorePath,
         crlPath, trustManagerFactoryPlugin, trustAll
      );

      return sslContexts.computeIfAbsent(cachKey, k -> {
         try {
            return CachingOpenSSLContextFactory.super.getClientSslContext(config,
               keystoreProvider, keystorePath, keystorePassword,
               truststoreProvider, truststorePath, truststorePassword,
               crlPath, trustManagerFactoryPlugin, trustAll
            );
         } catch (final Exception ex) {
            throw new RuntimeException("An unexpected exception occured while creating SslContext", ex);
         }
      });
   }

   @Override
   public SslContext getServerSslContext(final Map<String, Object> config,
      final String keystoreProvider, final String keystorePath, final String keystorePassword,
      final String truststoreProvider, final String truststorePath, final String truststorePassword,
      final String crlPath, final String trustManagerFactoryPlugin, final boolean trustAll
   ) throws Exception {

      final Object cachKey = getCacheKey(true, config,
         keystoreProvider, keystorePath,
         truststoreProvider, truststorePath,
         crlPath, trustManagerFactoryPlugin, trustAll
      );

      return sslContexts.computeIfAbsent(cachKey, k -> {
         try {
            return CachingOpenSSLContextFactory.super.getServerSslContext(config,
               keystoreProvider, keystorePath, keystorePassword,
               truststoreProvider, truststorePath, truststorePassword,
               crlPath, trustManagerFactoryPlugin, trustAll
            );
         } catch (final Exception ex) {
            throw new RuntimeException("An unexpected exception occured while creating SslContext", ex);
         }
      });
   }

   /**
    * Obtains/calculates a cache key for the corresponding {@link SslContext}.
    * <ol>
    * <li>If <code>config</code> contains an entry with key "sslContext", the associated value is returned
    * <li>Otherwise, a {@link CompositeCacheKey} is returned.
    * </ol>
    *
    * @return the SSL context name to cache/retrieve the {@link SslContext}.
    */
   protected Object getCacheKey(final boolean isForServer, final Map<String, Object> config,
      final String keystoreProvider, final String keystorePath,
      final String truststoreProvider, final String truststorePath,
      final String crlPath, final String trustManagerFactoryPlugin, final boolean trustAll
   ) {
      final Object cacheKey = ConfigurationHelper.getStringProperty(TransportConstants.SSL_CONTEXT_PROP_NAME, null, config);
      if (cacheKey != null)
         return cacheKey;

      return new CompositeCacheKey(isForServer,
         keystorePath, keystoreProvider,
         truststorePath, truststoreProvider,
         crlPath, trustManagerFactoryPlugin, trustAll
      );
   }

   @Override
   public int getPriority() {
      return 10;
   }
}
