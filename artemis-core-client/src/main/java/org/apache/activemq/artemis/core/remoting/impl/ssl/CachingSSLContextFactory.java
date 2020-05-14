/*
 * Copyright 2020 The Apache Software Foundation.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.utils.ConfigurationHelper;

/**
 * SSLContextFactory providing a cache of SSLContext.
 * Since SSLContext should be reused instead of recreated and are thread safe.
 * To activate it uou need to allow this Service to be discovered by having a
 * <code>META-INF/services/org.apache.activemq.artemis.spi.core.remoting.ssl.SSLContextFactory</code>
 * file with <code> org.apache.activemq.artemis.core.remoting.impl.ssl.CachingSSLContextFactory</code>
 * as value.
 */
public class CachingSSLContextFactory extends DefaultSSLContextFactory {

   private static final Map<String, SSLContext> SSL_CONTEXTS = Collections.synchronizedMap(new HashMap<>());

   @Override
   public void clearSSLContexts() {
      SSL_CONTEXTS.clear();
   }

   @Override
   public SSLContext getSSLContext(Map<String, Object> configuration,
           String keystoreProvider, String keystorePath, String keystorePassword,
           String truststoreProvider, String truststorePath, String truststorePassword,
           String crlPath, String trustManagerFactoryPlugin, boolean trustAll) throws Exception {
      String sslContextName = getSSLContextName(configuration, keystorePath, keystoreProvider, truststorePath, truststoreProvider);
      if (!SSL_CONTEXTS.containsKey(sslContextName)) {
         SSL_CONTEXTS.put(sslContextName, createSSLContext(configuration,
              keystoreProvider, keystorePath, keystorePassword,
              truststoreProvider, truststorePath, truststorePassword,
              crlPath, trustManagerFactoryPlugin, trustAll));
      }
      return SSL_CONTEXTS.get(sslContextName);
   }

   /**
    * Obtain the sslContextName :
    *  - if available the 'sslContext' from the configuration
    *  - otherwise if available the keyStorePath + '_' + keystoreProvider
    *  - otherwise the truststorePath + '_' + truststoreProvider.
    * @param configuration
    * @param keyStorePath
    * @param keystoreProvider
    * @param truststorePath
    * @param truststoreProvider
    * @return the ley associated to the SSLContext.
    */
   protected String getSSLContextName(Map<String, Object> configuration, String keyStorePath, String keystoreProvider, String truststorePath, String truststoreProvider) {
      String sslContextName = ConfigurationHelper.getStringProperty(TransportConstants.SSL_CONTEXT_PROP_NAME, null, configuration);
      if (sslContextName == null) {
         if (keyStorePath != null) {
            return keyStorePath + '_' + keystoreProvider;
         }
         return truststorePath + '_' + truststoreProvider;
      }
      return sslContextName;
   }

   @Override
   public int getPriority() {
      return 10;
   }
}
