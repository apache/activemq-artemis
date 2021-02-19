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

import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.spi.core.remoting.ssl.SSLContextConfig;
import org.apache.activemq.artemis.spi.core.remoting.ssl.SSLContextFactory;
import org.apache.activemq.artemis.utils.ConfigurationHelper;

/**
 * Simple SSLContextFactory for use in NettyConnector and NettyAcceptor.
 */
public class DefaultSSLContextFactory implements SSLContextFactory {

   @Override
   public SSLContext getSSLContext(final SSLContextConfig config, final Map<String, Object> additionalOpts) throws Exception {
      final boolean useDefaultSslContext = ConfigurationHelper.getBooleanProperty(
         TransportConstants.USE_DEFAULT_SSL_CONTEXT_PROP_NAME,
         TransportConstants.DEFAULT_USE_DEFAULT_SSL_CONTEXT,
         additionalOpts
      );

      if (useDefaultSslContext) {
         log.debug("Using the Default JDK SSLContext.");
         return SSLContext.getDefault();
      }

      log.debugf("Creating JDK SSLContext with %s", config);
      return new SSLSupport(config).createContext();
   }

   @Override
   public int getPriority() {
      return 5;
   }
}
