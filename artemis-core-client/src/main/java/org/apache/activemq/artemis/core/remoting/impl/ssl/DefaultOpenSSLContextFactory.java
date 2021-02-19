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

import io.netty.handler.ssl.SslContext;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.spi.core.remoting.ssl.OpenSSLContextFactory;
import org.apache.activemq.artemis.spi.core.remoting.ssl.SSLContextConfig;

/**
 * Default {@link OpenSSLContextFactory} for use in {@link NettyConnector} and NettyAcceptor.
 */
public class DefaultOpenSSLContextFactory implements OpenSSLContextFactory {

   /**
    * @param additionalOpts not used by this implementation
    *
    * @return an {@link SslContext} instance for the given configuration.
    */
   @Override
   public SslContext getClientSslContext(final SSLContextConfig config, final Map<String, Object> additionalOpts) throws Exception {
      log.debugf("Creating Client OpenSSL Context with %s", config);
      return new SSLSupport(config)
         .setSslProvider(TransportConstants.OPENSSL_PROVIDER)
         .createNettyClientContext();
   }

   /**
    * @param additionalOpts not used by this implementation
    *
    * @return an {@link SslContext} instance for the given configuration.
    */
   @Override
   public SslContext getServerSslContext(final SSLContextConfig config, final Map<String, Object> additionalOpts) throws Exception {
      log.debugf("Creating Server OpenSSL Context with %s", config);
      return new SSLSupport(config)
         .setSslProvider(TransportConstants.OPENSSL_PROVIDER)
         .createNettyContext();
   }

   @Override
   public int getPriority() {
      return 5;
   }
}
