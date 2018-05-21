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
package org.apache.activemq.artemis.tests.integration.ssl;

import io.netty.handler.ssl.OpenSsl;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class SSLTestBase extends ActiveMQTestBase {

   @Parameterized.Parameters(name = "sslProvider={0},clientProvider={1}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{TransportConstants.OPENSSL_PROVIDER, TransportConstants.DEFAULT_SSL_PROVIDER},
                                          {TransportConstants.OPENSSL_PROVIDER, TransportConstants.OPENSSL_PROVIDER},
                                          {TransportConstants.DEFAULT_SSL_PROVIDER, TransportConstants.DEFAULT_SSL_PROVIDER},
                                          {TransportConstants.DEFAULT_SSL_PROVIDER, TransportConstants.OPENSSL_PROVIDER}});
   }

   protected static final String QUEUE = "ssl.test.queue";

   protected final String PASSWORD = "secureexample";
   protected String SERVER_SIDE_KEYSTORE = "openssl-server-side-keystore.jks";
   protected String SERVER_SIDE_TRUSTSTORE = "openssl-server-side-truststore.jks";
   protected String CLIENT_SIDE_TRUSTSTORE = "openssl-client-side-truststore.jks";
   protected String CLIENT_SIDE_KEYSTORE = "openssl-client-side-keystore.jks";

   protected ActiveMQServer server;

   protected TransportConfiguration tc;

   protected String sslProvider;
   protected String clientSslProvider;

   public SSLTestBase(String sslProvider, String clientSslProvider) {
      this.sslProvider = sslProvider;
      this.clientSslProvider = clientSslProvider;
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      Map<String, Object> params = new HashMap<>();
      configureSSLParameters(params);
      ConfigurationImpl config = createBasicConfig();
      config.addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, getNettyAcceptorName()));
      config.addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));

      server = addServer(ActiveMQServers.newActiveMQServer(config, ManagementFactory.getPlatformMBeanServer(), null, false));

      server.start();
      waitForServerToStart(server);
      tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      tc.getParams().put(TransportConstants.HOST_PROP_NAME, params.get(TransportConstants.HOST_PROP_NAME));
      tc.getParams().put(TransportConstants.PORT_PROP_NAME, params.get(TransportConstants.PORT_PROP_NAME));
      tc.getParams().put(TransportConstants.SSL_PROVIDER, clientSslProvider);
   }

   protected void configureSSLParameters(Map<String, Object> params) {
      System.out.println("*** Configure server SSL using provider: " + sslProvider);
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.SSL_PROVIDER, sslProvider);
      params.put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, "JKS");
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, SERVER_SIDE_KEYSTORE);
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);
      params.put(TransportConstants.HOST_PROP_NAME, "localhost");
      params.put(TransportConstants.PORT_PROP_NAME, "61617");
   }

   public String getNettyAcceptorName() {
      return "SSLTestAcceptor";
   }


   protected boolean isOpenSSLSupported() {
      if (sslProvider.equals(TransportConstants.OPENSSL_PROVIDER) || clientSslProvider.equals(TransportConstants.OPENSSL_PROVIDER)) {
         return OpenSsl.isAvailable();
      }
      return true;
   }

}
