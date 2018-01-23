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

package org.apache.activemq.artemis.tests.integration.stomp;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class StompWithClientIdValidationTest extends StompTestBase {

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Override
   protected JMSServerManager createServer() throws Exception {
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME,
            StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PROTOCOLS_PROP_NAME,
            StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");

      TransportConfiguration stompTransport = new TransportConfiguration(
            NettyAcceptorFactory.class.getName(), params);

      Configuration config = createBasicConfig() //
            .setSecurityEnabled(isSecurityEnabled()) //
            .setPersistenceEnabled(isPersistenceEnabled()) //
            .addAcceptorConfiguration(stompTransport) //
            .addAcceptorConfiguration(
                  new TransportConfiguration(InVMAcceptorFactory.class.getName())) //
            .setConnectionTtlCheckInterval(500);

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager(
            InVMLoginModule.class.getName(), new SecurityConfiguration()) {
         @Override
         public String validateUser(String user, String password,
               RemotingConnection remotingConnection) {

            String validatedUser = super.validateUser(user, password, remotingConnection);
            if (validatedUser == null) {
               return null;
            }

            if ("STOMP".equals(remotingConnection.getProtocolName())) {
               final String clientId = remotingConnection.getClientID();
               /*
                * perform some kind of clientId validation, e.g. check presence or format
                */
               if (clientId == null || clientId.length() == 0) {
                  System.err.println("ClientID not set!");
                  return null;
               }
            }
            return validatedUser;
         }
      };

      securityManager.getConfiguration().addUser(defUser, defPass);

      ActiveMQServer activeMqServer = addServer(ActiveMQServers.newActiveMQServer(config,
            ManagementFactory.getPlatformMBeanServer(), securityManager));

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      server = new JMSServerManagerImpl(activeMqServer, jmsConfig);
      server.setRegistry(new JndiBindingRegistry(new InVMNamingContext()));
      return server;
   }

   @Test
   public void testStompConnectWithClientId() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);

      try {
         assertEquals("CONNECTED", conn.connect(defUser, defPass, "MyClientID").getCommand());
      } finally {
         conn.closeTransport();
      }
   }

   @Test
   public void testStompConnectWithoutClientId() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      try {
         assertEquals("ERROR", conn.connect(defUser, defPass).getCommand());
      } finally {
         conn.closeTransport();
      }
   }
}
