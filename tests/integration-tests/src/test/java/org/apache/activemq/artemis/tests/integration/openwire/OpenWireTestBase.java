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
package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.ConnectionFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;
import org.junit.After;
import org.junit.Before;

public class OpenWireTestBase extends ActiveMQTestBase {

   public static final String OWHOST = "localhost";
   public static final int OWPORT = 61616;

   protected ActiveMQServer server;

   protected JMSServerManagerImpl jmsServer;
   protected boolean realStore = false;
   protected boolean enableSecurity = false;

   protected ConnectionFactory coreCf;
   protected InVMNamingContext namingContext;

   protected MBeanServer mbeanServer;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = this.createServer(realStore, true);

      Configuration serverConfig = server.getConfiguration();

      serverConfig.getAddressesSettings().put("jms.queue.#", new AddressSettings().setAutoCreateJmsQueues(false).setDeadLetterAddress(new SimpleString("jms.queue.ActiveMQ.DLQ")));

      serverConfig.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));
      serverConfig.setSecurityEnabled(enableSecurity);

      extraServerConfig(serverConfig);

      if (enableSecurity) {
         ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
         securityManager.getConfiguration().addRole("openwireSender", "sender");
         securityManager.getConfiguration().addUser("openwireSender", "SeNdEr");
         //sender cannot receive
         Role senderRole = new Role("sender", true, false, false, false, true, true, false);

         securityManager.getConfiguration().addRole("openwireReceiver", "receiver");
         securityManager.getConfiguration().addUser("openwireReceiver", "ReCeIvEr");
         //receiver cannot send
         Role receiverRole = new Role("receiver", false, true, false, false, true, true, false);

         securityManager.getConfiguration().addRole("openwireGuest", "guest");
         securityManager.getConfiguration().addUser("openwireGuest", "GuEsT");

         //guest cannot do anything
         Role guestRole = new Role("guest", false, false, false, false, false, false, false);

         securityManager.getConfiguration().addRole("openwireDestinationManager", "manager");
         securityManager.getConfiguration().addUser("openwireDestinationManager", "DeStInAtIoN");

         //guest cannot do anything
         Role destRole = new Role("manager", false, false, false, false, true, true, false);

         Map<String, Set<Role>> settings = server.getConfiguration().getSecurityRoles();
         if (settings == null) {
            settings = new HashMap<String, Set<Role>>();
            server.getConfiguration().setSecurityRoles(settings);
         }
         Set<Role> anySet = settings.get("#");
         if (anySet == null) {
            anySet = new HashSet<Role>();
            settings.put("#", anySet);
         }
         anySet.add(senderRole);
         anySet.add(receiverRole);
         anySet.add(guestRole);
         anySet.add(destRole);
      }
      jmsServer = new JMSServerManagerImpl(server);
      namingContext = new InVMNamingContext();
      jmsServer.setRegistry(new JndiBindingRegistry(namingContext));
      jmsServer.start();

      registerConnectionFactory();

      mbeanServer = MBeanServerFactory.createMBeanServer();
      System.out.println("debug: server started");
   }

   //override this to add extra server configs
   protected void extraServerConfig(Configuration serverConfig) {
   }

   protected void registerConnectionFactory() throws Exception {
      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      createCF(connectorConfigs, "/cf");

      coreCf = (ConnectionFactory) namingContext.lookup("/cf");
   }

   protected void createCF(final List<TransportConfiguration> connectorConfigs,
                           final String... jndiBindings) throws Exception {
      final int retryInterval = 1000;
      final double retryIntervalMultiplier = 1.0;
      final int reconnectAttempts = -1;
      final int callTimeout = 30000;
      final boolean ha = false;
      List<String> connectorNames = registerConnectors(server, connectorConfigs);

      String cfName = name.getMethodName();
      if (cfName == null) {
         cfName = "cfOpenWire";
      }
      ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl().setName(cfName).setConnectorNames(connectorNames).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setCallTimeout(callTimeout).setReconnectAttempts(reconnectAttempts);
      jmsServer.createConnectionFactory(false, configuration, jndiBindings);
   }

   protected JMSServerControl getJMSServerControl() throws Exception {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      MBeanServerFactory.releaseMBeanServer(mbeanServer);
      mbeanServer = null;
      server.stop();
      super.tearDown();
   }

}
