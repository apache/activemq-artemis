/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.openwire;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.ConnectionFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Before;

public class OpenWireTestBase extends ServiceTestBase
{
   public static final String OWHOST = "localhost";
   public static final int OWPORT = 61616;

   protected HornetQServer server;

   protected JMSServerManagerImpl jmsServer;
   protected boolean realStore = false;
   protected boolean enableSecurity = false;

   protected ConnectionFactory coreCf;
   protected InVMNamingContext namingContext;

   protected MBeanServer mbeanServer;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = this.createServer(realStore, true);
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PORT_PROP_NAME, "61616");
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "OPENWIRE");
      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      Configuration serverConfig = server.getConfiguration();

      Map<String, AddressSettings> addressSettings = serverConfig.getAddressesSettings();
      String match = "jms.queue.#";
      AddressSettings dlaSettings = new AddressSettings();
      SimpleString dla = new SimpleString("jms.queue.ActiveMQ.DLQ");
      dlaSettings.setDeadLetterAddress(dla);
      addressSettings.put(match, dlaSettings);

      serverConfig.getAcceptorConfigurations().add(transportConfiguration);
      serverConfig.setSecurityEnabled(enableSecurity);

      extraServerConfig(serverConfig);

      if (enableSecurity)
      {
         server.getSecurityManager().addRole("openwireSender", "sender");
         server.getSecurityManager().addUser("openwireSender", "SeNdEr");
         //sender cannot receive
         Role senderRole = new Role("sender", true, false, false, false, true, true, false);

         server.getSecurityManager().addRole("openwireReceiver", "receiver");
         server.getSecurityManager().addUser("openwireReceiver", "ReCeIvEr");
         //receiver cannot send
         Role receiverRole = new Role("receiver", false, true, false, false, true, true, false);

         server.getSecurityManager().addRole("openwireGuest", "guest");
         server.getSecurityManager().addUser("openwireGuest", "GuEsT");

         //guest cannot do anything
         Role guestRole = new Role("guest", false, false, false, false, false, false, false);

         server.getSecurityManager().addRole("openwireDestinationManager", "manager");
         server.getSecurityManager().addUser("openwireDestinationManager", "DeStInAtIoN");

         //guest cannot do anything
         Role destRole = new Role("manager", false, false, false, false, true, true, false);

         Map<String, Set<Role>> settings = server.getConfiguration().getSecurityRoles();
         if (settings == null)
         {
            settings = new HashMap<String, Set<Role>>();
            server.getConfiguration().setSecurityRoles(settings);
         }
         Set<Role> anySet = settings.get("#");
         if (anySet == null)
         {
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
      jmsServer.setContext(namingContext);
      jmsServer.start();

      registerConnectionFactory();

      mbeanServer = MBeanServerFactory.createMBeanServer();
      System.out.println("debug: server started");
   }

   //override this to add extra server configs
   protected void extraServerConfig(Configuration serverConfig)
   {
   }

   protected void registerConnectionFactory() throws Exception
   {
      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      createCF(connectorConfigs, "/cf");

      coreCf = (ConnectionFactory) namingContext.lookup("/cf");
   }

   protected void createCF(final List<TransportConfiguration> connectorConfigs, final String... jndiBindings) throws Exception
   {
      final int retryInterval = 1000;
      final double retryIntervalMultiplier = 1.0;
      final int reconnectAttempts = -1;
      final int callTimeout = 30000;
      final boolean ha = false;
      List<String> connectorNames = registerConnectors(server, connectorConfigs);

      String cfName = name.getMethodName();
      if (cfName == null)
      {
         cfName = "cfOpenWire";
      }
      ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl()
         .setName(cfName)
         .setConnectorNames(connectorNames)
         .setRetryInterval(retryInterval)
         .setRetryIntervalMultiplier(retryIntervalMultiplier)
         .setCallTimeout(callTimeout)
         .setReconnectAttempts(reconnectAttempts);
      jmsServer.createConnectionFactory(false, configuration, jndiBindings);
   }

   protected JMSServerControl getJMSServerControl() throws Exception
   {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      MBeanServerFactory.releaseMBeanServer(mbeanServer);
      mbeanServer = null;
      server.stop();
      super.tearDown();
   }

}
