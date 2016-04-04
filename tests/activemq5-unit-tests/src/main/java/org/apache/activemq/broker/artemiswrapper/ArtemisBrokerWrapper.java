/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.artemiswrapper;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

public class ArtemisBrokerWrapper extends ArtemisBrokerBase {

   protected final Map<String, SimpleString> testQueues = new HashMap<>();
   protected JMSServerManagerImpl jmsServer;
   protected MBeanServer mbeanServer;

   public ArtemisBrokerWrapper(BrokerService brokerService, File temporaryFolder) {
      super(temporaryFolder);
      this.bservice = brokerService;
   }

   @Override
   public void start() throws Exception {
      clearDataRecreateServerDirs();

      mbeanServer = MBeanServerFactory.createMBeanServer();

      server = createServer(realStore, true);
      server.setMBeanServer(mbeanServer);

      server.getConfiguration().getAcceptorConfigurations().clear();

      Configuration serverConfig = server.getConfiguration();
      serverConfig.setJMXManagementEnabled(true);

      Map<String, AddressSettings> addressSettingsMap = serverConfig.getAddressesSettings();

      //do policy translation
      PolicyMap policyMap = this.bservice.getDestinationPolicy();

      if (policyMap != null) {
         translatePolicyMap(serverConfig, policyMap);
      }

      String match = "jms.queue.#";
      AddressSettings commonSettings = addressSettingsMap.get(match);
      if (commonSettings == null) {
         commonSettings = new AddressSettings();
         addressSettingsMap.put(match, commonSettings);
      }
      SimpleString dla = new SimpleString("jms.queue.ActiveMQ.DLQ");
      commonSettings.setDeadLetterAddress(dla);
      commonSettings.setAutoCreateJmsQueues(true);

      if (bservice.extraConnectors.size() == 0) {
         serverConfig.addAcceptorConfiguration("home", "tcp://localhost:61616?protocols=OPENWIRE,CORE");
      }
      if (this.bservice.enableSsl()) {
         //default
         addServerAcceptor(serverConfig, new BrokerService.ConnectorInfo(61611, true));
      }

      for (BrokerService.ConnectorInfo info : bservice.extraConnectors) {
         addServerAcceptor(serverConfig, info);
      }

      serverConfig.setSecurityEnabled(enableSecurity);

      //extraServerConfig(serverConfig);

      if (enableSecurity) {
         ActiveMQJAASSecurityManager sm = (ActiveMQJAASSecurityManager) server.getSecurityManager();
         SecurityConfiguration securityConfig = sm.getConfiguration();
         securityConfig.addRole("openwireSender", "sender");
         securityConfig.addUser("openwireSender", "SeNdEr");
         //sender cannot receive
         Role senderRole = new Role("sender", true, false, false, false, true, true, false);

         securityConfig.addRole("openwireReceiver", "receiver");
         securityConfig.addUser("openwireReceiver", "ReCeIvEr");
         //receiver cannot send
         Role receiverRole = new Role("receiver", false, true, false, false, true, true, false);

         securityConfig.addRole("openwireGuest", "guest");
         securityConfig.addUser("openwireGuest", "GuEsT");

         //guest cannot do anything
         Role guestRole = new Role("guest", false, false, false, false, false, false, false);

         securityConfig.addRole("openwireDestinationManager", "manager");
         securityConfig.addUser("openwireDestinationManager", "DeStInAtIoN");

         //guest cannot do anything
         Role destRole = new Role("manager", false, false, false, false, true, true, false);

         Map<String, Set<Role>> settings = server.getConfiguration().getSecurityRoles();
         if (settings == null) {
            settings = new HashMap<>();
            server.getConfiguration().setSecurityRoles(settings);
         }
         Set<Role> anySet = settings.get("#");
         if (anySet == null) {
            anySet = new HashSet<>();
            settings.put("#", anySet);
         }
         anySet.add(senderRole);
         anySet.add(receiverRole);
         anySet.add(guestRole);
         anySet.add(destRole);
      }

      Set<TransportConfiguration> acceptors = serverConfig.getAcceptorConfigurations();
      Iterator<TransportConfiguration> iter = acceptors.iterator();
      while (iter.hasNext()) {
         System.out.println("acceptor =>: " + iter.next());
      }

      jmsServer = new JMSServerManagerImpl(server);
      InVMNamingContext namingContext = new InVMNamingContext();
      jmsServer.setRegistry(new JndiBindingRegistry(namingContext));
      jmsServer.start();

      server.start();

      stopped = false;

   }

   private void addServerAcceptor(Configuration serverConfig, BrokerService.ConnectorInfo info) throws Exception {
      if (info.ssl) {
         HashMap<String, Object> params = new HashMap<String, Object>();
         params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
         params.put(TransportConstants.PORT_PROP_NAME, info.port);
         params.put(TransportConstants.PROTOCOLS_PROP_NAME, "OPENWIRE");
         params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, bservice.SERVER_SIDE_KEYSTORE);
         params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, bservice.KEYSTORE_PASSWORD);
         params.put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, bservice.storeType);
         if (bservice.SERVER_SIDE_TRUSTSTORE != null) {
            params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, bservice.SERVER_SIDE_TRUSTSTORE);
            params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, bservice.TRUSTSTORE_PASSWORD);
            params.put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, bservice.storeType);
         }
         TransportConfiguration sslTransportConfig = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
         serverConfig.getAcceptorConfigurations().add(sslTransportConfig);
      }
      else {
         serverConfig.addAcceptorConfiguration("homePort" + info.port, "tcp://localhost:" + info.port + "?protocols=OPENWIRE,CORE");
      }
   }

   private void translatePolicyMap(Configuration serverConfig, PolicyMap policyMap) {
      List allEntries = policyMap.getAllEntries();
      Map<String, AddressSettings> settingsMap = serverConfig.getAddressesSettings();
      for (Object o : allEntries) {
         PolicyEntry entry = (PolicyEntry) o;
         org.apache.activemq.command.ActiveMQDestination targetDest = entry.getDestination();
         String match = getCorePattern(targetDest);
         AddressSettings settings = settingsMap.get(match);
         if (settings == null) {
            settings = new AddressSettings();
            settingsMap.put(match, settings);
         }

         if (entry.isAdvisoryForSlowConsumers()) {
            settings.setSlowConsumerThreshold(1000);
            settings.setSlowConsumerCheckPeriod(1);
            settings.setSlowConsumerPolicy(SlowConsumerPolicy.NOTIFY);
         }
         if (entry.isProducerFlowControl()) {
            settings.setMaxSizeBytes(10240000).setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
            if (bservice.getSystemUsage().isSendFailIfNoSpace()) {
               settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
            }
         }
         int queuePrefetch = entry.getQueuePrefetch();
         settings.setQueuePrefetch(queuePrefetch);
      }

      PolicyEntry defaultEntry = policyMap.getDefaultEntry();
      if (defaultEntry != null) {
         AddressSettings defSettings = settingsMap.get("#");
         if (defSettings == null) {
            defSettings = new AddressSettings();
            settingsMap.put("#", defSettings);
         }
         if (defaultEntry.isProducerFlowControl()) {
            defSettings.setMaxSizeBytes(10240000).setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
            if (bservice.getSystemUsage().isSendFailIfNoSpace()) {
               defSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
            }
         }
      }
   }

   private String getCorePattern(org.apache.activemq.command.ActiveMQDestination dest) {
      String physicalName = dest.getPhysicalName();
      String pattern = physicalName.replace(">", "#");
      if (dest.isTopic()) {
         pattern = "jms.topic." + pattern;
      }
      else {
         pattern = "jms.queue." + pattern;
      }

      return pattern;
   }

   @Override
   public void stop() throws Exception {
      try {
         server.stop();
         testQueues.clear();
         stopped = true;
      }
      catch (Throwable t) {
         //ignore
      }
      finally {
         server = null;
      }
   }

   public void makeSureQueueExists(String qname) throws Exception {
      synchronized (testQueues) {
         SimpleString coreQ = testQueues.get(qname);
         if (coreQ == null) {
            coreQ = new SimpleString("jms.queue." + qname);
            try {
               this.server.createQueue(coreQ, coreQ, null, false, false);
               testQueues.put(qname, coreQ);
            }
            catch (ActiveMQQueueExistsException e) {
               //ignore
            }
         }
      }
   }

   public long getAMQueueMessageCount(String physicalName) {
      long count = 0;
      String qname = "jms.queue." + physicalName;
      Binding binding = server.getPostOffice().getBinding(new SimpleString(qname));
      if (binding != null) {
         QueueImpl q = (QueueImpl) binding.getBindable();
         count = q.getMessageCount();
      }
      return count;
   }

   public MBeanServer getMbeanServer() {
      return this.mbeanServer;
   }
}
