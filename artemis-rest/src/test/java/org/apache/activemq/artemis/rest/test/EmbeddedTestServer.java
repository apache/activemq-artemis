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
package org.apache.activemq.artemis.rest.test;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.rest.MessageServiceConfiguration;
import org.apache.activemq.artemis.rest.MessageServiceManager;
import org.jboss.logging.Logger;
import org.jboss.resteasy.plugins.server.tjws.TJWSEmbeddedJaxrsServer;
import org.jboss.resteasy.test.TestPortProvider;

class EmbeddedTestServer {
   private static final Logger log = Logger.getLogger(EmbeddedTestServer.class);

   protected MessageServiceManager manager = new MessageServiceManager(null);
   protected MessageServiceConfiguration config = new MessageServiceConfiguration();
   private ActiveMQServer activeMQServer;
   private TJWSEmbeddedJaxrsServer tjws = new TJWSEmbeddedJaxrsServer();

   EmbeddedTestServer() {
      int port = TestPortProvider.getPort();
      log.debug("default port is: " + port);
      tjws.setPort(port);
      tjws.setRootResourcePath("");
      tjws.setSecurityDomain(null);
   }

   public MessageServiceConfiguration getConfig() {
      return config;
   }

   public void setConfig(MessageServiceConfiguration config) {
      this.config = config;
   }

   public ActiveMQServer getActiveMQServer() {
      return activeMQServer;
   }

   TJWSEmbeddedJaxrsServer getJaxrsServer() {
      return tjws;
   }

   public MessageServiceManager getManager() {
      return manager;
   }

   public void start() throws Exception {
      log.debug("\nStarting EmbeddedTestServer");
      if (activeMQServer == null) {
         Configuration configuration = new ConfigurationImpl().setPersistenceEnabled(false).setSecurityEnabled(false).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

         activeMQServer = ActiveMQServers.newActiveMQServer(configuration);
         // set DLA and expiry to avoid spamming the log with warnings
         activeMQServer.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDeadLetterAddress(SimpleString.toSimpleString("DLA")).setExpiryAddress(SimpleString.toSimpleString("Expiry")));

         activeMQServer.start();
      }
      tjws.start();
      manager.setConfiguration(config);
      manager.start();
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getQueueManager().getDestination());
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getTopicManager().getDestination());

   }

   public void stop() throws Exception {
      log.debug("\nStopping EmbeddedTestServer");
      manager.stop();
      tjws.stop();
      activeMQServer.stop();
   }
}
