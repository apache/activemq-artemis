/**
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
package org.apache.activemq.rest.test;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.jboss.resteasy.plugins.server.tjws.TJWSEmbeddedJaxrsServer;
import org.apache.activemq.rest.MessageServiceConfiguration;
import org.apache.activemq.rest.MessageServiceManager;
import org.jboss.resteasy.test.TestPortProvider;

public class Embedded
{
   protected MessageServiceManager manager = new MessageServiceManager();
   protected MessageServiceConfiguration config = new MessageServiceConfiguration();
   protected ActiveMQServer activeMQServer;
   protected TJWSEmbeddedJaxrsServer tjws = new TJWSEmbeddedJaxrsServer();

   public Embedded()
   {
      int port = TestPortProvider.getPort();
      System.out.println("default port is: " + port);
      tjws.setPort(port);
      tjws.setRootResourcePath("");
      tjws.setSecurityDomain(null);
   }

   public MessageServiceConfiguration getConfig()
   {
      return config;
   }

   public void setConfig(MessageServiceConfiguration config)
   {
      this.config = config;
   }

   public ActiveMQServer getActiveMQServer()
   {
      return activeMQServer;
   }

   public void setActiveMQServer(ActiveMQServer activeMQServer)
   {
      this.activeMQServer = activeMQServer;
   }

   public TJWSEmbeddedJaxrsServer getJaxrsServer()
   {
      return tjws;
   }

   public MessageServiceManager getManager()
   {
      return manager;
   }

   public void start() throws Exception
   {
      System.out.println("\nStarting Embedded");
      if (activeMQServer == null)
      {
         Configuration configuration = new ConfigurationImpl()
            .setPersistenceEnabled(false)
            .setSecurityEnabled(false)
            .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

         activeMQServer = ActiveMQServers.newActiveMQServer(configuration);
         activeMQServer.start();
      }
      tjws.start();
      manager.setConfiguration(config);
      manager.start();
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getQueueManager().getDestination());
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getTopicManager().getDestination());

   }

   public void stop() throws Exception
   {
      System.out.println("\nStopping Embedded");
      manager.stop();
      tjws.stop();
      activeMQServer.stop();
   }
}
