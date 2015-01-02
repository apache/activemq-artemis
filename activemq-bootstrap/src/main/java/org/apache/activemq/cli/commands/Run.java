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
package org.apache.activemq.cli.commands;

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import org.apache.activemq.cli.ActiveMQ;
import org.apache.activemq.components.ExternalComponent;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.dto.BrokerDTO;
import org.apache.activemq.dto.ComponentDTO;
import org.apache.activemq.factory.BrokerFactory;
import org.apache.activemq.factory.SecurityManagerFactory;
import org.apache.activemq.integration.Broker;
import org.apache.activemq.integration.bootstrap.ActiveMQBootstrapLogger;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

@Command(name = "run", description = "runs the broker instance")
public class Run implements Action
{

   @Arguments(description = "Broker Configuration URI, default 'xml:${ACTIVEMQ_HOME}/config/non-clustered/bootstrap.xml'")
   String configuration;
   private ArrayList<ActiveMQComponent> components = new ArrayList<>();

   private Broker server;

   @Override
   public Object execute(ActionContext context) throws Exception
   {

      ActiveMQ.printBanner();

      String activemqHome = System.getProperty("activemq.home").replace("\\", "/");

      if (configuration == null)
      {
         configuration = "xml:" + activemqHome + "/config/non-clustered/bootstrap.xml";
      }

      System.out.println("Loading configuration file: " + configuration);

      BrokerDTO broker = BrokerFactory.createBrokerConfiguration(configuration);

      addShutdownHook(new File(broker.server.configuration).getParentFile());

      ActiveMQSecurityManager security = SecurityManagerFactory.create(broker.security);

      server = BrokerFactory.createServer(broker.server, security);

      server.start();

      if (broker.web != null)
      {
         broker.components.add(broker.web);
      }

      for (ComponentDTO componentDTO : broker.components)
      {
         Class clazz = this.getClass().getClassLoader().loadClass(componentDTO.componentClassName);
         ExternalComponent component = (ExternalComponent)clazz.newInstance();
         component.configure(componentDTO, activemqHome);
         component.start();
         components.add(component);
      }

      return null;
   }

   /**
    * Add a simple shutdown hook to stop the server.
    * @param configurationDir
    */
   private void addShutdownHook(File configurationDir)
   {
      final File file = new File(configurationDir,"STOP_ME");
      if (file.exists())
      {
         if (!file.delete())
         {
            ActiveMQBootstrapLogger.LOGGER.errorDeletingFile(file.getAbsolutePath());
         }
      }
      final Timer timer = new Timer("ActiveMQ Server Shutdown Timer", true);
      timer.scheduleAtFixedRate(new TimerTask()
      {
         @Override
         public void run()
         {
            if (file.exists())
            {
               try
               {
                  try
                  {
                     //TODO stop components
                     server.stop();
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
                  timer.cancel();
               }
               finally
               {
                  Runtime.getRuntime().exit(0);
               }
            }
         }
      }, 500, 500);
   }
}
