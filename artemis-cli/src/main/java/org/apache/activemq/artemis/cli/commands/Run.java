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
package org.apache.activemq.artemis.cli.commands;

import java.io.File;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import io.airlift.airline.Command;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.dto.BrokerDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.factory.BrokerFactory;
import org.apache.activemq.artemis.factory.SecurityManagerFactory;
import org.apache.activemq.artemis.integration.Broker;
import org.apache.activemq.artemis.integration.bootstrap.ActiveMQBootstrapLogger;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

@Command(name = "run", description = "runs the broker instance")
public class Run extends Configurable implements Action
{

   private Broker server;

   private ArrayList<ActiveMQComponent> components = new ArrayList<>();

   @Override
   public Object execute(ActionContext context) throws Exception
   {

      Artemis.printBanner();

      BrokerDTO broker = getBrokerDTO();

      addShutdownHook(broker.server.getConfigurationFile().getParentFile());

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
         component.configure(componentDTO, getBrokerInstance(), getBrokerHome());
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
      final Timer timer = new Timer("ActiveMQ Artemis Server Shutdown Timer", true);
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
