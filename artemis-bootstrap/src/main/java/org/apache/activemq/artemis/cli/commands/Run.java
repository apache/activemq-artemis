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

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import org.apache.activemq.artemis.cli.ActiveMQ;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.dto.BrokerDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.factory.BrokerFactory;
import org.apache.activemq.artemis.factory.SecurityManagerFactory;
import org.apache.activemq.artemis.integration.Broker;
import org.apache.activemq.artemis.integration.bootstrap.ActiveMQBootstrapLogger;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

@Command(name = "run", description = "runs the broker instance")
public class Run implements Action
{

   @Arguments(description = "Broker Configuration URI, default 'xml:${ACTIVEMQ_INSTANCE}/etc/bootstrap.xml'")
   String configuration;
   private ArrayList<ActiveMQComponent> components = new ArrayList<>();

   private Broker server;

   static String fixupFileURI(String value)
   {
      if (value != null && value.startsWith("file:"))
      {
         value = value.substring("file:".length());
         value = new File(value).toURI().toString();
      }
      return value;
   }

   @Override
   public Object execute(ActionContext context) throws Exception
   {

      ActiveMQ.printBanner();

      /* We use File URI for locating files.  The ACTIVEMQ_HOME variable is used to determine file paths.  For Windows
      the ACTIVEMQ_HOME variable will include back slashes (An invalid file URI character path separator).  For this
      reason we overwrite the ACTIVEMQ_HOME variable with backslashes replaced with forward slashes. */
      String activemqInstance = System.getProperty("activemq.instance").replace("\\", "/");
      System.setProperty("activemq.instance", activemqInstance);

      if (configuration == null)
      {
         File xmlFile = new File(new File(new File(activemqInstance), "etc"), "bootstrap.xml");
         configuration = "xml:" + xmlFile.toURI().toString().substring("file:".length());
      }

      // To support Windows paths as explained above.
      System.out.println("Loading configuration file: " + configuration);

      BrokerDTO broker = BrokerFactory.createBrokerConfiguration(configuration);

      String fileName = new URI(fixupFileURI(broker.server.configuration)).getSchemeSpecificPart();

      addShutdownHook(new File(fileName).getParentFile());

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
         component.configure(componentDTO, activemqInstance);
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
