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
package org.apache.activemq.artemis.cli.commands;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.tools.LockAbstract;
import org.apache.activemq.artemis.cli.factory.BrokerFactory;
import org.apache.activemq.artemis.cli.factory.jmx.ManagementFactory;
import org.apache.activemq.artemis.cli.factory.security.SecurityManagerFactory;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.core.server.management.ManagementContext;
import org.apache.activemq.artemis.dto.BrokerDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.dto.ManagementContextDTO;
import org.apache.activemq.artemis.integration.Broker;
import org.apache.activemq.artemis.integration.bootstrap.ActiveMQBootstrapLogger;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.ReusableLatch;

@Command(name = "run", description = "runs the broker instance")
public class Run extends LockAbstract {

   @Option(name = "--allow-kill", description = "This will allow the server to kill itself. Useful for tests (failover tests for instance)")
   boolean allowKill;

   private static boolean embedded = false;

   public static final ReusableLatch latchRunning = new ReusableLatch(0);

   private ManagementContext managementContext;

   /**
    * This will disable the System.exit at the end of the server.stop, as that means there are other things
    * happening on the same VM.
    *
    * @param embedded
    */
   public static void setEmbedded(boolean embedded) {
      Run.embedded = true;
   }

   private Broker server;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      ManagementContextDTO managementDTO = getManagementDTO();
      managementContext = ManagementFactory.create(managementDTO);

      Artemis.printBanner();

      BrokerDTO broker = getBrokerDTO();

      addShutdownHook(broker.server.getConfigurationFile().getParentFile());

      ActiveMQSecurityManager security = SecurityManagerFactory.create(broker.security);

      server = BrokerFactory.createServer(broker.server, security);

      managementContext.start();
      server.start();

      if (broker.web != null) {
         broker.components.add(broker.web);
      }

      for (ComponentDTO componentDTO : broker.components) {
         Class clazz = this.getClass().getClassLoader().loadClass(componentDTO.componentClassName);
         ExternalComponent component = (ExternalComponent) clazz.newInstance();
         component.configure(componentDTO, getBrokerInstance(), getBrokerHome());
         component.start();
         server.getServer().addExternalComponent(component);
      }
      return null;
   }

   /**
    * Add a simple shutdown hook to stop the server.
    *
    * @param configurationDir
    */
   private void addShutdownHook(File configurationDir) {

      latchRunning.countUp();
      final File file = new File(configurationDir, "STOP_ME");
      if (file.exists()) {
         if (!file.delete()) {
            ActiveMQBootstrapLogger.LOGGER.errorDeletingFile(file.getAbsolutePath());
         }
      }
      final File fileKill = new File(configurationDir, "KILL_ME");
      if (fileKill.exists()) {
         if (!fileKill.delete()) {
            ActiveMQBootstrapLogger.LOGGER.errorDeletingFile(fileKill.getAbsolutePath());
         }
      }

      final Timer timer = new Timer("ActiveMQ Artemis Server Shutdown Timer", true);
      timer.scheduleAtFixedRate(new TimerTask() {
         @Override
         public void run() {
            if (allowKill && fileKill.exists()) {
               try {
                  System.err.println("Halting by user request");
                  fileKill.delete();
               } catch (Throwable ignored) {
               }
               Runtime.getRuntime().halt(0);
            }
            if (file.exists()) {
               try {
                  stop();
                  timer.cancel();
               } finally {
                  System.out.println("Server stopped!");
                  System.out.flush();
                  latchRunning.countDown();
                  if (!embedded) {
                     Runtime.getRuntime().exit(0);
                  }
               }
            }
         }
      }, 500, 500);

      Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
         @Override
         public void run() {
            Run.this.stop();
         }
      });

   }

   protected void stop() {
      try {
         server.stop(true);
         if (managementContext != null) {
            managementContext.stop();
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
