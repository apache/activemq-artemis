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
package org.apache.activemq.server;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.FileDeploymentManager;
import org.apache.activemq.core.config.HAPolicyConfiguration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.config.impl.FileConfiguration;
import org.apache.activemq.core.config.impl.FileSecurityConfiguration;
import org.apache.activemq.core.config.impl.SecurityConfiguration;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.JournalType;
import org.apache.activemq.core.server.NodeManager;
import org.apache.activemq.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.core.server.impl.InVMNodeManager;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.jms.server.config.JMSConfiguration;
import org.apache.activemq.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.maven.InVMNodeManagerServer;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManagerImpl;

/**
 * This will bootstrap the HornetQ Server and also the naming server if required
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ActiveMQBootstrap
{
   private final String configurationDir;

   private final Boolean waitOnStart;

   private final String nodeId;

   private static Map<String, NodeManager> managerMap = new HashMap<String, NodeManager>();

   private boolean spawned = false;

   private ActiveMQServer server;

   private Configuration configuration;

   private JMSConfiguration jmsFileConfiguration;

   private SecurityConfiguration securityConfiguration;

   private JMSServerManager manager;

   private ActiveMQSecurityManager securityManager;


   public ActiveMQBootstrap(String configurationDir, Boolean waitOnStart, String nodeId, ActiveMQSecurityManager securityManager)
   {
      this.configurationDir = configurationDir;
      this.waitOnStart = waitOnStart;
      this.nodeId = nodeId;
      this.securityManager = securityManager;
   }

   public ActiveMQBootstrap(String[] args)
   {
      this.configurationDir = args[0];
      this.waitOnStart = Boolean.valueOf(args[1]);
      this.nodeId = args[2];
      spawned = true;
   }

   public void execute() throws Exception
   {
      try
      {
         if (configurationDir != null)
         {
            //extendPluginClasspath(configurationDir);
            configuration = new FileConfiguration();
            File file = new File(configurationDir + "/" + "activemq-configuration.xml");
            jmsFileConfiguration = new FileJMSConfiguration();
            FileDeploymentManager deploymentManager = new FileDeploymentManager(file.toURI().toURL().toExternalForm());
            deploymentManager.addDeployable((FileConfiguration)configuration);
            deploymentManager.addDeployable((FileJMSConfiguration) jmsFileConfiguration);

            securityConfiguration = new FileSecurityConfiguration("file://" + configurationDir + "/" + "activemq-users.properties",
                                                                  "file://" + configurationDir + "/" + "activemq-roles.properties",
                                                                  "guest",
                                                                  false,
                                                                  null);
            ((FileSecurityConfiguration)securityConfiguration).start();
            deploymentManager.readConfiguration();
         }
         else
         {
            configuration = new ConfigurationImpl();
            configuration.setJournalType(JournalType.NIO);
            jmsFileConfiguration = new JMSConfigurationImpl();
            securityConfiguration = new SecurityConfiguration();
         }

         createServer(configuration, jmsFileConfiguration);

         if (waitOnStart)
         {
            String dirName = System.getProperty("activemq.config.dir", ".");
            final File file = new File(dirName + "/STOP_ME");
            if (file.exists())
            {
               file.delete();
            }

            while (!file.exists())
            {
               Thread.sleep(500);
            }

            manager.stop();
            file.delete();
         }
         else
         {
            String dirName = configurationDir != null ? configurationDir : ".";
            final File stopFile = new File(dirName + "/STOP_ME");
            if (stopFile.exists())
            {
               stopFile.delete();
            }
            final File killFile = new File(dirName + "/KILL_ME");
            if (killFile.exists())
            {
               killFile.delete();
            }
            final File restartFile = new File(dirName + "/RESTART_ME");
            if (restartFile.exists())
            {
               restartFile.delete();
            }
            final Timer timer = new Timer("ActiveMQ Server Shutdown Timer", false);
            timer.scheduleAtFixedRate(new ServerStopTimerTask(stopFile, killFile, restartFile, timer), 500, 500);
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw new Exception(e.getMessage());
      }
   }

   private void createServer(Configuration configuration, JMSConfiguration jmsFileConfiguration) throws Exception
   {
      if (nodeId != null && !nodeId.equals("") && !nodeId.equals("null"))
      {
         InVMNodeManager nodeManager = (InVMNodeManager) managerMap.get(nodeId);
         if (nodeManager == null)
         {
            boolean replicatedBackup = configuration.getHAPolicyConfiguration().getType() == HAPolicyConfiguration.TYPE.REPLICA;
            nodeManager = new InVMNodeManager(replicatedBackup, configuration.getJournalDirectory());
            managerMap.put(nodeId, nodeManager);
         }
         server = new InVMNodeManagerServer(configuration, ManagementFactory.getPlatformMBeanServer(),
                                            securityManager != null ? securityManager : new ActiveMQSecurityManagerImpl(securityConfiguration), nodeManager);
      }
      else
      {
         server = new ActiveMQServerImpl(configuration, ManagementFactory.getPlatformMBeanServer(),
                                         securityManager != null ? securityManager : new ActiveMQSecurityManagerImpl(securityConfiguration));
      }

      manager = new JMSServerManagerImpl(server, jmsFileConfiguration);
      manager.start();
   }

   private class ServerStopTimerTask extends TimerTask
   {
      private final File stopFile;
      private final Timer timer;
      private final File killFile;
      private final File restartFile;

      public ServerStopTimerTask(File stopFile, File killFile, File restartFile, Timer timer)
      {
         this.stopFile = stopFile;
         this.killFile = killFile;
         this.restartFile = restartFile;
         this.timer = timer;
      }

      @Override
      public void run()
      {
         if (stopFile.exists())
         {
            try
            {
               timer.cancel();
            }
            finally
            {
               try
               {
                  if (manager != null)
                  {
                     manager.stop();
                     manager = null;
                  }
                  server = null;
                  stopFile.delete();
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
            if (spawned)
            {
               Runtime.getRuntime()
                  .halt(666);
            }
         }
         else if (killFile.exists())
         {
            try
            {
               manager.getActiveMQServer()
                  .stop(true);
               manager.stop();
               manager = null;
               server = null;
               killFile.delete();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
         else if (restartFile.exists())
         {
            try
            {
               createServer(configuration, jmsFileConfiguration);
               restartFile.delete();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      }
   }
}
