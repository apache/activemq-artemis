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
package org.apache.activemq.artemis.maven;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Properties;

import org.apache.activemq.artemis.server.ActiveMQBootstrap;
import org.apache.activemq.artemis.server.SpawnedActiveMQBootstrap;
import org.apache.activemq.artemis.server.SpawnedVMSupport;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.codehaus.classworlds.ClassRealm;
import org.codehaus.classworlds.ClassWorld;

/**
 * @phase verify
 * @goal start
 */
public class ActiveMQStartPlugin extends AbstractMojo

{
   static final String SKIPBROKERSTART = "skipBrokerStart";

   /**
    * The plugin descriptor
    */
   private PluginDescriptor descriptor;


   /**
    * @parameter default-value=false
    */
   private Boolean waitOnStart;

   /**
    * @parameter
    */
   private String configurationDir;

   /**
    * @parameter
    */
   private String nodeId;

   /**
    * @parameter default-value=false;
    */
   private Boolean fork;

   /**
    * @parameter default-value=false
    */
   private Boolean debug;

   /**
    * @parameter
    */
   private Properties systemProperties;

   /**
    * @parameter default-value=STARTED::
    */
   private String serverStartString;

   /**
    * @parameter
    */
   private ActiveMQSecurityManager securityManager;

   /**
    * registers a TestClusterMBean for test clients to use.
    */
   private boolean testClusterManager;

   public void execute() throws MojoExecutionException, MojoFailureException
   {
      String property = System.getProperty(SKIPBROKERSTART);
      if (property != null)
      {
         getLog().info("skipping Broker Start");
         return;
      }
      if (testClusterManager)
      {
         try
         {
            createClusterManagerMBean();
         }
         catch (Exception e)
         {
            throw new MojoExecutionException("Failed to create cluster manager mbean", e);
         }
      }

      if (systemProperties != null && !systemProperties.isEmpty())
      {
         System.getProperties()
            .putAll(systemProperties);
      }

      String workingPath = new File(".").getAbsolutePath();

      try
      {
         registerNode(nodeId, workingPath, configurationDir);
      }
      catch (Exception e1)
      {
         throw new MojoExecutionException("Failed to create cluster manager mbean", e1);
      }

      if (fork)
      {
         try
         {
            PluginDescriptor pd = (PluginDescriptor) getPluginContext().get("pluginDescriptor");
            final Process p = SpawnedVMSupport.spawnVM(pd.getArtifacts(),
                                                       "ActiveMQServer_" + (nodeId != null ? nodeId : ""),
                                                       SpawnedActiveMQBootstrap.class.getName(),
                                                       systemProperties,
                                                       true,
                                                       serverStartString,
                                                       "FAILED::",
                                                       ".",
                                                       configurationDir,
                                                       debug,
                                                       configurationDir,
                                                       "" + waitOnStart,
                                                       nodeId);
            Runtime.getRuntime().addShutdownHook(new Thread()
            {
               @Override
               public void run()
               {
                  //just to be on the safe side
                  p.destroy();
               }
            });
            if (waitOnStart)
            {
               p.waitFor();
            }
         }
         catch (Throwable e)
         {
            e.printStackTrace();
            throw new MojoExecutionException(e.getMessage());
         }
      }
      else
      {
         ActiveMQBootstrap bootstrap = new ActiveMQBootstrap(configurationDir, waitOnStart, nodeId, securityManager);
         if (configurationDir != null)
         {
            extendPluginClasspath(configurationDir);
         }
         try
         {
            bootstrap.execute();
         }
         catch (Exception e)
         {
            throw new MojoExecutionException(e.getMessage(), e);
         }
      }
   }

   private void registerNode(String nodeId, String workingPath,
                             String hornetqConfigurationDir) throws Exception
   {
      TestClusterManagerMBean control = PluginUtil.getTestClusterManager();
      if (control != null)
      {
         control.registerNode(nodeId, workingPath, hornetqConfigurationDir);
      }
   }

   private void createClusterManagerMBean() throws Exception
   {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      ObjectName name = ObjectName.getInstance("hornetq:module=test,type=TestClusterManager");
      mbeanServer.registerMBean(new TestClusterManager(), name);
   }

   public void extendPluginClasspath(String element) throws MojoExecutionException
   {
      ClassWorld world = new ClassWorld();
      ClassRealm realm;
      try
      {
         realm = world.newRealm(
            "maven.plugin." + getClass().getSimpleName() + ((nodeId == null) ? "" : nodeId),
            Thread.currentThread()
               .getContextClassLoader()
         );
         File elementFile = new File(element);
         getLog().debug("Adding element to plugin classpath" + elementFile.getPath());
         realm.addConstituent(elementFile.toURI()
                                 .toURL());
      }
      catch (Exception ex)
      {
         throw new MojoExecutionException(ex.toString(), ex);
      }
      System.out.println(Arrays.toString(realm.getConstituents()));
      Thread.currentThread()
         .setContextClassLoader(realm.getClassLoader());
   }
}
