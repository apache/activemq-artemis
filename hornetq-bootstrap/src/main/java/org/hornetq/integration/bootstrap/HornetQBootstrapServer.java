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
package org.hornetq.integration.bootstrap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.kernel.plugins.bootstrap.basic.BasicBootstrap;
import org.jboss.kernel.plugins.deployment.xml.BeanXMLDeployer;
import org.jboss.kernel.spi.config.KernelConfig;
import org.jboss.kernel.spi.deployment.KernelDeployment;

/**
 * This is the method in which the HornetQ server can be deployed externally outside of jBoss.
 * Alternatively a user can embed by using the same code as in main
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class HornetQBootstrapServer extends BasicBootstrap
{
   /**
    * The deployer
    */
   protected BeanXMLDeployer deployer;

   /**
    * The deployments
    */
   protected List<KernelDeployment> deployments = new CopyOnWriteArrayList<KernelDeployment>();

   /**
    * The arguments
    */
   private final String[] args;

   private Properties properties;

   /**
    * Bootstrap the kernel from the command line
    *
    * @param args the command line arguments
    * @throws Exception for any error
    */
   public static void main(final String[] args) throws Exception
   {
      HornetQBootstrapLogger.LOGGER.serverStarting();

      final HornetQBootstrapServer bootstrap = new HornetQBootstrapServer(args);

      bootstrap.run();

      bootstrap.addShutdownHook();
   }

   /**
    * Add a simple shutdown hook to stop the server.
    */
   private void addShutdownHook()
   {
      String dirName = System.getProperty("hornetq.config.dir", ".");
      final File file = new File(dirName + "/STOP_ME");
      if (file.exists())
      {
         if (!file.delete())
         {
            HornetQBootstrapLogger.LOGGER.errorDeletingFile(file.getAbsolutePath());
         }
      }
      final Timer timer = new Timer("HornetQ Server Shutdown Timer", true);
      timer.scheduleAtFixedRate(new TimerTask()
      {
         @Override
         public void run()
         {
            if (file.exists())
            {
               try
               {
                  shutDown();
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

   @Override
   public void run()
   {
      try
      {
         super.run();
      }
      catch (RuntimeException e)
      {
         HornetQBootstrapLogger.LOGGER.errorStartingServer(e);

         throw e;
      }
   }

   /**
    * JBoss 1.0.0 final
    * Standalone
    * Create a new bootstrap
    *
    * @param args the arguments
    * @throws Exception for any error
    */
   public HornetQBootstrapServer(final String... args) throws Exception
   {
      super();
      this.args = args;
   }

   public HornetQBootstrapServer(final KernelConfig kernelConfig, final String... args) throws Exception
   {
      super(kernelConfig);
      this.args = args;
   }

   @Override
   public void bootstrap() throws Throwable
   {
      super.bootstrap();
      deployer = new BeanXMLDeployer(getKernel());
      Runtime.getRuntime().addShutdownHook(new Shutdown());

      for (String arg : args)
      {
         deploy(arg);
      }

      deployer.validate();
   }

   /**
    * Undeploy a deployment
    *
    * @param deployment the deployment
    */
   public void undeploy(final KernelDeployment deployment) throws Throwable
   {
      HornetQBootstrapLogger.LOGGER.debug("Undeploying " + deployment.getName());
      deployments.remove(deployment);
      try
      {
         deployer.undeploy(deployment);
         HornetQBootstrapLogger.LOGGER.debug("Undeployed " + deployment.getName());
      }
      catch (Throwable t)
      {
         HornetQBootstrapLogger.LOGGER.errorDuringUndeployment(t, deployment.getName());
         HornetQBootstrapLogger.LOGGER.warn("Error during undeployment: " + deployment.getName(), t);
      }
   }

   public KernelDeployment deploy(final String arg) throws Throwable
   {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      URL url = cl.getResource(arg);
      if (url == null)
      {
         url = cl.getResource("META-INF/" + arg);
      }
      // try the system classpath
      if (url == null)
      {
         url = getClass().getClassLoader().getResource(arg);
      }
      if (url == null)
      {
         File file = new File(arg);
         if (file.exists())
         {
            url = file.toURI().toURL();
         }
      }
      if (url == null)
      {
         throw new RuntimeException("Unable to find resource:" + arg);
      }
      return deploy(url);
   }

   /**
    * Deploys a XML on the container
    */
   public KernelDeployment deploy(final String name, final String xml) throws Throwable
   {
      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
      PrintStream printOut = new PrintStream(byteOut);
      printOut.print(xml);
      printOut.flush();
      ByteArrayInputStream is = new ByteArrayInputStream(byteOut.toByteArray());

      KernelDeployment deployment = deployer.deploy(name, is);

      deployments.add(deployment);

      return deployment;
   }

   /**
    * Deploy a url
    *
    * @param url the deployment url
    * @throws Throwable for any error
    */
   private KernelDeployment deploy(final URL url) throws Throwable
   {
      HornetQBootstrapLogger.LOGGER.debug("Deploying " + url);
      KernelDeployment deployment = deployer.deploy(url);
      deployments.add(deployment);
      HornetQBootstrapLogger.LOGGER.debug("Deployed " + url);
      return deployment;
   }

   public void shutDown()
   {
      HornetQBootstrapLogger.LOGGER.serverStopping();

      ListIterator<KernelDeployment> iterator = deployments.listIterator(deployments.size());
      while (iterator.hasPrevious())
      {
         KernelDeployment deployment = iterator.previous();
         try
         {
            undeploy(deployment);
         }
         catch (Throwable t)
         {
            HornetQBootstrapLogger.LOGGER.errorDuringUndeployment(t, deployment.getName());
         }
      }
   }

   @Override
   protected Properties getConfigProperties()
   {
      return properties;
   }

   public void setProperties(final Properties props)
   {
      properties = props;
   }

   private final class Shutdown extends Thread
   {
      public Shutdown()
      {
         super("hornetq-shutdown-thread");
      }

      @Override
      public void run()
      {
         shutDown();
      }
   }
}
