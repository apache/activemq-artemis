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
package org.apache.activemq6.core.deployers.impl;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.activemq6.api.core.Pair;
import org.apache.activemq6.core.deployers.Deployer;
import org.apache.activemq6.core.deployers.DeploymentManager;
import org.apache.activemq6.core.server.HornetQServerLogger;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class FileDeploymentManager implements Runnable, DeploymentManager
{
   private final List<Deployer> deployers = new ArrayList<Deployer>();

   private final Map<Pair<URI, Deployer>, DeployInfo> deployed = new HashMap<Pair<URI, Deployer>, DeployInfo>();

   private ScheduledExecutorService scheduler;

   private boolean started;

   private final long period;

   private ScheduledFuture<?> future;

   public FileDeploymentManager(final long period)
   {
      this.period = period;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      started = true;

      scheduler = Executors.newSingleThreadScheduledExecutor();

      future = scheduler.scheduleWithFixedDelay(this, period, period, TimeUnit.MILLISECONDS);
   }

   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }

      started = false;

      if (future != null)
      {
         future.cancel(false);

         future = null;
      }

      scheduler.shutdown();

      scheduler = null;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   /**
    * registers a Deployer object which will handle the deployment of URL's
    *
    * @param deployer The Deployer object
    * @throws Exception
    */
   public synchronized void registerDeployer(final Deployer deployer) throws Exception
   {
      if (!deployers.contains(deployer))
      {
         deployers.add(deployer);

         String[] filenames = deployer.getConfigFileNames();

         for (String filename : filenames)
         {
            HornetQServerLogger.LOGGER.debug("the filename is " + filename);

            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(filename);

            while (urls.hasMoreElements())
            {
               URI uri = urls.nextElement().toURI();

               HornetQServerLogger.LOGGER.debug("Got URI " + uri);

               try
               {
                  HornetQServerLogger.LOGGER.debug("Deploying " + uri + " for " + deployer.getClass().getSimpleName());
                  deployer.deploy(uri);
               }
               catch (Exception e)
               {
                  HornetQServerLogger.LOGGER.errorDeployingURI(e, uri);
               }

               Pair<URI, Deployer> pair = new Pair<URI, Deployer>(uri, deployer);

               deployed.put(pair, new DeployInfo(deployer, getFileFromURI(uri).lastModified()));
            }
         }
      }
   }

   @Override
   public synchronized void unregisterDeployer(final Deployer deployer) throws Exception
   {
      if (deployers.remove(deployer))
      {
         String[] filenames = deployer.getConfigFileNames();
         for (String filename : filenames)
         {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(filename);
            while (urls.hasMoreElements())
            {
               URI url = urls.nextElement().toURI();

               Pair<URI, Deployer> pair = new Pair<URI, Deployer>(url, deployer);

               deployed.remove(pair);
            }
         }
      }
   }

   private File getFileFromURI(final URI uri) throws MalformedURLException, UnsupportedEncodingException
   {
      return new File(URLDecoder.decode(uri.toURL().getFile(), "UTF-8"));
   }

   /**
    * called by the ExecutorService every n seconds
    */
   public synchronized void run()
   {
      if (!started)
      {
         return;
      }

      try
      {
         for (Deployer deployer : deployers)
         {
            String[] filenames = deployer.getConfigFileNames();

            for (String filename : filenames)
            {
               Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(filename);

               while (urls.hasMoreElements())
               {
                  URL url = urls.nextElement();
                  URI uri;
                  try
                  {
                     uri = url.toURI();
                  }
                  catch (URISyntaxException e)
                  {
                     HornetQServerLogger.LOGGER.errorDeployingURI(e);
                     continue;
                  }

                  Pair<URI, Deployer> pair = new Pair<URI, Deployer>(uri, deployer);

                  DeployInfo info = deployed.get(pair);

                  long newLastModified = getFileFromURI(uri).lastModified();

                  if (info == null)
                  {
                     try
                     {
                        deployer.deploy(uri);

                        deployed.put(pair, new DeployInfo(deployer, getFileFromURI(uri).lastModified()));
                     }
                     catch (Exception e)
                     {
                        HornetQServerLogger.LOGGER.errorDeployingURI(e, uri);
                     }
                  }
                  else if (newLastModified > info.lastModified)
                  {
                     try
                     {
                        deployer.redeploy(uri);

                        deployed.put(pair, new DeployInfo(deployer, getFileFromURI(uri).lastModified()));
                     }
                     catch (Exception e)
                     {
                        HornetQServerLogger.LOGGER.errorDeployingURI(e, uri);
                     }
                  }
               }
            }
         }
         List<Pair<URI, Deployer>> toRemove = new ArrayList<Pair<URI, Deployer>>();
         for (Map.Entry<Pair<URI, Deployer>, DeployInfo> entry : deployed.entrySet())
         {
            Pair<URI, Deployer> pair = entry.getKey();
            try
            {
               if (!fileExists(pair.getA()))
               {
                  Deployer deployer = entry.getValue().deployer;
                  HornetQServerLogger.LOGGER.debug("Undeploying " + deployer + " with url " + pair.getA());
                  deployer.undeploy(pair.getA());
                  toRemove.add(pair);
               }
            }
            catch (URISyntaxException e)
            {
               HornetQServerLogger.LOGGER.errorUnDeployingURI(e, pair.getA());
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorUnDeployingURI(e, pair.getA());
            }
         }
         for (Pair<URI, Deployer> pair : toRemove)
         {
            deployed.remove(pair);
         }
      }
      catch (IOException e)
      {
         HornetQServerLogger.LOGGER.errorScanningURLs(e);
      }
   }

   public synchronized List<Deployer> getDeployers()
   {
      return deployers;
   }

   public synchronized Map<Pair<URI, Deployer>, DeployInfo> getDeployed()
   {
      return deployed;
   }

   // Private -------------------------------------------------------

   /**
    * Checks if the URI is among the current thread context class loader's resources.
    * <p/>
    * We do not check that the corresponding file exists using File.exists() directly as it would
    * fail in the case the resource is loaded from inside an EAR file (see
    * https://jira.jboss.org/jira/browse/HORNETQ-122)
    *
    * @throws URISyntaxException
    */
   private boolean fileExists(final URI resourceURI) throws URISyntaxException
   {
      try
      {
         File f = getFileFromURI(resourceURI); // this was the original line, which doesn't work for
         // File-URLs with white spaces: File f = new
         // File(resourceURL.getPath());
         Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(f.getName());
         while (resources.hasMoreElements())
         {
            URI url = resources.nextElement().toURI();
            if (url.equals(resourceURI))
            {
               return true;
            }
         }
      }
      catch (IOException e)
      {
         return false;
      }
      return false;
   }

   // Inner classes -------------------------------------------------------------------------------------------

   public static class DeployInfo
   {
      public Deployer deployer;

      public long lastModified;

      DeployInfo(final Deployer deployer, final long lastModified)
      {
         this.deployer = deployer;
         this.lastModified = lastModified;
      }
   }
}
