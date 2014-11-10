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

package org.hornetq.jms.server.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.jnp.server.Main;
import org.jnp.server.NamingBeanImpl;

/**
 * This server class is only used in the standalone mode, its used to control the life cycle of the Naming Server to allow
 * it to be activated and deactivated
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         11/8/12
 */
public class StandaloneNamingServer implements HornetQComponent
{
   private Main jndiServer;

   private HornetQServer server;

   private NamingBeanImpl namingBean;

   private int port = 1099;

   private String bindAddress = "localhost";

   private int rmiPort = 1098;

   private String rmiBindAddress = "localhost";

   private ExecutorService executor;

   public StandaloneNamingServer(HornetQServer server)
   {
      this.server = server;
   }

   @Override
   public void start() throws Exception
   {
      server.registerActivateCallback(new ServerActivateCallback());
   }

   @Override
   public void stop() throws Exception
   {
   }

   @Override
   public boolean isStarted()
   {
      return false;
   }

   public void setPort(int port)
   {
      this.port = port;
   }

   public void setBindAddress(String bindAddress)
   {
      this.bindAddress = bindAddress;
   }

   public void setRmiPort(int rmiPort)
   {
      this.rmiPort = rmiPort;
   }

   public void setRmiBindAddress(String rmiBindAddress)
   {
      this.rmiBindAddress = rmiBindAddress;
   }

   private class ServerActivateCallback implements ActivateCallback
   {
      private boolean activated = false;

      @Override
      public synchronized void preActivate()
      {
         if (activated)
         {
            return;
         }
         try
         {
            jndiServer = new Main();
            namingBean = new NamingBeanImpl();
            jndiServer.setNamingInfo(namingBean);
            executor = Executors.newCachedThreadPool();
            jndiServer.setLookupExector(executor);
            jndiServer.setPort(port);
            jndiServer.setBindAddress(bindAddress);
            jndiServer.setRmiPort(rmiPort);
            jndiServer.setRmiBindAddress(rmiBindAddress);
            namingBean.start();
            jndiServer.start();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.unableToStartNamingServer(e);
         }

         activated = true;
      }

      @Override
      public void activated()
      {

      }

      @Override
      public synchronized void deActivate()
      {
         if (!activated)
         {
            return;
         }
         if (jndiServer != null)
         {
            try
            {
               jndiServer.stop();
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.unableToStopNamingServer(e);
            }
         }
         if (namingBean != null)
         {
            namingBean.stop();
         }
         if (executor != null)
         {
            executor.shutdown();
         }
         activated = false;
      }

      @Override
      public void activationComplete()
      {

      }
   }
}
