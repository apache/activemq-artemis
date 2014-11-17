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
package org.apache.activemq.core.server.embedded;

import javax.management.MBeanServer;

import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.FileConfiguration;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.impl.HornetQServerImpl;
import org.apache.activemq.spi.core.security.HornetQSecurityManager;
import org.apache.activemq.spi.core.security.HornetQSecurityManagerImpl;

/**
 * Helper class to simplify bootstrap of HornetQ server.  Bootstraps from classpath-based config files.
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class EmbeddedHornetQ
{
   protected HornetQSecurityManager securityManager;
   protected String configResourcePath = null;
   protected Configuration configuration;
   protected HornetQServer hornetQServer;
   protected MBeanServer mbeanServer;

   /**
    * Classpath resource for hornetq server config.  Defaults to 'hornetq-configuration.xml'.
    *
    * @param filename
    */
   public void setConfigResourcePath(String filename)
   {
      configResourcePath = filename;
   }

   /**
    * Set the hornetq security manager.  This defaults to org.apache.activemq.spi.core.security.HornetQSecurityManagerImpl
    *
    * @param securityManager
    */
   public void setSecurityManager(HornetQSecurityManager securityManager)
   {
      this.securityManager = securityManager;
   }

   /**
    * Use this mbean server to register management beans.  If not set, no mbeans will be registered.
    *
    * @param mbeanServer
    */
   public void setMbeanServer(MBeanServer mbeanServer)
   {
      this.mbeanServer = mbeanServer;
   }

   /**
    * Set this object if you are not using file-based configuration.  The default implementation will load
    * configuration from a file.
    *
    * @param configuration
    */
   public void setConfiguration(Configuration configuration)
   {
      this.configuration = configuration;
   }

   public HornetQServer getHornetQServer()
   {
      return hornetQServer;
   }

   public void start() throws Exception
   {
      initStart();
      hornetQServer.start();

   }

   protected void initStart() throws Exception
   {
      if (configuration == null)
      {
         if (configResourcePath == null) configResourcePath = "hornetq-configuration.xml";
         FileConfiguration config = new FileConfiguration(configResourcePath);
         config.start();
         configuration = config;
      }
      if (securityManager == null)
      {
         securityManager = new HornetQSecurityManagerImpl();
      }
      if (mbeanServer == null)
      {
         hornetQServer = new HornetQServerImpl(configuration, securityManager);
      }
      else
      {
         hornetQServer = new HornetQServerImpl(configuration, mbeanServer, securityManager);
      }
   }

   public void stop() throws Exception
   {
      hornetQServer.stop();
   }
}
