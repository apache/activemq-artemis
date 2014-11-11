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
package org.apache.activemq6.core.server;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.server.impl.HornetQServerImpl;
import org.apache.activemq6.spi.core.security.HornetQSecurityManager;
import org.apache.activemq6.spi.core.security.HornetQSecurityManagerImpl;

/**
 * HornetQServers is a factory class for instantiating HornetQServer instances.
 * <p>
 * This class should be used when you want to instantiate a HornetQServer instance for embedding in
 * your own application, as opposed to directly instantiating an implementing instance.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class HornetQServers
{

   private HornetQServers()
   {
      // Utility class
   }

   public static HornetQServer newHornetQServer(final Configuration config, final boolean enablePersistence)
   {
      HornetQSecurityManager securityManager = new HornetQSecurityManagerImpl();

      HornetQServer server = HornetQServers.newHornetQServer(config,
                                                      ManagementFactory.getPlatformMBeanServer(),
                                                      securityManager,
                                                      enablePersistence);

      return server;
   }

   public static HornetQServer newHornetQServer(final Configuration config)
   {
      return HornetQServers.newHornetQServer(config, config.isPersistenceEnabled());
   }

   public static HornetQServer newHornetQServer(final Configuration config,
                                                final MBeanServer mbeanServer,
                                                final boolean enablePersistence)
   {
      HornetQSecurityManager securityManager = new HornetQSecurityManagerImpl();

      HornetQServer server = HornetQServers.newHornetQServer(config, mbeanServer, securityManager, enablePersistence);

      return server;
   }

   public static HornetQServer newHornetQServer(final Configuration config, final MBeanServer mbeanServer)
   {
      return HornetQServers.newHornetQServer(config, mbeanServer, true);
   }

   public static HornetQServer newHornetQServer(final Configuration config,
                                                final MBeanServer mbeanServer,
                                                final HornetQSecurityManager securityManager)
   {
      HornetQServer server = HornetQServers.newHornetQServer(config, mbeanServer, securityManager, true);

      return server;
   }

   public static HornetQServer newHornetQServer(final Configuration config,
                                                final MBeanServer mbeanServer,
                                                final HornetQSecurityManager securityManager,
                                                final boolean enablePersistence)
   {
      config.setPersistenceEnabled(enablePersistence);

      HornetQServer server = new HornetQServerImpl(config, mbeanServer, securityManager);

      return server;
   }

   public static HornetQServer newHornetQServer(Configuration config,
         String defUser, String defPass)
   {
      HornetQSecurityManager securityManager = new HornetQSecurityManagerImpl();

      securityManager.addUser(defUser, defPass);

      HornetQServer server = HornetQServers.newHornetQServer(config,
                                                      ManagementFactory.getPlatformMBeanServer(),
                                                      securityManager,
                                                      config.isPersistenceEnabled());

      return server;
   }

   public static HornetQServer newHornetQServer(final Configuration config,
                                                final MBeanServer mbeanServer,
                                                final boolean enablePersistence,
                                                String user,
                                                String password)
   {
      HornetQSecurityManager securityManager = new HornetQSecurityManagerImpl();

      securityManager.addUser(user, password);

      HornetQServer server = HornetQServers.newHornetQServer(config, mbeanServer, securityManager, enablePersistence);

      return server;
   }

}
