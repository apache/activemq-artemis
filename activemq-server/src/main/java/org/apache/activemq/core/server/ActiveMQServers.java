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
package org.apache.activemq.core.server;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManagerImpl;

/**
 * ActiveMQServers is a factory class for instantiating ActiveMQServer instances.
 * <p>
 * This class should be used when you want to instantiate a ActiveMQServer instance for embedding in
 * your own application, as opposed to directly instantiating an implementing instance.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class ActiveMQServers
{

   private ActiveMQServers()
   {
      // Utility class
   }

   public static ActiveMQServer newActiveMQServer(final Configuration config, final boolean enablePersistence)
   {
      ActiveMQSecurityManager securityManager = new ActiveMQSecurityManagerImpl();

      ActiveMQServer server = ActiveMQServers.newActiveMQServer(config,
                                                                ManagementFactory.getPlatformMBeanServer(),
                                                                securityManager,
                                                                enablePersistence);

      return server;
   }

   public static ActiveMQServer newActiveMQServer(final Configuration config)
   {
      return ActiveMQServers.newActiveMQServer(config, config.isPersistenceEnabled());
   }

   public static ActiveMQServer newActiveMQServer(final Configuration config,
                                                  final MBeanServer mbeanServer,
                                                  final boolean enablePersistence)
   {
      ActiveMQSecurityManager securityManager = new ActiveMQSecurityManagerImpl();

      ActiveMQServer server = ActiveMQServers.newActiveMQServer(config, mbeanServer, securityManager, enablePersistence);

      return server;
   }

   public static ActiveMQServer newActiveMQServer(final Configuration config, final MBeanServer mbeanServer)
   {
      return ActiveMQServers.newActiveMQServer(config, mbeanServer, true);
   }

   public static ActiveMQServer newActiveMQServer(final Configuration config,
                                                  final MBeanServer mbeanServer,
                                                  final ActiveMQSecurityManager securityManager)
   {
      ActiveMQServer server = ActiveMQServers.newActiveMQServer(config, mbeanServer, securityManager, true);

      return server;
   }

   public static ActiveMQServer newActiveMQServer(final Configuration config,
                                                  final MBeanServer mbeanServer,
                                                  final ActiveMQSecurityManager securityManager,
                                                  final boolean enablePersistence)
   {
      config.setPersistenceEnabled(enablePersistence);

      ActiveMQServer server = new ActiveMQServerImpl(config, mbeanServer, securityManager);

      return server;
   }

   public static ActiveMQServer newActiveMQServer(Configuration config,
                                                  String defUser, String defPass)
   {
      ActiveMQSecurityManager securityManager = new ActiveMQSecurityManagerImpl();

      securityManager.addUser(defUser, defPass);

      ActiveMQServer server = ActiveMQServers.newActiveMQServer(config,
                                                                ManagementFactory.getPlatformMBeanServer(),
                                                                securityManager,
                                                                config.isPersistenceEnabled());

      return server;
   }

   public static ActiveMQServer newActiveMQServer(final Configuration config,
                                                  final MBeanServer mbeanServer,
                                                  final boolean enablePersistence,
                                                  String user,
                                                  String password)
   {
      ActiveMQSecurityManager securityManager = new ActiveMQSecurityManagerImpl();

      securityManager.addUser(user, password);

      ActiveMQServer server = ActiveMQServers.newActiveMQServer(config, mbeanServer, securityManager, enablePersistence);

      return server;
   }

}
