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

package org.apache.activemq.ra.recovery;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;

import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.server.recovery.ActiveMQRegistryBase;
import org.apache.activemq.jms.server.recovery.XARecoveryConfig;
import org.apache.activemq.ra.ActiveMQRALogger;
import org.apache.activemq.utils.ClassloadingUtil;
import org.apache.activemq.utils.ConcurrentHashSet;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         9/21/11
 */
public final class RecoveryManager
{
   private ActiveMQRegistryBase registry;

   private static final String RESOURCE_RECOVERY_CLASS_NAMES = "org.jboss.as.messaging.jms.AS7RecoveryRegistry;"
            + "org.jboss.as.integration.activemq.recovery.AS5RecoveryRegistry";

   private final Set<XARecoveryConfig> resources = new ConcurrentHashSet<XARecoveryConfig>();

   public void start(final boolean useAutoRecovery)
   {
      if (useAutoRecovery)
      {
         locateRecoveryRegistry();
      }
      else
      {
         registry = null;
      }
   }

   public XARecoveryConfig register(ActiveMQConnectionFactory factory, String userName, String password)
   {
      ActiveMQRALogger.LOGGER.debug("registering recovery for factory : " + factory);

      XARecoveryConfig config = XARecoveryConfig.newConfig(factory, userName, password);
      resources.add(config);
      if (registry != null)
      {
         registry.register(config);
      }
      return config;
   }


   public void unRegister(XARecoveryConfig resourceRecovery)
   {
      if (registry != null)
      {
         registry.unRegister(resourceRecovery);
      }
   }

   public void stop()
   {
      if (registry != null)
      {
         for (XARecoveryConfig recovery : resources)
         {
            registry.unRegister(recovery);
         }
         registry.stop();
      }


      resources.clear();
   }

   private void locateRecoveryRegistry()
   {
      String[] locatorClasses = RESOURCE_RECOVERY_CLASS_NAMES.split(";");

      for (String locatorClasse : locatorClasses)
      {
         try
         {
            registry = (ActiveMQRegistryBase) safeInitNewInstance(locatorClasse);
         }
         catch (Throwable e)
         {
            ActiveMQRALogger.LOGGER.debug("unable to load  recovery registry " + locatorClasse, e);
         }
         if (registry != null)
         {
            break;
         }
      }

      if (registry != null)
      {
         ActiveMQRALogger.LOGGER.debug("Recovery Registry located = " + registry);
      }
   }

   /** This seems duplicate code all over the place, but for security reasons we can't let something like this to be open in a
    *  utility class, as it would be a door to load anything you like in a safe VM.
    *  For that reason any class trying to do a privileged block should do with the AccessController directly.
    */
   private static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            return ClassloadingUtil.newInstanceFromClassLoader(className);
         }
      });
   }

   public Set<XARecoveryConfig> getResources()
   {
      return resources;
   }
}
