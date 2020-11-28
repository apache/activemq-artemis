/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.management;


import javax.management.NotCompliantMBeanException;

import org.apache.activemq.artemis.core.config.JMXConnectorConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ServiceComponent;
import org.apache.activemq.artemis.core.server.management.impl.HawtioSecurityControlImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public class ManagementContext implements ServiceComponent {

   private volatile boolean isStarted = false;
   private JMXAccessControlList accessControlList;
   private JMXConnectorConfiguration jmxConnectorConfiguration;
   private ManagementConnector mBeanServer;
   private ArtemisMBeanServerGuard guardHandler;
   private ActiveMQSecurityManager securityManager;

   public void init() {
      if (accessControlList != null) {
         //if we are configured then assume we want to use the guard so set the system property
         System.setProperty("javax.management.builder.initial", ArtemisMBeanServerBuilder.class.getCanonicalName());
         guardHandler = new ArtemisMBeanServerGuard();
         guardHandler.setJMXAccessControlList(accessControlList);
         ArtemisMBeanServerBuilder.setGuard(guardHandler);
      }
   }

   @Override
   public void start() throws Exception {
      if (isStarted) {
         return;
      }
      synchronized (this) {
         if (isStarted) {
            return;
         }
         isStarted = true;
         if (jmxConnectorConfiguration != null) {
            mBeanServer = new ManagementConnector(jmxConnectorConfiguration, securityManager);
            mBeanServer.start();
         }
      }
   }

   @Override
   public void stop() throws Exception {
      if (!isStarted) {
         return;
      }
      synchronized (this) {
         if (!isStarted) {
            return;
         }
         isStarted = false;
         if (mBeanServer != null) {
            mBeanServer.stop();
         }
      }
   }

   @Override
   public void stop(boolean shutdown) throws Exception {
      if (shutdown) {
         stop();
      }
   }

   @Override
   public boolean isStarted() {
      return isStarted;
   }

   public void setAccessControlList(JMXAccessControlList accessControlList) {
      this.accessControlList = accessControlList;
   }

   public JMXAccessControlList getAccessControlList() {
      return accessControlList;
   }

   public void setJmxConnectorConfiguration(JMXConnectorConfiguration jmxConnectorConfiguration) {
      this.jmxConnectorConfiguration = jmxConnectorConfiguration;
   }

   public JMXConnectorConfiguration getJmxConnectorConfiguration() {
      return jmxConnectorConfiguration;
   }

   public HawtioSecurityControl getSecurityMBean(StorageManager storageManager) {
      try {
         return new HawtioSecurityControlImpl(guardHandler, storageManager);
      } catch (NotCompliantMBeanException e) {
         e.printStackTrace();
         return null;
      }
   }

   public ArtemisMBeanServerGuard getArtemisMBeanServerGuard() {
      return guardHandler;
   }

   public void setSecurityManager(ActiveMQSecurityManager securityManager) {
      this.securityManager = securityManager;
   }

   public ActiveMQSecurityManager getSecurityManager() {
      return securityManager;
   }

   public ManagementConnector getManagementConnector() {
      return mBeanServer;
   }
}
