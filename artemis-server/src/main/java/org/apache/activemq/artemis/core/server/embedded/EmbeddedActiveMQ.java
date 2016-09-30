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
package org.apache.activemq.artemis.core.server.embedded;

import javax.management.MBeanServer;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

/**
 * Helper class to simplify bootstrap of ActiveMQ Artemis server.  Bootstraps from classpath-based config files.
 */
public class EmbeddedActiveMQ {

   protected ActiveMQSecurityManager securityManager;
   protected String configResourcePath = null;
   protected Configuration configuration;
   protected ActiveMQServer activeMQServer;
   protected MBeanServer mbeanServer;

   /**
    * Classpath resource for activemq server config.  Defaults to 'broker.xml'.
    *
    * @param filename
    */
   public EmbeddedActiveMQ setConfigResourcePath(String filename) {
      configResourcePath = filename;
      return this;
   }

   /**
    * Set the activemq security manager.  This defaults to org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl
    *
    * @param securityManager
    */
   public EmbeddedActiveMQ setSecurityManager(ActiveMQSecurityManager securityManager) {
      this.securityManager = securityManager;
      return this;
   }

   /**
    * It will iterate the cluster connections until you have at least the number of expected servers
    *
    * @param timeWait   Time to wait on each iteration
    * @param unit       unit of time to wait
    * @param iterations number of iterations
    * @param servers    number of minimal servers
    * @return
    */
   public boolean waitClusterForming(long timeWait, TimeUnit unit, int iterations, int servers) throws Exception {
      if (activeMQServer.getClusterManager().getClusterConnections() == null || activeMQServer.getClusterManager().getClusterConnections().size() == 0) {
         return servers == 0;
      }

      for (int i = 0; i < iterations; i++) {
         for (ClusterConnection connection : activeMQServer.getClusterManager().getClusterConnections()) {
            if (connection.getTopology().getMembers().size() == servers) {
               return true;
            }
            Thread.sleep(unit.toMillis(timeWait));
         }
      }

      return false;
   }

   /**
    * Use this mbean server to register management beans.  If not set, no mbeans will be registered.
    *
    * @param mbeanServer
    */
   public EmbeddedActiveMQ setMbeanServer(MBeanServer mbeanServer) {
      this.mbeanServer = mbeanServer;
      return this;
   }

   /**
    * Set this object if you are not using file-based configuration.  The default implementation will load
    * configuration from a file.
    *
    * @param configuration
    */
   public EmbeddedActiveMQ setConfiguration(Configuration configuration) {
      this.configuration = configuration;
      return this;
   }

   public ActiveMQServer getActiveMQServer() {
      return activeMQServer;
   }

   public EmbeddedActiveMQ start() throws Exception {
      initStart();
      activeMQServer.start();
      return this;
   }

   protected void initStart() throws Exception {
      if (configuration == null) {
         if (configResourcePath == null)
            configResourcePath = "broker.xml";
         FileDeploymentManager deploymentManager = new FileDeploymentManager(configResourcePath);
         FileConfiguration config = new FileConfiguration();
         deploymentManager.addDeployable(config);
         deploymentManager.readConfiguration();
         configuration = config;
      }
      if (securityManager == null) {
         securityManager = new ActiveMQJAASSecurityManager();
      }
      if (mbeanServer == null) {
         activeMQServer = new ActiveMQServerImpl(configuration, securityManager);
      } else {
         activeMQServer = new ActiveMQServerImpl(configuration, mbeanServer, securityManager);
      }
   }

   public EmbeddedActiveMQ stop() throws Exception {
      activeMQServer.stop();
      return this;
   }
}
