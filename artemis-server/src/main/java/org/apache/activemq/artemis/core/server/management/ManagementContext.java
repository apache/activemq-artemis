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


import org.apache.activemq.artemis.core.config.JMXConnectorConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;

public class ManagementContext implements ActiveMQComponent {
   private boolean isStarted = false;
   private JMXAccessControlList accessControlList;
   private JMXConnectorConfiguration jmxConnectorConfiguration;
   private ManagementConnector mBeanServer;

   @Override
   public void start() throws Exception {
      if (accessControlList != null) {
         //if we are configured then assume we want to use the guard so set the system property
         System.setProperty("javax.management.builder.initial", ArtemisMBeanServerBuilder.class.getCanonicalName());
         ArtemisMBeanServerGuard guardHandler = new ArtemisMBeanServerGuard();
         guardHandler.setJMXAccessControlList(accessControlList);
         ArtemisMBeanServerBuilder.setGuard(guardHandler);
      }

      if (jmxConnectorConfiguration != null) {
         mBeanServer = new ManagementConnector(jmxConnectorConfiguration);
         mBeanServer.start();
      }
      isStarted = true;
   }

   @Override
   public void stop() throws Exception {
      isStarted = false;
      if (mBeanServer != null) {
         mBeanServer.stop();
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
}
