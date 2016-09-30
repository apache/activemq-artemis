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
package org.apache.activemq.artemis.core.management.impl;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.management.ClusterConnectionControl;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;

public class ClusterConnectionControlImpl extends AbstractControl implements ClusterConnectionControl {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClusterConnection clusterConnection;

   private final ClusterConnectionConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClusterConnectionControlImpl(final ClusterConnection clusterConnection,
                                       final StorageManager storageManager,
                                       final ClusterConnectionConfiguration configuration) throws Exception {
      super(ClusterConnectionControl.class, storageManager);
      this.clusterConnection = clusterConnection;
      this.configuration = configuration;
   }

   // ClusterConnectionControlMBean implementation ---------------------------

   @Override
   public String getAddress() {
      clearIO();
      try {
         return configuration.getAddress();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getDiscoveryGroupName() {
      clearIO();
      try {
         return configuration.getDiscoveryGroupName();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public int getMaxHops() {
      clearIO();
      try {
         return configuration.getMaxHops();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getName() {
      clearIO();
      try {
         return configuration.getName();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public long getRetryInterval() {
      clearIO();
      try {
         return configuration.getRetryInterval();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getNodeID() {
      clearIO();
      try {
         return clusterConnection.getNodeID();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getStaticConnectors() {
      clearIO();
      try {
         List<String> staticConnectors = configuration.getStaticConnectors();
         if (staticConnectors == null) {
            return null;
         } else {
            return staticConnectors.toArray(new String[staticConnectors.size()]);
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getStaticConnectorsAsJSON() throws Exception {
      clearIO();
      try {
         return JsonUtil.toJsonArray(configuration.getStaticConnectors()).toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isDuplicateDetection() {
      clearIO();
      try {
         return configuration.isDuplicateDetection();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getMessageLoadBalancingType() {
      clearIO();
      try {
         return configuration.getMessageLoadBalancingType().getType();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getTopology() {
      clearIO();
      try {
         return clusterConnection.getTopology().describe();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, String> getNodes() throws Exception {
      clearIO();
      try {
         return clusterConnection.getNodes();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isStarted() {
      clearIO();
      try {
         return clusterConnection.isStarted();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void start() throws Exception {
      clearIO();
      try {
         clusterConnection.start();
         clusterConnection.flushExecutor();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void stop() throws Exception {
      clearIO();
      try {
         clusterConnection.stop();
         clusterConnection.flushExecutor();
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(ClusterConnectionControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(ClusterConnectionControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
