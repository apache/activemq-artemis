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

import java.util.List;
import java.util.Map;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.management.ClusterConnectionControl;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.impl.BridgeMetrics;
import org.apache.activemq.artemis.logs.AuditLogger;

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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getAddress(this.clusterConnection);
      }
      clearIO();
      try {
         return configuration.getAddress();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getDiscoveryGroupName() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDiscoveryGroupName(this.clusterConnection);
      }
      clearIO();
      try {
         return configuration.getDiscoveryGroupName();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public int getMaxHops() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMaxHops(this.clusterConnection);
      }
      clearIO();
      try {
         return configuration.getMaxHops();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getName() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getName(this.clusterConnection);
      }
      clearIO();
      try {
         return configuration.getName();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public long getRetryInterval() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getRetryInterval(this.clusterConnection);
      }
      clearIO();
      try {
         return configuration.getRetryInterval();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getNodeID() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getNodeID(this.clusterConnection);
      }
      clearIO();
      try {
         return clusterConnection.getNodeID();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String[] getStaticConnectors() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getStaticConnectors(this.clusterConnection);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.getStaticConnectorsAsJSON(this.clusterConnection);
      }
      clearIO();
      try {
         return JsonUtil.toJsonArray(configuration.getStaticConnectors()).toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isDuplicateDetection() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isDuplicateDetection(this.clusterConnection);
      }
      clearIO();
      try {
         return configuration.isDuplicateDetection();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getMessageLoadBalancingType() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessageLoadBalancingType(this.clusterConnection);
      }
      clearIO();
      try {
         return configuration.getMessageLoadBalancingType().getType();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getTopology() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getTopology(this.clusterConnection);
      }
      clearIO();
      try {
         return clusterConnection.getTopology().describe();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, String> getNodes() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getNodes(this.clusterConnection);
      }
      clearIO();
      try {
         return clusterConnection.getNodes();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isStarted() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isStarted(this.clusterConnection);
      }
      clearIO();
      try {
         return clusterConnection.isStarted();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void start() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.startClusterConnection(this.clusterConnection);
      }
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
      if (AuditLogger.isEnabled()) {
         AuditLogger.stopClusterConnection(this.clusterConnection);
      }
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

   @Override
   public long getMessagesPendingAcknowledgement() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesPendingAcknowledgement(this.clusterConnection);
      }
      clearIO();
      try {
         return clusterConnection.getMetrics().getMessagesPendingAcknowledgement();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesAcknowledged() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesAcknowledged(this.clusterConnection);
      }
      clearIO();
      try {
         return clusterConnection.getMetrics().getMessagesAcknowledged();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object> getMetrics()  {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMetrics(this.clusterConnection);
      }
      clearIO();
      try {
         return clusterConnection.getMetrics().convertToMap();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object> getBridgeMetrics(String nodeId) {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getBridgeMetrics(this.clusterConnection, nodeId);
      }
      clearIO();
      try {
         final BridgeMetrics bridgeMetrics = clusterConnection.getBridgeMetrics(nodeId);
         return bridgeMetrics != null ? bridgeMetrics.convertToMap() : null;
      } finally {
         blockOnIO();
      }

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
