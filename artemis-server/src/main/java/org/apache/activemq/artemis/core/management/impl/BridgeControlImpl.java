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
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.logs.AuditLogger;

public class BridgeControlImpl extends AbstractControl implements BridgeControl {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Bridge bridge;

   private final BridgeConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BridgeControlImpl(final Bridge bridge,
                            final StorageManager storageManager,
                            final BridgeConfiguration configuration) throws Exception {
      super(BridgeControl.class, storageManager);
      this.bridge = bridge;
      this.configuration = configuration;
   }

   // BridgeControlMBean implementation ---------------------------

   @Override
   public String[] getStaticConnectors() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getStaticConnectors(this.bridge);
      }
      clearIO();
      try {
         List<String> staticConnectors = configuration.getStaticConnectors();
         return staticConnectors.toArray(new String[staticConnectors.size()]);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getForwardingAddress() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getForwardingAddress(this.bridge);
      }
      clearIO();
      try {
         return configuration.getForwardingAddress();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getQueueName() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getQueueName(this.bridge);
      }
      clearIO();
      try {
         return configuration.getQueueName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getDiscoveryGroupName() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getDiscoveryGroupName(this.bridge);
      }
      clearIO();
      try {
         return configuration.getDiscoveryGroupName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getFilterString() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getFilterString(this.bridge);
      }
      clearIO();
      try {
         return configuration.getFilterString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getReconnectAttempts() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getReconnectAttempts(this.bridge);
      }
      clearIO();
      try {
         return configuration.getReconnectAttempts();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getName() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getName(this.bridge);
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
         AuditLogger.getRetryInterval(this.bridge);
      }
      clearIO();
      try {
         return configuration.getRetryInterval();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public double getRetryIntervalMultiplier() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getRetryIntervalMultiplier(this.bridge);
      }
      clearIO();
      try {
         return configuration.getRetryIntervalMultiplier();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getTransformerClassName() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getTransformerClassName(this.bridge);
      }
      clearIO();
      try {
         return configuration.getTransformerConfiguration() == null ? null : configuration.getTransformerConfiguration().getClassName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getTransformerPropertiesAsJSON() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getTransformerPropertiesAsJSON(this.bridge);
      }
      return JsonUtil.toJsonObject(getTransformerProperties()).toString();
   }

   @Override
   public Map<String, String> getTransformerProperties() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getTransformerProperties(this.bridge);
      }
      clearIO();
      try {
         return configuration.getTransformerConfiguration() == null ? null : configuration.getTransformerConfiguration().getProperties();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isStarted() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isStartedBridge(this.bridge);
      }
      clearIO();
      try {
         return bridge.isStarted();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isUseDuplicateDetection() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isUseDuplicateDetection(this.bridge);
      }
      clearIO();
      try {
         return configuration.isUseDuplicateDetection();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isHA() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.isHA(this.bridge);
      }
      clearIO();
      try {
         return configuration.isHA();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void start() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.startBridge(this.bridge);
      }
      clearIO();
      try {
         bridge.start();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void stop() throws Exception {
      if (AuditLogger.isEnabled()) {
         AuditLogger.stopBridge(this.bridge);
      }
      clearIO();
      try {
         bridge.stop();
         bridge.flushExecutor();
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(BridgeControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(BridgeControl.class);
   }

   @Override
   public long getMessagesPendingAcknowledgement() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesPendingAcknowledgement(this.bridge);
      }
      clearIO();
      try {
         return bridge.getMetrics().getMessagesPendingAcknowledgement();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesAcknowledged() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMessagesAcknowledged(this.bridge);
      }
      clearIO();
      try {
         return bridge.getMetrics().getMessagesAcknowledged();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object> getMetrics() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMetrics(this.bridge);
      }
      clearIO();
      try {
         return bridge.getMetrics().convertToMap();
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
