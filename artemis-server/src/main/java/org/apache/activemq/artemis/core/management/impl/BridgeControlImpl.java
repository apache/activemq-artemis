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
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.cluster.Bridge;

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
      clearIO();
      try {
         return configuration.getForwardingAddress();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getQueueName() {
      clearIO();
      try {
         return configuration.getQueueName();
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
   public String getFilterString() {
      clearIO();
      try {
         return configuration.getFilterString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getReconnectAttempts() {
      clearIO();
      try {
         return configuration.getReconnectAttempts();
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
   public double getRetryIntervalMultiplier() {
      clearIO();
      try {
         return configuration.getRetryIntervalMultiplier();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getTransformerClassName() {
      clearIO();
      try {
         return configuration.getTransformerConfiguration() == null ? null : configuration.getTransformerConfiguration().getClassName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getTransformerPropertiesAsJSON() {
      return JsonUtil.toJsonObject(getTransformerProperties()).toString();
   }

   @Override
   public Map<String, String> getTransformerProperties() {
      clearIO();
      try {
         return configuration.getTransformerConfiguration() == null ? null : configuration.getTransformerConfiguration().getProperties();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isStarted() {
      clearIO();
      try {
         return bridge.isStarted();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isUseDuplicateDetection() {
      clearIO();
      try {
         return configuration.isUseDuplicateDetection();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isHA() {
      clearIO();
      try {
         return configuration.isHA();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void start() throws Exception {
      clearIO();
      try {
         bridge.start();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void stop() throws Exception {
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

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
