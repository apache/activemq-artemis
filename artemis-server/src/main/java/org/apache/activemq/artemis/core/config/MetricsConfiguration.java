/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.config;

import java.io.Serializable;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.server.metrics.ActiveMQMetricsPlugin;

public class MetricsConfiguration implements Serializable {

   private boolean jvmMemory = ActiveMQDefaultConfiguration.getDefaultJvmMemoryMetrics();
   private boolean jvmGc = ActiveMQDefaultConfiguration.getDefaultJvmGcMetrics();
   private boolean jvmThread = ActiveMQDefaultConfiguration.getDefaultJvmThreadMetrics();
   private ActiveMQMetricsPlugin plugin;

   public boolean isJvmMemory() {
      return jvmMemory;
   }

   public MetricsConfiguration setJvmMemory(boolean jvmMemory) {
      this.jvmMemory = jvmMemory;
      return this;
   }

   public boolean isJvmGc() {
      return jvmGc;
   }

   public MetricsConfiguration setJvmGc(boolean jvmGc) {
      this.jvmGc = jvmGc;
      return this;
   }

   public boolean isJvmThread() {
      return jvmThread;
   }

   public MetricsConfiguration setJvmThread(boolean jvmThread) {
      this.jvmThread = jvmThread;
      return this;
   }

   public ActiveMQMetricsPlugin getPlugin() {
      return plugin;
   }

   public MetricsConfiguration setPlugin(ActiveMQMetricsPlugin plugin) {
      this.plugin = plugin;
      return this;
   }
}
