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

package org.apache.activemq.artemis.core.server.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.MetricsConfiguration;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.jboss.logging.Logger;

public class MetricsManager {

   private static final Logger log = Logger.getLogger(MetricsManager.class);

   private final String brokerName;

   private final MeterRegistry meterRegistry;

   private final Map<String, List<Meter>> meters = new ConcurrentHashMap<>();

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   public MetricsManager(String brokerName,
                         MetricsConfiguration metricsConfiguration,
                         HierarchicalRepository<AddressSettings> addressSettingsRepository) {
      this.brokerName = brokerName;
      meterRegistry = metricsConfiguration.getPlugin().getRegistry();
      Metrics.globalRegistry.add(meterRegistry);
      this.addressSettingsRepository = addressSettingsRepository;
      if (metricsConfiguration.isJvmMemory()) {
         new JvmMemoryMetrics().bindTo(meterRegistry);
      }
      if (metricsConfiguration.isJvmGc()) {
         new JvmGcMetrics().bindTo(meterRegistry);
      }
      if (metricsConfiguration.isJvmThread()) {
         new JvmThreadMetrics().bindTo(meterRegistry);
      }
   }

   public MeterRegistry getMeterRegistry() {
      return meterRegistry;
   }

   @FunctionalInterface
   public interface MetricGaugeBuilder {

      void register(String metricName, Object state, ToDoubleFunction f, String description);
   }

   public void registerQueueGauge(String address, String queue, Consumer<MetricGaugeBuilder> builder) {
      final MeterRegistry meterRegistry = this.meterRegistry;
      if (meterRegistry == null || !addressSettingsRepository.getMatch(address).isEnableMetrics()) {
         return;
      }
      final List<Gauge.Builder> newMeters = new ArrayList<>();
      builder.accept((metricName, state, f, description) -> {
         Gauge.Builder meter = Gauge
            .builder("artemis." + metricName, state, f)
            .tag("broker", brokerName)
            .tag("address", address)
            .tag("queue", queue)
            .description(description);
         newMeters.add(meter);
      });
      final String resource = ResourceNames.QUEUE + queue;
      registerMeter(newMeters, resource);
   }

   public void registerAddressGauge(String address, Consumer<MetricGaugeBuilder> builder) {
      final MeterRegistry meterRegistry = this.meterRegistry;
      if (meterRegistry == null || !addressSettingsRepository.getMatch(address).isEnableMetrics()) {
         return;
      }
      final List<Gauge.Builder> newMeters = new ArrayList<>();
      builder.accept((metricName, state, f, description) -> {
         Gauge.Builder meter = Gauge
            .builder("artemis." + metricName, state, f)
            .tag("broker", brokerName)
            .tag("address", address)
            .description(description);
         newMeters.add(meter);
      });
      final String resource = ResourceNames.ADDRESS + address;
      registerMeter(newMeters, resource);
   }

   public void registerBrokerGauge(Consumer<MetricGaugeBuilder> builder) {
      final MeterRegistry meterRegistry = this.meterRegistry;
      if (meterRegistry == null) {
         return;
      }
      final List<Gauge.Builder> newMeters = new ArrayList<>();
      builder.accept((metricName, state, f, description) -> {
         Gauge.Builder meter = Gauge
            .builder("artemis." + metricName, state, f)
            .tag("broker", brokerName)
            .description(description);
         newMeters.add(meter);
      });
      final String resource = ResourceNames.BROKER + "." + brokerName;
      registerMeter(newMeters, resource);
   }

   private void registerMeter(List<Gauge.Builder> newMeters, String resource) {
      this.meters.compute(resource, (s, meters) -> {
         //the old meters are ignored on purpose
         meters = new ArrayList<>(newMeters.size());
         for (Gauge.Builder gaugeBuilder : newMeters) {
            Gauge gauge = gaugeBuilder.register(meterRegistry);
            meters.add(gauge);
            if (log.isDebugEnabled()) {
               log.debug("Registered meter: " + gauge.getId());
            }
         }
         return meters;
      });
   }

   public void remove(String component) {
      meters.computeIfPresent(component, (s, meters) -> {
         if (meters == null) {
            return null;
         }
         for (Meter meter : meters) {
            Meter removed = meterRegistry.remove(meter);
            if (log.isDebugEnabled()) {
               log.debug("Unregistered meter: " + removed.getId());
            }
         }
         return null;
      });
   }
}
