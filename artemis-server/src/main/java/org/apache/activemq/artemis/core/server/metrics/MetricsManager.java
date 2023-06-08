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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Gauge.Builder;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.logging.Log4j2Metrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.MetricsConfiguration;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.security.impl.SecurityStoreImpl;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final String brokerName;

   private final MeterRegistry meterRegistry;

   private final Map<String, List<Meter>> meters = new ConcurrentHashMap<>();

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   public MetricsManager(String brokerName,
                         MetricsConfiguration metricsConfiguration,
                         HierarchicalRepository<AddressSettings> addressSettingsRepository,
                         SecurityStore securityStore) {
      this.brokerName = brokerName;
      this.meterRegistry = metricsConfiguration.getPlugin().getRegistry();
      this.addressSettingsRepository = addressSettingsRepository;
      if (meterRegistry != null) {
         Metrics.globalRegistry.add(meterRegistry);
         meterRegistry.config().commonTags("broker", brokerName);
         if (metricsConfiguration.isJvmMemory()) {
            new JvmMemoryMetrics().bindTo(meterRegistry);
         }
         if (metricsConfiguration.isJvmGc()) {
            new JvmGcMetrics().bindTo(meterRegistry);
         }
         if (metricsConfiguration.isJvmThread()) {
            new JvmThreadMetrics().bindTo(meterRegistry);
         }
         if (metricsConfiguration.isNettyPool()) {
            new NettyPooledAllocatorMetrics(PooledByteBufAllocator.DEFAULT.metric()).bindTo(meterRegistry);
         }
         if (metricsConfiguration.isFileDescriptors()) {
            new FileDescriptorMetrics().bindTo(meterRegistry);
         }
         if (metricsConfiguration.isProcessor()) {
            new ProcessorMetrics().bindTo(meterRegistry);
         }
         if (metricsConfiguration.isUptime()) {
            new UptimeMetrics().bindTo(meterRegistry);
         }
         if (metricsConfiguration.isLogging()) {
            new Log4j2Metrics().bindTo(meterRegistry);
         }
         if (metricsConfiguration.isSecurityCaches() && securityStore.isSecurityEnabled()) {
            CaffeineCacheMetrics.monitor(meterRegistry, ((SecurityStoreImpl)securityStore).getAuthenticationCache(), "authentication");
            CaffeineCacheMetrics.monitor(meterRegistry, ((SecurityStoreImpl)securityStore).getAuthorizationCache(), "authorization");
         }
      }
   }

   public MeterRegistry getMeterRegistry() {
      return meterRegistry;
   }

   @FunctionalInterface
   public interface MetricGaugeBuilder {

      void build(String metricName, Object state, ToDoubleFunction<Object> f, String description, List<Tag> tags);
   }

   public void registerQueueGauge(String address, String queue, Consumer<MetricGaugeBuilder> builder) {
      if (this.meterRegistry == null || !addressSettingsRepository.getMatch(address).isEnableMetrics()) {
         return;
      }
      final List<Builder<Object>> gaugeBuilders = new ArrayList<>();
      builder.accept((metricName, state, f, description, tags) -> {
         Builder<Object> meter = Gauge
            .builder("artemis." + metricName, state, f)
            .tag("address", address)
            .tag("queue", queue)
            .tags(tags)
            .description(description);
         gaugeBuilders.add(meter);
      });
      registerMeters(gaugeBuilders, ResourceNames.QUEUE + queue);
   }

   public void registerAddressGauge(String address, Consumer<MetricGaugeBuilder> builder) {
      if (this.meterRegistry == null || !addressSettingsRepository.getMatch(address).isEnableMetrics()) {
         return;
      }
      final List<Builder<Object>> gaugeBuilders = new ArrayList<>();
      builder.accept((metricName, state, f, description, tags) -> {
         Builder<Object> meter = Gauge
            .builder("artemis." + metricName, state, f)
            .tag("address", address)
            .tags(tags)
            .description(description);
         gaugeBuilders.add(meter);
      });
      registerMeters(gaugeBuilders, ResourceNames.ADDRESS + address);
   }

   public void registerBrokerGauge(Consumer<MetricGaugeBuilder> builder) {
      if (this.meterRegistry == null) {
         return;
      }
      final List<Builder<Object>> gaugeBuilders = new ArrayList<>();
      builder.accept((metricName, state, f, description, tags) -> {
         Builder<Object> meter = Gauge
            .builder("artemis." + metricName, state, f)
            .tags(tags)
            .description(description);
         gaugeBuilders.add(meter);
      });
      registerMeters(gaugeBuilders, ResourceNames.BROKER + "." + brokerName);
   }

   private void registerMeters(List<Builder<Object>> gaugeBuilders, String resource) {
      if (meters.get(resource) != null) {
         throw ActiveMQMessageBundle.BUNDLE.metersAlreadyRegistered(resource);
      }
      logger.debug("Registering meters for {}", resource);
      List<Meter> newMeters = new ArrayList<>(gaugeBuilders.size());
      for (Builder<Object> gaugeBuilder : gaugeBuilders) {
         Gauge gauge = gaugeBuilder.register(meterRegistry);
         newMeters.add(gauge);
         logger.debug("Registered meter: {}", gauge.getId());
      }
      meters.put(resource, newMeters);
   }

   public void remove(String resource) {
      List<Meter> resourceMeters = meters.remove(resource);
      if (resourceMeters != null) {
         logger.debug("Unregistering meters for {}", resource);
         for (Meter meter : resourceMeters) {
            Meter removed = meterRegistry.remove(meter);
            if (removed != null) {
               logger.debug("Unregistered meter: {}", removed.getId());
            } else {
               logger.debug("Attempted to unregister meter {}, but it wasn't found in the registry", meter);
            }
         }
      } else {
         logger.debug("Attempted to unregister meters for {}, but none were found.", resource);
      }
   }
}
