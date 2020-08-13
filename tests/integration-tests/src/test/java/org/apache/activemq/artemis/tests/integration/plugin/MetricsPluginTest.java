/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.MetricsConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.metrics.plugins.SimpleMetricsPlugin;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.Matchers.containsInAnyOrder;

@RunWith(Parameterized.class)
public class MetricsPluginTest extends ActiveMQTestBase {

   private boolean legacyConfig;

   @Parameterized.Parameters(name = "legacyConfig={0}")
   public static Collection<Object[]> getParams() {
      return Arrays.asList(new Object[][]{{true}, {false}});
   }

   public MetricsPluginTest(boolean legacyConfig) {
      this.legacyConfig = legacyConfig;
   }

   protected ActiveMQServer server;
   protected ClientSession session;
   protected ClientSessionFactory sf;
   protected ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(false, createDefaultInVMConfig());
      if (legacyConfig) {
         server.getConfiguration().setMetricsPlugin(new SimpleMetricsPlugin().init(null));
      } else {
         server.getConfiguration().setMetricsConfiguration(new MetricsConfiguration().setPlugin(new SimpleMetricsPlugin().init(null)));
      }
      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }

   @Test
   public void testForArtemisMetricsPresence() throws Exception {
      class Metric {
         public final String name;
         public final String description;
         public final Double value;

         private Metric(String name, String description, Double value) {
            this.name = name;
            this.description = description;
            this.value = value;
         }

         @Override
         public String toString() {
            return name + ": " + value + " (" + description + ")";
         }

         @Override
         public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Metric metric = (Metric) o;
            return Objects.equals(name, metric.name) &&
                    Objects.equals(description, metric.description) &&
                    Objects.equals(value, metric.value);
         }

         @Override
         public int hashCode() {
            return Objects.hash(name, description, value);
         }
      }

      final String queueName = "simpleQueue";
      final String addressName = "simpleAddress";
      session.createQueue(new QueueConfiguration(queueName).setAddress(addressName).setRoutingType(RoutingType.ANYCAST));

      Map<Meter.Id, Double> metrics = getMetrics();
      List<Metric> artemisMetrics = metrics.entrySet().stream()
              .map(entry -> new Metric(
                      entry.getKey().getName(),
                      entry.getKey().getDescription(),
                      entry.getValue()))
              .filter(metric -> metric.name.startsWith("artemis"))
              .collect(Collectors.toList());

//      for (Metric metric : artemisMetrics) {
//          IntegrationTestLogger.LOGGER.info(metric);
//      }

      assertThat(artemisMetrics, containsInAnyOrder(
              // artemis.(un)routed.message.count is present twice, because of activemq.notifications address
              new Metric("artemis.address.memory.usage", "Memory used by all the addresses on broker for in-memory messages", 0.0),
              new Metric("artemis.address.memory.usage.percentage", "Memory used by all the addresses on broker as a percentage of the global-max-size", 0.0),
              new Metric("artemis.connection.count", "Number of clients connected to this server", 1.0),
              new Metric("artemis.consumer.count", "number of consumers consuming messages from this queue", 0.0),
              new Metric("artemis.delivering.durable.message.count", "number of durable messages that this queue is currently delivering to its consumers", 0.0),
              new Metric("artemis.delivering.durable.persistent.size", "persistent size of durable messages that this queue is currently delivering to its consumers", 0.0),
              new Metric("artemis.delivering.message.count", "number of messages that this queue is currently delivering to its consumers", 0.0),
              new Metric("artemis.delivering.persistent_size", "persistent size of messages that this queue is currently delivering to its consumers", 0.0),
              new Metric("artemis.disk.store.usage", "Percentage of total disk store used", 0.0),
              new Metric("artemis.durable.message.count", "number of durable messages currently in this queue (includes scheduled, paged, and in-delivery messages)", 0.0),
              new Metric("artemis.durable.persistent.size", "persistent size of durable messages currently in this queue (includes scheduled, paged, and in-delivery messages)", 0.0),
              new Metric("artemis.message.count", "number of messages currently in this queue (includes scheduled, paged, and in-delivery messages)", 0.0),
              new Metric("artemis.messages.acknowledged", "number of messages acknowledged from this queue since it was created", 0.0),
              new Metric("artemis.messages.added", "number of messages added to this queue since it was created", 0.0),
              new Metric("artemis.messages.expired", "number of messages expired from this queue since it was created", 0.0),
              new Metric("artemis.messages.killed", "number of messages removed from this queue since it was created due to exceeding the max delivery attempts", 0.0),
              new Metric("artemis.persistent.size", "persistent size of all messages (including durable and non-durable) currently in this queue (includes scheduled, paged, and in-delivery messages)", 0.0),
              new Metric("artemis.routed.message.count", "number of messages routed to one or more bindings", 0.0),
              new Metric("artemis.routed.message.count", "number of messages routed to one or more bindings", 0.0),
              new Metric("artemis.scheduled.durable.message.count", "number of durable scheduled messages in this queue", 0.0),
              new Metric("artemis.scheduled.durable.persistent.size", "persistent size of durable scheduled messages in this queue", 0.0),
              new Metric("artemis.scheduled.message.count", "number of scheduled messages in this queue", 0.0),
              new Metric("artemis.scheduled.persistent.size", "persistent size of scheduled messages in this queue", 0.0),
              new Metric("artemis.total.connection.count", "Number of clients which have connected to this server since it was started", 1.0),
              new Metric("artemis.unrouted.message.count", "number of messages not routed to any bindings", 0.0),
              new Metric("artemis.unrouted.message.count", "number of messages not routed to any bindings", 2.0),
              new Metric("artemis.address.size", "the number of estimated bytes being used by all the queue(s) bound to this address; used to control paging and blocking", 0.0),
              new Metric("artemis.address.size", "the number of estimated bytes being used by all the queue(s) bound to this address; used to control paging and blocking", 0.0),
              new Metric("artemis.number.of.pages", "number of pages used by this address", 0.0),
              new Metric("artemis.number.of.pages", "number of pages used by this address", 0.0)
      ));
   }

   @Test
   public void testForBasicMetricsPresenceAndValue() throws Exception {
      internalTestForBasicMetrics(true);
   }

   @Test
   public void testDisablingMetrics() throws Exception {
      internalTestForBasicMetrics(false);
   }

   private void internalTestForBasicMetrics(boolean enabled) throws Exception {
      final String data = "Simple Text " + UUID.randomUUID().toString();
      final String queueName = "simpleQueue";
      final String addressName = "simpleAddress";

      server.getAddressSettingsRepository().getMatch(addressName).setEnableMetrics(enabled);

      session.createQueue(new QueueConfiguration(queueName).setAddress(addressName).setRoutingType(RoutingType.ANYCAST));
      ClientProducer producer = session.createProducer(addressName);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString(data);
      producer.send(message);
      producer.close();

      Queue queue = server.locateQueue(queueName);
      Wait.assertEquals(1, queue::getMessageCount);

      Map<Meter.Id, Double> metrics = getMetrics();

      checkMetric(metrics, "artemis.message.count", "queue", queueName, 1.0, enabled);
      checkMetric(metrics, "artemis.messages.added", "queue", queueName, 1.0, enabled);
      checkMetric(metrics, "artemis.messages.acknowledged", "queue", queueName, 0.0, enabled);
      checkMetric(metrics, "artemis.durable.message.count", "queue", queueName, 1.0, enabled);
      checkMetric(metrics, "artemis.delivering.message.count", "queue", queueName, 0.0, enabled);
      checkMetric(metrics, "artemis.routed.message.count", "address", addressName, 1.0, enabled);
      checkMetric(metrics, "artemis.unrouted.message.count", "address", addressName, 0.0, enabled);
      checkMetric(metrics, "artemis.consumer.count", "queue", queueName, 0.0, enabled);

      ClientConsumer consumer = session.createConsumer(queueName);
      session.start();
      message = consumer.receive(1000);
      assertNotNull(message);

      metrics = getMetrics();
      checkMetric(metrics, "artemis.delivering.message.count", "queue", queueName, 1.0, enabled);
      checkMetric(metrics, "artemis.consumer.count", "queue", queueName, 1.0, enabled);

      message.acknowledge();
      assertEquals(data, message.getBodyBuffer().readString());
      session.commit(); // force the ack to be committed

      assertTrue(Wait.waitFor(() -> server.locateQueue(SimpleString.toSimpleString(queueName)).getMessagesAcknowledged() == 1, 1000, 100));

      consumer.close();

      metrics = getMetrics();

      checkMetric(metrics, "artemis.message.count", "queue", queueName, 0.0, enabled);
      checkMetric(metrics, "artemis.messages.added", "queue", queueName, 1.0, enabled);
      checkMetric(metrics, "artemis.messages.acknowledged", "queue", queueName, 1.0, enabled);
      checkMetric(metrics, "artemis.durable.message.count", "queue", queueName, 0.0, enabled);
      checkMetric(metrics, "artemis.delivering.message.count", "queue", queueName, 0.0, enabled);
      checkMetric(metrics, "artemis.routed.message.count", "address", addressName, 1.0, enabled);
      checkMetric(metrics, "artemis.unrouted.message.count", "address", addressName, 0.0, enabled);
      checkMetric(metrics, "artemis.consumer.count", "queue", queueName, 0.0, enabled);
   }

   @Test
   public void testMessageCountWithPaging() throws Exception {
      final String data = "Simple Text " + UUID.randomUUID().toString();
      final String queueName = "simpleQueue";
      final String addressName = "simpleAddress";

      server.getAddressSettingsRepository().getMatch(addressName).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setMaxSizeBytes(1024 * 10).setPageSizeBytes(1024 * 5);

      session.createQueue(new QueueConfiguration(queueName).setAddress(addressName).setRoutingType(RoutingType.ANYCAST));
      ClientProducer producer = session.createProducer(addressName);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString(data);
      long messageCount = 0;
      while (!server.getPagingManager().getPageStore(new SimpleString(addressName)).isPaging()) {
         producer.send(message);
         messageCount++;
      }

      Wait.assertEquals(messageCount, server.locateQueue(queueName)::getMessageCount, 2000, 100);
      checkMetric(getMetrics(), "artemis.message.count", "queue", queueName, Double.valueOf(messageCount));

      for (int i = 0; i < messageCount; i++) {
         producer.send(message);
      }
      producer.close();

      Wait.assertEquals(messageCount * 2, server.locateQueue(queueName)::getMessageCount, 2000, 100);
      checkMetric(getMetrics(), "artemis.message.count", "queue", queueName, Double.valueOf(messageCount * 2));
   }

   public Map<Meter.Id, Double> getMetrics() {
      return getMetrics(server);
   }

   public static Map<Meter.Id, Double> getMetrics(ActiveMQServer server) {
      Map<Meter.Id, Double> metrics = new HashMap<>();
      List<Meter> meters = server.getMetricsManager().getMeterRegistry().getMeters();
      assertTrue(meters.size() > 0);
      for (Meter meter : meters) {
         Iterable<Measurement> measurements = meter.measure();
         for (Measurement measurement : measurements) {
            metrics.put(meter.getId(), measurement.getValue());
         }
      }
      return metrics;
   }

   public void checkMetric(Map<Meter.Id, Double> metrics, String metric, String tag, String tagValue, Double expectedValue) {
      checkMetric(metrics, metric, tag, tagValue, expectedValue, true);
   }

   public void checkMetric(Map<Meter.Id, Double> metrics, String metric, String tag, String tagValue, Double expectedValue, boolean enabled) {
      OptionalDouble actualValue = metrics.entrySet().stream()
              .filter(entry -> metric.equals(entry.getKey().getName()))
              .filter(entry -> tagValue.equals(entry.getKey().getTag(tag)))
              .mapToDouble(Map.Entry::getValue)
              .findFirst();

      if (enabled) {
         assertTrue(metric + " for " + tag + " " + tagValue + " not present", actualValue.isPresent());
         assertEquals(metric + " not equal", expectedValue, actualValue.getAsDouble(), 0);
      } else {
         assertFalse(metric + " for " + tag + " " + tagValue + " present", actualValue.isPresent());
      }
   }
}
