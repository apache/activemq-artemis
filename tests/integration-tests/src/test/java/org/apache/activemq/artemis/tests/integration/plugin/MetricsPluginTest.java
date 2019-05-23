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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.metrics.plugins.SimpleMetricsPlugin;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class MetricsPluginTest extends ActiveMQTestBase {

   protected ActiveMQServer server;
   protected ClientSession session;
   protected ClientSessionFactory sf;
   protected ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(false, createDefaultInVMConfig());
      server.getConfiguration().setMetricsPlugin(new SimpleMetricsPlugin().init(null));
      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }

   @Test
   public void testForBasicMetricsPresenceAndValue() throws Exception {
      final String data = "Simple Text " + UUID.randomUUID().toString();
      final String queueName = "simpleQueue";
      final String addressName = "simpleAddress";

      session.createQueue(addressName, RoutingType.ANYCAST, queueName, null, true);
      ClientProducer producer = session.createProducer(addressName);
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString(data);
      producer.send(message);
      producer.close();

      Map<String, Double> metrics = getMetrics();

      checkMetric(metrics, "'artemis.message.count'", queueName, 1.0);
      checkMetric(metrics, "'artemis.messages.added'", queueName, 1.0);
      checkMetric(metrics, "'artemis.messages.acknowledged'", queueName, 0.0);
      checkMetric(metrics, "'artemis.durable.message.count'", queueName, 1.0);
      checkMetric(metrics, "'artemis.delivering.message.count'", queueName, 0.0);
      checkMetric(metrics, "'artemis.routed.message.count'", addressName, 1.0);
      checkMetric(metrics, "'artemis.unrouted.message.count'", addressName, 0.0);
      checkMetric(metrics, "'artemis.consumer.count'", queueName, 0.0);

      ClientConsumer consumer = session.createConsumer(queueName);
      session.start();
      message = consumer.receive(1000);
      assertNotNull(message);

      metrics = getMetrics();
      checkMetric(metrics, "'artemis.delivering.message.count'", queueName, 1.0);
      checkMetric(metrics, "'artemis.consumer.count'", queueName, 1.0);

      message.acknowledge();
      assertEquals(data, message.getBodyBuffer().readString());
      session.commit(); // force the ack to be committed

      assertTrue(Wait.waitFor(() -> server.locateQueue(SimpleString.toSimpleString(queueName)).getMessagesAcknowledged() == 1, 1000, 100));

      consumer.close();

      metrics = getMetrics();

      checkMetric(metrics, "'artemis.message.count'", queueName, 0.0);
      checkMetric(metrics, "'artemis.messages.added'", queueName, 1.0);
      checkMetric(metrics, "'artemis.messages.acknowledged'", queueName, 1.0);
      checkMetric(metrics, "'artemis.durable.message.count'", queueName, 0.0);
      checkMetric(metrics, "'artemis.delivering.message.count'", queueName, 0.0);
      checkMetric(metrics, "'artemis.routed.message.count'", addressName, 1.0);
      checkMetric(metrics, "'artemis.unrouted.message.count'", addressName, 0.0);
      checkMetric(metrics, "'artemis.consumer.count'", queueName, 0.0);
   }

   public Map<String, Double> getMetrics() {
      Map<String, Double> metrics = new HashMap<>();
      List<Meter> meters = server.getMetricsManager().getMeterRegistry().getMeters();
      assertTrue(meters.size() > 0);
      for (Meter meter : meters) {
         Iterable<Measurement> measurements = meter.measure();
         for (Measurement measurement : measurements) {
            metrics.put(meter.getId().toString(), measurement.getValue());
            // if (meter.getId().getName().startsWith("artemis")) {
            //    IntegrationTestLogger.LOGGER.info(meter.getId().toString() + ": " + measurement.getValue() + " (" + meter.getId().getDescription() + ")");
            // }
         }
      }
      return metrics;
   }

   public void checkMetric(Map<String, Double> metrics, String metric, String tag, Double value) {
      boolean contains = false;
      for (Map.Entry<String, Double> entry : metrics.entrySet()) {
         if (entry.getKey().contains(metric) && entry.getKey().contains(tag)) {
            contains = true;
            assertEquals(metric + " not equal", value, entry.getValue(), 0);
            break;
         }
      }
      assertTrue(contains);
   }
}
