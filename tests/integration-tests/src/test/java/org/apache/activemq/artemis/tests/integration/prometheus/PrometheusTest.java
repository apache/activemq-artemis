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

package org.apache.activemq.artemis.tests.integration.prometheus;

import java.util.Enumeration;
import java.util.UUID;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class PrometheusTest extends ActiveMQTestBase {

   protected ActiveMQServer server;

   protected ClientSession session;

   protected ClientSessionFactory sf;

   protected ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false, createDefaultInVMConfig());
      server.getConfiguration().setJMXManagementEnabled(true);
      server.start();

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }

   @Test
   public void testExpectedMetricsArePresent() throws Exception {
      final String data = "Simple Text " + UUID.randomUUID().toString();
      final String queueName = "simpleQueue";
      final String addressName = "simpleAddress";

      session.createQueue(addressName, RoutingType.ANYCAST, queueName);
      ClientProducer producer = session.createProducer(addressName);
      ClientMessage message = session.createMessage(false);
      message.getBodyBuffer().writeString(data);
      producer.send(message);
      producer.close();
      ClientConsumer consumer = session.createConsumer(queueName);
      session.start();
      message = consumer.receive(1000);
      assertNotNull(message);
      message.acknowledge();
      session.commit();
      assertEquals(data, message.getBodyBuffer().readString());

      // Make sure metrics for the queue are available
      String[] sampleNames = {"artemis_messages_added", "artemis_messages_acknowledged"};
      Double[] sampleValues = {1.0, 1.0};
      boolean[] sampleFound = new boolean[2];
      int i = 0;
      Enumeration<Collector.MetricFamilySamples> allSamples = CollectorRegistry.defaultRegistry.metricFamilySamples();
      while (allSamples.hasMoreElements()) {
         Collector.MetricFamilySamples samples = allSamples.nextElement();
         for (int j = 0; j < sampleNames.length; j++) {
            if (samples.name.equals(sampleNames[j])) {
               IntegrationTestLogger.LOGGER.info(samples);
               for (Collector.MetricFamilySamples.Sample sample : samples.samples) {
                  if (sample.labelValues.contains(queueName)) {
                     assertEquals(sampleValues[j], sample.value, 0);
                     sampleFound[j] = true;
                  }
               }
            }
         }
         i++;
      }

      for (int j = 0; j < sampleFound.length; j++) {
         assertTrue(sampleNames[j] + " not found!", sampleFound[j]);
      }
   }
}
