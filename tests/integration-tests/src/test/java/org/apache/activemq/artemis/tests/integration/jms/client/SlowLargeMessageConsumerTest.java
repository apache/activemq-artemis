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

package org.apache.activemq.artemis.tests.integration.jms.client;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

public class SlowLargeMessageConsumerTest extends JMSTestBase {

   private static final String TOPIC = "SlowLargeMessageConsumerTopic";


   @Override
   protected void extraServerConfig(ActiveMQServer server) {
      server.getConfiguration().getAddressSettings().put(TOPIC, new AddressSettings().setExpiryDelay(100L).setMaxSizeBytes(1024));
   }

   /**
     * @see <a href="https://issues.apache.org/jira/browse/ARTEMIS-4141">ARTEMIS-4141</a>
     */
   @Test
   public void ensureSlowConsumerOfLargeMessageNeverGetsStuck() throws Exception {
      try (Connection conn = cf.createConnection()) {
         conn.start();
         try (Session sessionConsumer = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
              Session sessionProducer = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
            final Destination topic = sessionConsumer.createTopic(TOPIC);
            final MessageConsumer consumer = sessionConsumer.createConsumer(topic);
            final AtomicBoolean slow = new AtomicBoolean(true);
            final CountDownLatch messageReceived = new CountDownLatch(1);
            consumer.setMessageListener(message -> {
               if (slow.get()) {
                  try {
                     TimeUnit.MILLISECONDS.sleep(50);
                  } catch (InterruptedException ex) {
                     Thread.currentThread().interrupt();
                  }
               } else {
                  messageReceived.countDown();
               }
            });
            final MessageProducer producer = sessionProducer.createProducer(topic);
            int msgSize = 512 * 1024;
            for (int i = 0; i < 100; i++) {
               producer.send(sessionProducer.createObjectMessage(RandomUtils.nextBytes(msgSize)));
               TimeUnit.MILLISECONDS.sleep(25);
            }
            TimeUnit.MILLISECONDS.sleep(100);
            slow.set(false);
            producer.send(sessionProducer.createObjectMessage(RandomUtils.nextBytes(msgSize)));
            assertTrue(messageReceived.await(500, TimeUnit.MILLISECONDS));
         }
      }
   }
}
