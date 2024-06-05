/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.activemq.artemis.tests.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public class MQTTSessionExpiryIntervalTest extends MQTTTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   @Timeout(60)
   public void testCustomSessionExpiryInterval() throws Exception {
      final MQTT mqttSub = createMQTTConnection("MQTT-Sub-Client", false);

      BlockingConnection connectionSub = mqttSub.blockingConnection();
      connectionSub.connect();

      assertEquals(1, getSessions().size());

      Topic[] topics = {new Topic("TopicA", QoS.EXACTLY_ONCE)};
      connectionSub.subscribe(topics);
      connectionSub.disconnect();

      Wait.assertEquals(0, () -> getSessions().size(), 10000, 100);
   }

   @Override
   protected void addMQTTConnector() throws Exception {
      server.getConfiguration().addAcceptorConfiguration("MQTT", "tcp://localhost:" + port + "?protocols=MQTT;anycastPrefix=anycast:;multicastPrefix=multicast:;defaultMqttSessionExpiryInterval=3");

      logger.debug("Added MQTT connector to broker");
   }
}
