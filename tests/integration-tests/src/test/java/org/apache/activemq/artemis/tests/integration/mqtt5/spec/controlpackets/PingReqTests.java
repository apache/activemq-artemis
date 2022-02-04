/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.mqtt5.spec.controlpackets;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.mqtt.MqttMessageType;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTInterceptor;
import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.jboss.logging.Logger;
import org.junit.Test;

public class PingReqTests  extends MQTT5TestSupport {

   private static final Logger log = Logger.getLogger(PingReqTests.class);

   public PingReqTests(String protocol) {
      super(protocol);
   }

   /*
    * [MQTT-3.12.4-1] The Server MUST send a PINGRESP packet in response to a PINGREQ packet.
    */
   @Test(timeout = DEFAULT_TIMEOUT)
   public void testPingResp() throws Exception {
      final CountDownLatch latch = new CountDownLatch(4);

      MQTTInterceptor outgoingInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PINGRESP) {
            latch.countDown();
         }
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(outgoingInterceptor);

      MqttClient client = createPahoClient(RandomUtil.randomString());
      MqttConnectionOptions options = new MqttConnectionOptions();
      options.setKeepAliveInterval(1);
      client.connect(options);

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      client.disconnect();
   }
}
