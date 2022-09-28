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
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTInterceptor;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTReasonCodes;
import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.junit.Test;

/**
 * The broker doesn't send any "Reason String" or "User Property" in the PUBACK packet for any reason. Therefore, these are not tested here:
 *
 * [MQTT-3.4.2-2] The sender MUST NOT send this property if it would increase the size of the PUBACK packet beyond the Maximum Packet Size specified by the receiver.
 * [MQTT-3.4.2-3] The sender MUST NOT send this property if it would increase the size of the PUBACK packet beyond the Maximum Packet Size specified by the receiver.
 */

public class PubAckTests extends MQTT5TestSupport {

   public PubAckTests(String protocol) {
      super(protocol);
   }

   /*
    * [MQTT-3.4.2-1] The Client or Server sending the PUBACK packet MUST use one of the PUBACK Reason Codes.
    */
   @Test(timeout = DEFAULT_TIMEOUT)
   public void testPubAckReasonCode() throws Exception {
      final String TOPIC = RandomUtil.randomString();
      final CountDownLatch latch = new CountDownLatch(1);

      MQTTInterceptor outgoingInterceptor = (packet, connection) -> {
         if (packet.fixedHeader().messageType() == MqttMessageType.PUBACK) {
            assertEquals(MQTTReasonCodes.SUCCESS, ((MqttPubReplyMessageVariableHeader)packet.variableHeader()).reasonCode());
            latch.countDown();
         }
         return true;
      };
      server.getRemotingService().addOutgoingInterceptor(outgoingInterceptor);

      MqttClient publisher = createPahoClient("publisher");
      publisher.connect();
      publisher.publish(TOPIC, new byte[0], 1, false);

      assertTrue(latch.await(2, TimeUnit.SECONDS));

      publisher.disconnect();
      publisher.close();
   }
}