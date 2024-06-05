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

package org.apache.activemq.artemis.core.protocol.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class StateSerDeTest {

   @Test
   @Timeout(30)
   public void testSerDe() throws Exception {
      for (int i = 0; i < 500; i++) {
         String clientId = RandomUtil.randomString();
         MQTTSessionState unserialized = new MQTTSessionState(clientId);
         Integer subscriptionIdentifier = RandomUtil.randomPositiveIntOrNull();
         for (int j = 0; j < RandomUtil.randomInterval(1, 50); j++) {
            MqttTopicSubscription sub = new MqttTopicSubscription(RandomUtil.randomString(),
                                                                  new MqttSubscriptionOption(MqttQoS.valueOf(RandomUtil.randomInterval(0, 3)),
                                                                                             RandomUtil.randomBoolean(),
                                                                                             RandomUtil.randomBoolean(),
                                                                                             MqttSubscriptionOption.RetainedHandlingPolicy.valueOf(RandomUtil.randomInterval(0, 3))));
            unserialized.addSubscription(sub, MQTTUtil.MQTT_WILDCARD, subscriptionIdentifier);
         }

         CoreMessage serializedState = MQTTStateManager.serializeState(unserialized, 0);
         MQTTSessionState deserialized = new MQTTSessionState(serializedState);

         assertEquals(unserialized.getClientId(), deserialized.getClientId());
         for (Pair<MqttTopicSubscription, Integer> unserializedEntry : unserialized.getSubscriptionsPlusID()) {
            MqttTopicSubscription unserializedSub = unserializedEntry.getA();
            Integer unserializedSubId = unserializedEntry.getB();
            Pair<MqttTopicSubscription, Integer> deserializedEntry = deserialized.getSubscriptionPlusID(unserializedSub.topicName());
            MqttTopicSubscription deserializedSub = deserializedEntry.getA();
            Integer deserializedSubId = deserializedEntry.getB();

            assertTrue(compareSubs(unserializedSub, deserializedSub));
            assertEquals(unserializedSubId, deserializedSubId);
         }
      }
   }

   private boolean compareSubs(MqttTopicSubscription a, MqttTopicSubscription b) {
      if (a == b) {
         return true;
      }
      if (a == null || b == null) {
         return false;
      }
      if (a.topicName() == null) {
         if (b.topicName() != null) {
            return false;
         }
      } else if (!a.topicName().equals(b.topicName())) {
         return false;
      }
      if (a.option() == null) {
         if (b.option() != null) {
            return false;
         }
      } else {
         if (a.option().qos() == null) {
            if (b.option().qos() != null) {
               return false;
            }
         } else if (a.option().qos().value() != b.option().qos().value()) {
            return false;
         }
         if (a.option().retainHandling() == null) {
            if (b.option().retainHandling() != null) {
               return false;
            }
         } else if (a.option().retainHandling().value() != b.option().retainHandling().value()) {
            return false;
         }
         if (a.option().isRetainAsPublished() != b.option().isRetainAsPublished()) {
            return false;
         }
         if (a.option().isNoLocal() != b.option().isNoLocal()) {
            return false;
         }
      }

      return true;
   }
}
