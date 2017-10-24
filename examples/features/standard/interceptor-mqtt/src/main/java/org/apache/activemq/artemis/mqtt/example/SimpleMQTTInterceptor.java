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
package org.apache.activemq.artemis.mqtt.example;

import java.nio.charset.Charset;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTInterceptor;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;


/**
 * A simple Interceptor implementation
 */
public class SimpleMQTTInterceptor implements MQTTInterceptor {

   @Override
   public boolean intercept(final MqttMessage mqttMessage, RemotingConnection connection) {
      System.out.println("MQTT control packet was intercepted " + mqttMessage.fixedHeader().messageType());

      // If you need to handle an specific packet type:
      if (mqttMessage instanceof MqttPublishMessage) {
         MqttPublishMessage message = (MqttPublishMessage) mqttMessage;


         String originalMessage = message.payload().toString(Charset.forName("UTF-8"));
         System.out.println("Original message: " + originalMessage);

         // The new message content must not be bigger that the original content.
         String modifiedMessage = "Modified message ";

         message.payload().setBytes(0, modifiedMessage.getBytes());
      } else {
         if (mqttMessage instanceof MqttConnectMessage) {
            MqttConnectMessage connectMessage = (MqttConnectMessage) mqttMessage;
            System.out.println("MQTT CONNECT control packet was intercepted " + connectMessage);
         }
      }


      // We return true which means "call next interceptor" (if there is one) or target.
      // If we returned false, it means "abort call" - no more interceptors would be called and neither would
      // the target
      return true;
   }

}
