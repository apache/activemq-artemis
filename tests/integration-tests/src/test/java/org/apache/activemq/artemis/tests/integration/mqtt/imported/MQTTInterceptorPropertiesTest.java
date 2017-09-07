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
package org.apache.activemq.artemis.tests.integration.mqtt.imported;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTInterceptor;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTSession;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.felix.resolver.util.ArrayMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class MQTTInterceptorPropertiesTest extends MQTTTestSupport {

   @Override
   @Before
   public void setUp() throws Exception {
      Field sessions = MQTTSession.class.getDeclaredField("SESSIONS");
      sessions.setAccessible(true);
      sessions.set(null, new ConcurrentHashMap<>());
      super.setUp();
   }

   private static final String ADDRESS = "address";
   private static final String MESSAGE_TEXT = "messageText";
   private static final String RETAINED = "retained";


   private boolean checkMessageProperties(MqttMessage message, Map<String, Object> expectedProperties) {
      System.out.println("Checking properties in interceptor");
      try {
         assertNotNull(message);
         assertNotNull(server.getNodeID());
         MqttFixedHeader header = message.fixedHeader();
         assertNotNull(header.messageType());
         assertEquals(header.qosLevel().value(), AT_MOST_ONCE);
         assertEquals(header.isRetain(), expectedProperties.get(RETAINED));
      } catch (Throwable t) {
         collector.addError(t);
      }
      return true;
   }

   @Rule
   public ErrorCollector collector = new ErrorCollector();

   @Test(timeout = 60000)
   public void testCheckInterceptedMQTTMessageProperties() throws Exception {
      final String addressQueue = name.getMethodName();
      final String msgText = "Test intercepted message";
      final boolean retained = true;

      Map<String, Object> expectedProperties = new ArrayMap<>();
      expectedProperties.put(ADDRESS, addressQueue);
      expectedProperties.put(MESSAGE_TEXT, msgText);
      expectedProperties.put(RETAINED, retained);


      final MQTTClientProvider subscribeProvider = getMQTTClientProvider();
      initializeConnection(subscribeProvider);

      subscribeProvider.subscribe(addressQueue, AT_MOST_ONCE);

      final CountDownLatch latch = new CountDownLatch(1);
      MQTTInterceptor incomingInterceptor = new MQTTInterceptor() {
         @Override
         public boolean intercept(MqttMessage packet, RemotingConnection connection) throws ActiveMQException {
            System.out.println("incoming");
            if (packet.getClass() == MqttPublishMessage.class) {
               return checkMessageProperties(packet, expectedProperties);
            } else {
               return true;
            }

         }
      };

      MQTTInterceptor outgoingInterceptor = new MQTTInterceptor() {
         @Override
         public boolean intercept(MqttMessage packet, RemotingConnection connection) throws ActiveMQException {
            System.out.println("outgoing");
            if (packet.getClass() == MqttPublishMessage.class) {
               return checkMessageProperties(packet, expectedProperties);
            } else {
               return true;
            }
         }
      };
      server.getRemotingService().addIncomingInterceptor(incomingInterceptor);
      server.getRemotingService().addOutgoingInterceptor(outgoingInterceptor);


      Thread thread = new Thread(new Runnable() {
         @Override
         public void run() {
            try {
               byte[] payload = subscribeProvider.receive(10000);
               assertNotNull("Should get a message", payload);
               latch.countDown();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      });
      thread.start();

      final MQTTClientProvider publishProvider = getMQTTClientProvider();
      initializeConnection(publishProvider);
      publishProvider.publish(addressQueue, msgText.getBytes(), AT_MOST_ONCE, retained);

      latch.await(10, TimeUnit.SECONDS);
      subscribeProvider.disconnect();
      publishProvider.disconnect();
   }
}
