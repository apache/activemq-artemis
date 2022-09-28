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

package org.apache.activemq.artemis.tests.integration.mqtt5;

import java.util.Arrays;
import java.util.Collection;

import org.eclipse.paho.mqttv5.client.MqttClient;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class BasicSslTests extends MQTT5TestSupport {

   public BasicSslTests(String protocol) {
      super(protocol);
   }

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection<Object[]> getParams() {
      return Arrays.asList(new Object[][] {
         {SSL},
         {WSS}
      });
   }

   @Override
   public boolean isUseSsl() {
      return true;
   }

   /*
    * Basic SSL test. Just connect.
    */
   @Test(timeout = DEFAULT_TIMEOUT)
   public void testSsl() throws Exception {
      MqttClient client = createPahoClient("client");
      client.connect(getSslMqttConnectOptions());
   }
}
