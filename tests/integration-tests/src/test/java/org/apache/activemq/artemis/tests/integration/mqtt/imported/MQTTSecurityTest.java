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
package org.apache.activemq.artemis.tests.integration.mqtt.imported;

import java.io.EOFException;
import java.util.Arrays;

import org.apache.activemq.artemis.tests.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.junit.Test;

public class MQTTSecurityTest extends MQTTTestSupport {

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Test(timeout = 30000)
   public void testConnection() throws Exception {
      for (String version : Arrays.asList("3.1", "3.1.1")) {

         BlockingConnection connection = null;
         try {
            MQTT mqtt = createMQTTConnection("test-" + version, true);
            mqtt.setUserName(fullUser);
            mqtt.setPassword(fullPass);
            mqtt.setConnectAttemptsMax(1);
            mqtt.setVersion(version);
            connection = mqtt.blockingConnection();
            connection.connect();
            BlockingConnection finalConnection = connection;
            assertTrue("Should be connected", Wait.waitFor(() -> finalConnection.isConnected(), 5000, 100));
         } finally {
            if (connection != null && connection.isConnected()) connection.disconnect();
         }
      }
   }

   @Test(timeout = 30000, expected = EOFException.class)
   public void testConnectionWithNullPassword() throws Exception {
      for (String version : Arrays.asList("3.1", "3.1.1")) {

         BlockingConnection connection = null;
         try {
            MQTT mqtt = createMQTTConnection("test-" + version, true);
            mqtt.setUserName(fullUser);
            mqtt.setPassword((String) null);
            mqtt.setConnectAttemptsMax(1);
            mqtt.setVersion(version);
            connection = mqtt.blockingConnection();
            connection.connect();
            fail("Connect should fail");
         } finally {
            if (connection != null && connection.isConnected()) connection.disconnect();
         }
      }
   }
}
