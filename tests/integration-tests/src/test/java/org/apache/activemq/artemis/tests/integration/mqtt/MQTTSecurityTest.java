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

import static org.apache.activemq.artemis.core.protocol.mqtt.MQTTProtocolManagerFactory.MQTT_PROTOCOL_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.EOFException;
import java.util.Arrays;

import org.apache.activemq.artemis.tests.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.MQTTException;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.CONNACK;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class MQTTSecurityTest extends MQTTTestSupport {

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Test
   @Timeout(30)
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
            assertTrue(Wait.waitFor(() -> finalConnection.isConnected(), 5000, 100), "Should be connected");
         } finally {
            if (connection != null && connection.isConnected()) connection.disconnect();
         }
      }
   }

   @Test
   @Timeout(30)
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
         } catch (MQTTException e) {
            assertEquals(CONNACK.Code.CONNECTION_REFUSED_NOT_AUTHORIZED, e.connack.code());
         } catch (Exception e) {
            fail("Should have caught an MQTTException");
         } finally {
            if (connection != null && connection.isConnected())
               connection.disconnect();
         }
      }
   }

   @Test
   @Timeout(30)
   public void testPublishAuthorizationFailOn311WithDisconnect() throws Exception {
      String version = "3.1.1";

      BlockingConnection connection = null;
      try {
         MQTT mqtt = createMQTTConnection("test-" + version, true);
         mqtt.setUserName(noprivUser);
         mqtt.setPassword(noprivPass);
         mqtt.setConnectAttemptsMax(1);
         mqtt.setVersion(version);
         connection = mqtt.blockingConnection();
         connection.connect();
         connection.publish("foo", new byte[0], QoS.EXACTLY_ONCE, false);
         fail("Should have triggered an exception");
      } catch (EOFException e) {
         // OK
      } catch (Exception e) {
         e.printStackTrace();
         fail("Should not have caught an Exception");
      } finally {
         if (connection != null && connection.isConnected())
            connection.disconnect();
      }
   }

   @Test
   @Timeout(30)
   public void testPublishAuthorizationFailOn311WithoutDisconnect() throws Exception {
      setAcceptorProperty("closeMqttConnectionOnPublishAuthorizationFailure=false");
      String version = "3.1.1";

      BlockingConnection connection = null;
      try {
         MQTT mqtt = createMQTTConnection("test-" + version, true);
         mqtt.setUserName(noprivUser);
         mqtt.setPassword(noprivPass);
         mqtt.setConnectAttemptsMax(1);
         mqtt.setVersion(version);
         connection = mqtt.blockingConnection();
         connection.connect();
         connection.publish("foo", new byte[0], QoS.EXACTLY_ONCE, false);
         assertTrue(connection.isConnected());
      } catch (Exception e) {
         e.printStackTrace();
         fail("Should not have caught an Exception");
      } finally {
         if (connection != null && connection.isConnected())
            connection.disconnect();
      }
   }

   @Test
   @Timeout(30)
   public void testPublishAuthorizationFailOn31() throws Exception {
      String version = "3.1";

      BlockingConnection connection = null;
      try {
         MQTT mqtt = createMQTTConnection("test-" + version, true);
         mqtt.setUserName(noprivUser);
         mqtt.setPassword(noprivPass);
         mqtt.setConnectAttemptsMax(1);
         mqtt.setVersion(version);
         connection = mqtt.blockingConnection();
         connection.connect();
         connection.publish("foo", new byte[0], QoS.EXACTLY_ONCE, false);
         assertTrue(connection.isConnected());
      } catch (Exception e) {
         e.printStackTrace();
         fail("Should not have caught an Exception");
      } finally {
         if (connection != null && connection.isConnected())
            connection.disconnect();
      }
   }

   @Test
   @Timeout(30)
   public void testSubscribeAuthorizationFail() throws Exception {
      for (String version : Arrays.asList("3.1", "3.1.1")) {
         BlockingConnection connection = null;
         try {
            MQTT mqtt = createMQTTConnection("test-" + version, true);
            mqtt.setUserName(noprivUser);
            mqtt.setPassword(noprivPass);
            mqtt.setConnectAttemptsMax(1);
            mqtt.setVersion(version);
            connection = mqtt.blockingConnection();
            connection.connect();
            connection.subscribe(new Topic[]{new Topic("foo", QoS.AT_MOST_ONCE)});
            assertTrue(connection.isConnected());
         } catch (Exception e) {
            e.printStackTrace();
            fail("Should not have caught an Exception");
         } finally {
            if (connection != null && connection.isConnected())
               connection.disconnect();
         }
      }
   }

   protected void setAcceptorProperty(String property) throws Exception {
      server.getRemotingService().getAcceptor(MQTT_PROTOCOL_NAME).stop();
      server.getRemotingService().createAcceptor(MQTT_PROTOCOL_NAME, "tcp://localhost:" + port + "?protocols=MQTT;" + property).start();
   }
}
