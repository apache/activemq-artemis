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

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTConnectionManager;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTSession;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTUtil;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.activemq.transport.amqp.client.*;
import org.fusesource.mqtt.client.*;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.PUBLISH;
import org.jboss.logmanager.Level;
import org.jboss.logmanager.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.jms.*;
import java.io.EOFException;
import java.lang.reflect.Field;
import java.net.ProtocolException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.activemq.artemis.core.server.Queue;

/**
 * QT
 * MQTT Test imported from ActiveMQ MQTT component.
 */
public class MQTTSecurityTest extends MQTTTestSupport {

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Override
   @Before
   public void setUp() throws Exception {
      Field sessions = MQTTSession.class.getDeclaredField("SESSIONS");
      sessions.setAccessible(true);
      sessions.set(null, new ConcurrentHashMap<>());

      Field connectedClients = MQTTConnectionManager.class.getDeclaredField("CONNECTED_CLIENTS");
      connectedClients.setAccessible(true);
      connectedClients.set(null, new ConcurrentHashSet<>());
      super.setUp();

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

   @Test(timeout = 30000)
   public void testConnectionWithNullPassword() throws Exception {
      //TODO(jdanek) just for reporting a Jira
      Logger.getLogger("").setLevel(Level.DEBUG);

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
