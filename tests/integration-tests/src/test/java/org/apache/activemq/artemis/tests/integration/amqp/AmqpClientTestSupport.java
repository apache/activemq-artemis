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

package org.apache.activemq.artemis.tests.integration.amqp;

import java.net.URI;
import java.util.LinkedList;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.junit.After;
import org.junit.Before;

/**
 * Test support class for tests that will be using the AMQP Proton wrapper client.
 * This is to make it easier to migrate tests from ActiveMQ5
 */
public class AmqpClientTestSupport extends ActiveMQTestBase {

   ActiveMQServer server;

   LinkedList<AmqpConnection> connections = new LinkedList<>();

   protected AmqpConnection addConnection(AmqpConnection connection) {
      connections.add(connection);
      return connection;
   }

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, true);
      server.start();
   }

   @After
   @Override
   public void tearDown() throws Exception {
      super.tearDown();

      for (AmqpConnection conn : connections) {
         try {
            conn.close();
         } catch (Throwable ignored) {
            ignored.printStackTrace();
         }
      }
      server.stop();
   }

   public Queue getProxyToQueue(String queueName) {
      return server.locateQueue(SimpleString.toSimpleString(queueName));
   }

   private String connectorScheme = "amqp";
   private boolean useSSL;

   public String getTestName() {
      return "jms.queue." + getName();
   }

   public AmqpClientTestSupport() {
   }

   public AmqpClientTestSupport(String connectorScheme, boolean useSSL) {
      this.connectorScheme = connectorScheme;
      this.useSSL = useSSL;
   }

   public String getConnectorScheme() {
      return connectorScheme;
   }

   public boolean isUseSSL() {
      return useSSL;
   }

   public String getAmqpConnectionURIOptions() {
      return "";
   }

   protected boolean isUseTcpConnector() {
      return !isUseSSL() && !connectorScheme.contains("nio") && !connectorScheme.contains("ws");
   }

   protected boolean isUseSslConnector() {
      return isUseSSL() && !connectorScheme.contains("nio") && !connectorScheme.contains("wss");
   }

   protected boolean isUseNioConnector() {
      return !isUseSSL() && connectorScheme.contains("nio");
   }

   protected boolean isUseNioPlusSslConnector() {
      return isUseSSL() && connectorScheme.contains("nio");
   }

   protected boolean isUseWsConnector() {
      return !isUseSSL() && connectorScheme.contains("ws");
   }

   protected boolean isUseWssConnector() {
      return isUseSSL() && connectorScheme.contains("wss");
   }

   public URI getBrokerAmqpConnectionURI() {
      boolean webSocket = false;

      try {
         int port = 61616;

         String uri = null;

         if (isUseSSL()) {
            if (webSocket) {
               uri = "wss://127.0.0.1:" + port;
            } else {
               uri = "ssl://127.0.0.1:" + port;
            }
         } else {
            if (webSocket) {
               uri = "ws://127.0.0.1:" + port;
            } else {
               uri = "tcp://127.0.0.1:" + port;
            }
         }

         if (!getAmqpConnectionURIOptions().isEmpty()) {
            uri = uri + "?" + getAmqpConnectionURIOptions();
         }

         return new URI(uri);
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   public AmqpConnection createAmqpConnection() throws Exception {
      return createAmqpConnection(getBrokerAmqpConnectionURI());
   }

   public AmqpConnection createAmqpConnection(String username, String password) throws Exception {
      return createAmqpConnection(getBrokerAmqpConnectionURI(), username, password);
   }

   public AmqpConnection createAmqpConnection(URI brokerURI) throws Exception {
      return createAmqpConnection(brokerURI, null, null);
   }

   public AmqpConnection createAmqpConnection(URI brokerURI, String username, String password) throws Exception {
      return createAmqpClient(brokerURI, username, password).connect();
   }

   public AmqpClient createAmqpClient() throws Exception {
      return createAmqpClient(getBrokerAmqpConnectionURI(), null, null);
   }

   public AmqpClient createAmqpClient(URI brokerURI) throws Exception {
      return createAmqpClient(brokerURI, null, null);
   }

   public AmqpClient createAmqpClient(String username, String password) throws Exception {
      return createAmqpClient(getBrokerAmqpConnectionURI(), username, password);
   }

   public AmqpClient createAmqpClient(URI brokerURI, String username, String password) throws Exception {
      return new AmqpClient(brokerURI, username, password);
   }
}
