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
package org.apache.activemq.artemis.tests.e2e.proxy_protocol;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.core.management.impl.view.predicate.ActiveMQFilterPredicate;
import org.apache.activemq.artemis.jms.client.ActiveMQQueueConnectionFactory;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.tests.e2e.common.ContainerService;
import org.apache.activemq.artemis.tests.e2e.common.E2ETestBase;
import org.apache.activemq.artemis.tests.e2e.common.ValidateContainer;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * You need to build the Artemis Docker image with 'mvn install -De2e-tests.skipImageBuild=false' before this test is
 * executed.
 */
public class HAProxyTest extends E2ETestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   // see haproxy.cfg
   private static final int PROXY_PORT_V1 = 51617;
   private static final int PROXY_PORT_V2 = 51627;
   private static final int PROXY_PORT_SSL = 51618;
   private static final int PROXY_PORT_INVALID = 51616;

   private static final int BROKER_PROXY_PORT = 61617;
   private static final int BROKER_PROXY_SSL_PORT = 61618;
   private static final int BROKER_STANDARD_PORT = 61616;

   public static final String USER_PASS = "artemis";
   public static final String PROXY_PROTOCOL_VERSION_1 = "V1";
   public static final String PROXY_PROTOCOL_VERSION_2 = "V2";

   static Object network;
   static Object haProxy;
   static Object artemisServer;

   static ContainerService service = ContainerService.getService();

   @BeforeEach
   public void disableThreadcheck() {
      disableCheckThread();
   }

   private static final String HAPROXY_HOME = basedir + "/target/proxy-protocol";

   @BeforeAll
   public static void startServers() throws Exception {
      ValidateContainer.assumeArtemisContainer();

      assertNotNull(basedir);

      network = service.newNetwork();

      artemisServer = service.newBrokerImage();
      service.setNetwork(artemisServer, network);
      service.exposePorts(artemisServer, BROKER_PROXY_PORT, BROKER_PROXY_SSL_PORT, BROKER_STANDARD_PORT);
      service.exposeHosts(artemisServer, "broker");
      service.prepareInstance(HAPROXY_HOME);
      service.exposeBrokerHome(artemisServer, HAPROXY_HOME);
      service.startLogging(artemisServer, "ArtemisServer:");

      recreateBrokerDirectory(HAPROXY_HOME);

      service.start(artemisServer);

      haProxy = service.newHaProxyImage();
      service.setNetwork(haProxy, network);
      service.exposePorts(haProxy, PROXY_PORT_V1, PROXY_PORT_V2, PROXY_PORT_SSL, PROXY_PORT_INVALID);
      service.exposeHosts(haProxy, "haproxy");
      service.exposeFile(haProxy, basedir + "/src/main/resources/servers/proxy-protocol/haproxy.cfg", "/usr/local/etc/haproxy/haproxy.cfg");
      service.startLogging(haProxy, "haproxy:");
      service.start(haProxy);
   }

   @AfterAll
   public static void stopServer() {
      service.stop(artemisServer);
      service.stop(haProxy);
   }

   /*
    * a non-proxied connection shouldn't be able to connect to an acceptor using proxyEnabled=true
    */
   @Test
   public void testNonProxiedConnectionToProxyAcceptor() {
      testFailure(artemisServer, BROKER_PROXY_PORT);
   }

   /*
    * a proxied connection shouldn't be able to connect to an acceptor using proxyEnabled=false
    */
   @Test
   public void testProxiedConnectionToNonProxyAcceptor() {
      testFailure(haProxy, PROXY_PORT_INVALID);
   }

   private void testFailure(Object target, int port) {
      assertThrows(JMSException.class, () -> {
         assertTimeout(Duration.ofMillis(2000), () -> testSendReceive(service.createCF(target, "AMQP", port), PROXY_PROTOCOL_VERSION_1));
      });
   }

   @Test
   public void testSendReceiveCoreV1() throws Exception {
      testSendReceive(service.createCF(haProxy, "CORE", PROXY_PORT_V1), PROXY_PROTOCOL_VERSION_1);
   }

   @Test
   public void testSendReceiveAMQPV1() throws Exception {
      testSendReceive(service.createCF(haProxy, "AMQP", PROXY_PORT_V1), PROXY_PROTOCOL_VERSION_1);
   }

   @Test
   public void testSendReceiveOpenWireV1() throws Exception {
      testSendReceive(service.createCF(haProxy, "OPENWIRE", PROXY_PORT_V1), PROXY_PROTOCOL_VERSION_1);
   }

   @Test
   public void testSendReceiveCoreV2() throws Exception {
      testSendReceive(service.createCF(haProxy, "CORE", PROXY_PORT_V2), PROXY_PROTOCOL_VERSION_2);
   }

   @Test
   public void testSendReceiveAMQPV2() throws Exception {
      testSendReceive(service.createCF(haProxy, "AMQP", PROXY_PORT_V2), PROXY_PROTOCOL_VERSION_2);
   }

   @Test
   public void testSendReceiveOpenWireV2() throws Exception {
      testSendReceive(service.createCF(haProxy, "OPENWIRE", PROXY_PORT_V2), PROXY_PROTOCOL_VERSION_2);
   }

   @Test
   public void testSendReceiveCoreV1Ssl() throws Exception {
      testSendReceive(service.createCF(haProxy, "CORE", PROXY_PORT_SSL, "?sslEnabled=true;trustStorePath=server-ca-truststore.jks;trustStorePassword=securepass"), PROXY_PROTOCOL_VERSION_1);
   }

   @Test
   public void testSendReceiveAmqpV1Ssl() throws Exception {
      URL truststorePath = Thread.currentThread().getContextClassLoader().getResource("server-ca-truststore.jks");
      assertNotNull(truststorePath, "Truststore file not found on classpath");
      String truststore = truststorePath.getPath();
      testSendReceive(service.createCF(haProxy, "AMQPS", PROXY_PORT_SSL, "?transport.trustStoreLocation=" + truststore + "&transport.trustStorePassword=securepass"), PROXY_PROTOCOL_VERSION_1);
   }

   @Test
   public void testSendReceiveOpenWireV1Ssl() throws Exception {
      ActiveMQSslConnectionFactory cf = (ActiveMQSslConnectionFactory) service.createCF(haProxy, "OPENWIRE_SSL", PROXY_PORT_SSL);
      cf.setTrustStore("server-ca-truststore.jks");
      cf.setKeyStorePassword("securepass");
      testSendReceive(cf, PROXY_PROTOCOL_VERSION_1);
   }

   private void testSendReceive(ConnectionFactory cf, String version) throws Exception {
      int numberOfMessages = 100;

      for (int dest = 0; dest < 5; dest++) {
         Connection producerConnection = cf.createConnection(USER_PASS, USER_PASS);
         Wait.assertTrue(() -> verifyProxyConnectionCount(1, version));
         Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("queue.test" + dest);
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         for (int i = 0; i < numberOfMessages; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }

         Connection consumerConnection = cf.createConnection(USER_PASS, USER_PASS);
         Wait.assertTrue(() -> verifyProxyConnectionCount(2, version));
         Session sessionConsumer = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queueConsumer = sessionConsumer.createQueue("queue.test" + dest);
         MessageConsumer consumer = sessionConsumer.createConsumer(queueConsumer);
         consumerConnection.start();

         for (int i = 0; i < numberOfMessages; i++) {
            Message message = consumer.receive(5000);
            assertNotNull(message);
         }

         producerConnection.close();
         consumerConnection.close();
      }
   }

   @Test
   public void testSendReceiveMqttV1() throws Exception {
      testSendReceiveMqtt("tcp://" + service.getHost(haProxy) + ":" + service.getPort(haProxy, PROXY_PORT_V1), PROXY_PROTOCOL_VERSION_1);
   }

   @Test
   public void testSendReceiveMqttV2() throws Exception {
      testSendReceiveMqtt("tcp://" + service.getHost(haProxy) + ":" + service.getPort(haProxy, PROXY_PORT_V2), PROXY_PROTOCOL_VERSION_2);
   }

   private void testSendReceiveMqtt(String url, String version) throws Exception {
      String topic = RandomUtil.randomUUIDString();
      MqttConnectionOptions connectionOptions = new MqttConnectionOptions();
      connectionOptions.setPassword(USER_PASS.getBytes(StandardCharsets.UTF_8));
      connectionOptions.setUserName(USER_PASS);

      CountDownLatch latch = new CountDownLatch(1);
      MqttClient subscriber = new MqttClient(url, "subscriber", new MemoryPersistence());
      subscriber.connect(connectionOptions);
      subscriber.setCallback(new MqttCallback() {
         @Override
         public void disconnected(MqttDisconnectResponse mqttDisconnectResponse) {
         }

         @Override
         public void mqttErrorOccurred(MqttException e) {
         }

         @Override
         public void deliveryComplete(IMqttToken iMqttToken) {
         }

         @Override
         public void connectComplete(boolean b, String s) {
         }

         @Override
         public void authPacketArrived(int i, MqttProperties mqttProperties) {
         }

         @Override
         public void messageArrived(String topic, MqttMessage message) {
            logger.info("Message received from topic {}, message={}", topic, message);
            latch.countDown();
         }
      });
      subscriber.subscribe(topic, AT_LEAST_ONCE.value());

      MqttClient producer = new MqttClient(url, "producer", new MemoryPersistence());
      producer.connect(connectionOptions);
      producer.publish(topic, "myMessage".getBytes(StandardCharsets.UTF_8), 1, false);
      assertTrue(latch.await(500, TimeUnit.MILLISECONDS));
      Wait.assertTrue(() -> verifyProxyConnectionCount(2, version));
      subscriber.disconnect();
      producer.disconnect();
   }

   private boolean verifyProxyConnectionCount(int expectedConnections, String version) throws Exception {
      // this connection goes directly to the broker so it won't be counted as a proxy connection
      QueueConnectionFactory cf = new ActiveMQQueueConnectionFactory("tcp://" + service.getHost(artemisServer) + ":" + service.getPort(artemisServer, BROKER_STANDARD_PORT));
      try (QueueConnection c = cf.createQueueConnection(USER_PASS, USER_PASS)) {
         QueueSession s = c.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");
         QueueRequestor requestor = new QueueRequestor(s, managementQueue);
         c.start();
         Message m = s.createMessage();
         String filter = createJsonFilter("proxyProtocolVersion", ActiveMQFilterPredicate.Operation.EQUALS.toString(), version);
         JMSManagementHelper.putOperationInvocation(m, ResourceNames.BROKER, "listConnections", filter, 1, 50);
         JsonObject result = JsonUtil.readJsonObject((String) JMSManagementHelper.getResult(requestor.request(m), String.class));
         if (expectedConnections != result.getJsonNumber("count").intValue()) {
            return false;
         }
         JsonArray connections = result.getJsonArray("data");
         for (int i = 0; i < expectedConnections; i++) {
            if (connections.getJsonObject(i).getString("proxyAddress").isEmpty()) {
               return false;
            }
         }
         return true;
      }
   }
}
