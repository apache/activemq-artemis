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
package org.apache.activemq.artemis.tests.integration.plugin;

import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_ADD_ADDRESS;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_ADD_BINDING;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CLOSE_CONSUMER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CLOSE_SESSION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CREATE_CONNECTION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CREATE_CONSUMER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CREATE_QUEUE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CREATE_SESSION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_DELIVER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_DEPLOY_BRIDGE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_DESTROY_CONNECTION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_DESTROY_QUEUE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_MESSAGE_ROUTE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_REMOVE_ADDRESS;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_REMOVE_BINDING;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_SEND;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_SESSION_METADATA_ADDED;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_UPDATE_ADDRESS;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_ADD_ADDRESS;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_ADD_BINDING;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CLOSE_CONSUMER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CLOSE_SESSION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CREATE_CONSUMER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CREATE_QUEUE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CREATE_SESSION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_DELIVER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_DEPLOY_BRIDGE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_DESTROY_QUEUE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_MESSAGE_ROUTE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_REMOVE_ADDRESS;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_REMOVE_BINDING;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_SEND;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_SESSION_METADATA_ADDED;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_UPDATE_ADDRESS;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.MESSAGE_ACKED;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.MESSAGE_EXPIRED;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

public class CorePluginTest extends JMSTestBase {

   private Queue queue;

   private final Map<String, AtomicInteger> methodCalls = new HashMap<>();
   private final MethodCalledVerifier verifier = new MethodCalledVerifier(methodCalls);
   private final ConfigurationVerifier configurationVerifier = new ConfigurationVerifier();
   public static final String INVM_CONNECTOR_FACTORY = InVMConnectorFactory.class.getCanonicalName();

   @Override
   protected Configuration createDefaultConfig(boolean netty) throws Exception {
      Configuration config = super.createDefaultConfig(netty);
      config.registerBrokerPlugin(verifier);
      Map<String, String> props = new HashMap<>(1);
      props.put(ConfigurationVerifier.PROPERTY1, "val_1");
      configurationVerifier.init(props);
      config.registerBrokerPlugin(configurationVerifier);
      config.setMessageExpiryScanPeriod(0); // disable expiry scan so it's alwyas through delivery
      return config;
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      queue = createQueue("queue1");
   }


   @Test
   public void testSendReceive() throws Exception {
      final AckPluginVerifier ackVerifier = new AckPluginVerifier((consumer, reason) -> {
         assertEquals(AckReason.NORMAL, reason);
         assertNotNull(consumer);
      });

      server.registerBrokerPlugin(ackVerifier);

      conn = cf.createConnection();
      conn.start();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = sess.createProducer(queue);
      MessageConsumer cons = sess.createConsumer(queue);

      TextMessage msg1 = sess.createTextMessage("test");
      prod.send(msg1);
      TextMessage received1 = (TextMessage)cons.receive(1000);
      assertNotNull(received1);

      conn.close();

      verifier.validatePluginMethodsEquals(0, MESSAGE_EXPIRED, BEFORE_DEPLOY_BRIDGE, AFTER_DEPLOY_BRIDGE,
            BEFORE_DESTROY_QUEUE, AFTER_DESTROY_QUEUE, BEFORE_UPDATE_ADDRESS, AFTER_UPDATE_ADDRESS,
            BEFORE_REMOVE_ADDRESS, AFTER_REMOVE_ADDRESS, BEFORE_REMOVE_BINDING, AFTER_REMOVE_BINDING);
      verifier.validatePluginMethodsEquals(2, BEFORE_CREATE_SESSION, AFTER_CREATE_SESSION, BEFORE_CLOSE_SESSION,
            AFTER_CLOSE_SESSION, BEFORE_ADD_ADDRESS, AFTER_ADD_ADDRESS);
      verifier.validatePluginMethodsAtLeast(1, BEFORE_MESSAGE_ROUTE,
            AFTER_MESSAGE_ROUTE);

      assertEquals("configurationVerifier is invoked", 1, configurationVerifier.afterSendCounter.get());
      assertEquals("configurationVerifier is invoked", 1, configurationVerifier.successRoutedCounter.get());
      assertEquals("configurationVerifier config set", "val_1", configurationVerifier.value1);

      assertFalse(ackVerifier.getErrorMsg(), ackVerifier.hasError());
   }

   @Test
   public void testDestroyQueue() throws Exception {
      conn = cf.createConnection();
      conn.start();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      sess.createProducer(queue);
      conn.close();

      server.destroyQueue(new SimpleString(queue.getQueueName()));

      verifier.validatePluginMethodsEquals(2, BEFORE_ADD_ADDRESS, AFTER_ADD_ADDRESS);
      verifier.validatePluginMethodsEquals(1, BEFORE_CREATE_QUEUE, AFTER_CREATE_QUEUE, BEFORE_DESTROY_QUEUE,
            AFTER_DESTROY_QUEUE, BEFORE_REMOVE_BINDING, AFTER_REMOVE_BINDING);
   }

   @Test
   public void testAutoCreateQueue() throws Exception {
      conn = cf.createConnection();
      conn.start();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue autoCreatedQueue = sess.createQueue("autoCreatedQueue");
      sess.createConsumer(autoCreatedQueue);
      conn.close();

      verifier.validatePluginMethodsEquals(1,  BEFORE_DESTROY_QUEUE,
            AFTER_DESTROY_QUEUE, BEFORE_REMOVE_ADDRESS, AFTER_REMOVE_ADDRESS);

      verifier.validatePluginMethodsEquals(3, BEFORE_ADD_ADDRESS,
                                           AFTER_ADD_ADDRESS);

      verifier.validatePluginMethodsEquals(2, BEFORE_CREATE_QUEUE, AFTER_CREATE_QUEUE,
            BEFORE_ADD_BINDING, AFTER_ADD_BINDING);
   }

   @Test
   public void testAutoCreateTopic() throws Exception {
      conn = cf.createConnection();
      conn.start();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic autoCreatedTopic = sess.createTopic("autoCreatedTopic");
      sess.createConsumer(autoCreatedTopic);
      conn.close();

      //before/add address called just once to remove autocreated destination
      verifier.validatePluginMethodsEquals(1,  BEFORE_DESTROY_QUEUE,
            AFTER_DESTROY_QUEUE, BEFORE_REMOVE_ADDRESS, AFTER_REMOVE_ADDRESS, BEFORE_REMOVE_BINDING, AFTER_REMOVE_BINDING);

      //Before/Add address are called twice because of the autocreated destination and the
      //created destination in the before method
      verifier.validatePluginMethodsEquals(3, BEFORE_ADD_ADDRESS,
                                           AFTER_ADD_ADDRESS);

      verifier.validatePluginMethodsEquals(2, BEFORE_CREATE_QUEUE, AFTER_CREATE_QUEUE,
            BEFORE_ADD_BINDING, AFTER_ADD_BINDING);
   }

   @Test
   public void testMessageExpireServer() throws Exception {
      final AckPluginVerifier expiredVerifier = new AckPluginVerifier((ref, reason) -> assertEquals(AckReason.EXPIRED, reason));
      server.registerBrokerPlugin(expiredVerifier);

      conn = cf.createConnection();
      conn.setClientID("test");
      conn.start();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = sess.createProducer(queue);
      prod.setTimeToLive(1);
      MessageConsumer cons = sess.createConsumer(queue);
      Thread.sleep(100);
      TextMessage msg1 = sess.createTextMessage("test");
      prod.send(msg1);
      Thread.sleep(100);
      assertNull(cons.receive(100));

      conn.close();

      verifier.validatePluginMethodsEquals(0, BEFORE_DEPLOY_BRIDGE, AFTER_DEPLOY_BRIDGE,
            BEFORE_REMOVE_ADDRESS, AFTER_REMOVE_ADDRESS, BEFORE_REMOVE_BINDING, AFTER_REMOVE_BINDING);
      verifier.validatePluginMethodsAtLeast(1, AFTER_CREATE_CONNECTION, AFTER_DESTROY_CONNECTION,
            BEFORE_CREATE_CONSUMER, AFTER_CREATE_CONSUMER, BEFORE_CLOSE_CONSUMER, AFTER_CLOSE_CONSUMER,
            BEFORE_SESSION_METADATA_ADDED, AFTER_SESSION_METADATA_ADDED, BEFORE_ADD_ADDRESS, AFTER_ADD_ADDRESS,
            BEFORE_CREATE_QUEUE, AFTER_CREATE_QUEUE, MESSAGE_ACKED, BEFORE_SEND, AFTER_SEND, BEFORE_MESSAGE_ROUTE,
            AFTER_MESSAGE_ROUTE, MESSAGE_EXPIRED, BEFORE_ADD_ADDRESS, AFTER_ADD_ADDRESS, BEFORE_ADD_BINDING, AFTER_ADD_BINDING);
      verifier.validatePluginMethodsEquals(2, BEFORE_CREATE_SESSION, AFTER_CREATE_SESSION, BEFORE_CLOSE_SESSION,
            AFTER_CLOSE_SESSION);

      assertFalse(expiredVerifier.getErrorMsg(), expiredVerifier.hasError());
   }

   @Test
   public void testMessageExpireClient() throws Exception {
      final AckPluginVerifier expiredVerifier = new AckPluginVerifier((ref, reason) -> assertEquals(AckReason.EXPIRED, reason));
      server.registerBrokerPlugin(expiredVerifier);

      conn = cf.createConnection();
      conn.start();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = sess.createProducer(queue);
      prod.setTimeToLive(500);
      MessageConsumer cons = sess.createConsumer(queue);

      for (int i = 0; i < 10; i++) {
         TextMessage msg1 = sess.createTextMessage("test");
         prod.send(msg1);
      }
      Thread.sleep(500);
      assertNull(cons.receive(500));

      conn.close();

      verifier.validatePluginMethodsEquals(0, BEFORE_DEPLOY_BRIDGE, AFTER_DEPLOY_BRIDGE,
            BEFORE_REMOVE_ADDRESS, AFTER_REMOVE_ADDRESS, BEFORE_REMOVE_BINDING, AFTER_REMOVE_BINDING);
      verifier.validatePluginMethodsAtLeast(1, AFTER_CREATE_CONNECTION, AFTER_DESTROY_CONNECTION,
            BEFORE_CREATE_CONSUMER, AFTER_CREATE_CONSUMER, BEFORE_CLOSE_CONSUMER, AFTER_CLOSE_CONSUMER,
            BEFORE_CREATE_QUEUE, AFTER_CREATE_QUEUE, MESSAGE_ACKED, BEFORE_SEND, AFTER_SEND, BEFORE_MESSAGE_ROUTE,
            AFTER_MESSAGE_ROUTE, BEFORE_DELIVER, AFTER_DELIVER, MESSAGE_EXPIRED, BEFORE_ADD_ADDRESS,
            AFTER_ADD_ADDRESS, BEFORE_ADD_BINDING, AFTER_ADD_BINDING);
      verifier.validatePluginMethodsEquals(2, BEFORE_CREATE_SESSION, AFTER_CREATE_SESSION, BEFORE_CLOSE_SESSION,
            AFTER_CLOSE_SESSION);

      assertFalse(expiredVerifier.getErrorMsg(), expiredVerifier.hasError());
   }

   @Test
   public void testSimpleBridge() throws Exception {
      server.stop();
      ActiveMQServer server0;
      ActiveMQServer server1;

      Map<String, Object> server0Params = new HashMap<>();
      server0 = createClusteredServerWithParams(false, 0, false, server0Params);

      Map<String, Object> server1Params = new HashMap<>();
      server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      server1 = createClusteredServerWithParams(false, 1, false, server1Params);

      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";

      TransportConfiguration server1tc = new TransportConfiguration(INVM_CONNECTOR_FACTORY, server1Params);

      HashMap<String, TransportConfiguration> connectors = new HashMap<>();
      connectors.put(server1tc.getName(), server1tc);
      server0.getConfiguration().setConnectorConfigurations(connectors);
      server0.registerBrokerPlugin(verifier);

      ArrayList<String> connectorConfig = new ArrayList<>();
      connectorConfig.add(server1tc.getName());
      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration().setName("bridge1")
                                                                         .setQueueName(queueName0)
                                                                         .setForwardingAddress(forwardAddress)
                                                                         .setRetryInterval(1000)
                                                                         .setReconnectAttemptsOnSameNode(-1)
                                                                         .setUseDuplicateDetection(false)
                                                                         .setStaticConnectors(connectorConfig);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName0);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      server1.start();
      server0.start();

      verifier.validatePluginMethodsEquals(1, BEFORE_DEPLOY_BRIDGE, AFTER_DEPLOY_BRIDGE);

      server0.stop();
      server1.stop();

   }


   @Test
   public void testUpdateAddress() throws Exception {
      server.addOrUpdateAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.ANYCAST));
      server.addOrUpdateAddressInfo(new AddressInfo(SimpleString.toSimpleString("test"), RoutingType.MULTICAST));

      verifier.validatePluginMethodsEquals(1, BEFORE_UPDATE_ADDRESS, AFTER_UPDATE_ADDRESS);
   }

   private class AckPluginVerifier implements ActiveMQServerPlugin {

      private BiConsumer<ServerConsumer, AckReason> assertion;
      private Throwable error;

      AckPluginVerifier(BiConsumer<ServerConsumer, AckReason> assertion) {
         this.assertion = assertion;
      }

      @Override
      public void messageAcknowledged(MessageReference ref, AckReason reason, ServerConsumer consumer) {
         try {
            assertion.accept(consumer, reason);
         } catch (Throwable e) {
            error = e;
            throw e;
         }
      }

      private boolean hasError() {
         return error != null;
      }

      private String getErrorMsg() {
         return hasError() ? error.getMessage() : "";
      }
   }

}
