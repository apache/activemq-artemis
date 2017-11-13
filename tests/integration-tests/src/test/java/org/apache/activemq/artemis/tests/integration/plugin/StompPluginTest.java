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
package org.apache.activemq.artemis.tests.integration.plugin;


import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CLOSE_CONSUMER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CLOSE_SESSION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CREATE_CONNECTION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CREATE_CONSUMER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CREATE_QUEUE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_CREATE_SESSION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_DELIVER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_DEPLOY_BRIDGE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_DESTROY_CONNECTION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_MESSAGE_ROUTE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.AFTER_SEND;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CLOSE_CONSUMER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CLOSE_SESSION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CREATE_CONSUMER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CREATE_QUEUE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_CREATE_SESSION;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_DELIVER;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_DEPLOY_BRIDGE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_MESSAGE_ROUTE;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.BEFORE_SEND;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.MESSAGE_ACKED;
import static org.apache.activemq.artemis.tests.integration.plugin.MethodCalledVerifier.MESSAGE_EXPIRED;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.stomp.StompTestBase;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionV12;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StompPluginTest extends StompTestBase {

   private static final transient IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   public static final String CLIENT_ID = "myclientid";

   private StompClientConnectionV12 conn;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      conn = (StompClientConnectionV12)StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         boolean connected = conn != null && conn.isConnected();
         log.debug("Connection 1.2 : " + connected);
         if (connected) {
            conn.disconnect();
         }
      } finally {
         super.tearDown();
      }
   }

   private final Map<String, AtomicInteger> methodCalls = new HashMap<>();
   private final MethodCalledVerifier verifier = new MethodCalledVerifier(methodCalls);

   @Override
   protected JMSServerManager createServer() throws Exception {
      JMSServerManager server = super.createServer();
      server.getActiveMQServer().registerBrokerPlugin(verifier);
      return server;
   }

   @Test
   public void testSendAndReceive() throws Exception {

      // subscribe
      //StompClientConnection newConn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      try {
         URI uri = new URI("ws+v12.stomp://localhost:61613");
         StompClientConnection newConn = StompClientConnectionFactory.createClientConnection(uri);
         newConn.connect(defUser, defPass);
         subscribe(newConn, "a-sub");

         send(newConn, getQueuePrefix() + getQueueName(), "text/plain", "Hello World 1!");
         ClientStompFrame frame = newConn.receiveFrame();

         System.out.println("received " + frame);
         Assert.assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

         verifier.validatePluginMethodsAtLeast(1, MESSAGE_ACKED, BEFORE_SEND, AFTER_SEND, BEFORE_MESSAGE_ROUTE, AFTER_MESSAGE_ROUTE, BEFORE_DELIVER,
                                               AFTER_DELIVER);


         // unsub
         unsubscribe(newConn, "a-sub");

         newConn.disconnect();

         verifier.validatePluginMethodsEquals(0, MESSAGE_EXPIRED, BEFORE_DEPLOY_BRIDGE, AFTER_DEPLOY_BRIDGE);
         verifier.validatePluginMethodsAtLeast(1, AFTER_CREATE_CONNECTION, AFTER_DESTROY_CONNECTION, BEFORE_CREATE_SESSION,
                                               AFTER_CREATE_SESSION, BEFORE_CLOSE_SESSION, AFTER_CLOSE_SESSION, BEFORE_CREATE_CONSUMER,
                                               AFTER_CREATE_CONSUMER, BEFORE_CLOSE_CONSUMER, AFTER_CLOSE_CONSUMER, BEFORE_CREATE_QUEUE, AFTER_CREATE_QUEUE,
                                               MESSAGE_ACKED, BEFORE_SEND, AFTER_SEND, BEFORE_MESSAGE_ROUTE, AFTER_MESSAGE_ROUTE, BEFORE_DELIVER,
                                               AFTER_DELIVER);

      } catch (Throwable e) {
         e.printStackTrace();
      }

   }

}
