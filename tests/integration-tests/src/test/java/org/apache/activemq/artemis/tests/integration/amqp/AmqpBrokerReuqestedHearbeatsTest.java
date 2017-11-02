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
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpConnectionListener;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.engine.Connection;
import org.junit.Test;

/**
 * Test handling of heartbeats requested by the broker.
 */
public class AmqpBrokerReuqestedHearbeatsTest extends AmqpClientTestSupport {

   private final int TEST_IDLE_TIMEOUT = 1000;

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      server.getConfiguration().setConnectionTtlCheckInterval(TEST_IDLE_TIMEOUT / 3);
      server.getConfiguration().setConnectionTTLOverride(TEST_IDLE_TIMEOUT);
   }

   @Test(timeout = 60000)
   public void testBrokerSendsHalfConfiguredIdleTimeout() throws Exception {
      AmqpClient client = createAmqpClient();
      assertNotNull(client);

      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Connection connection) {
            assertEquals("Broker did not send half the idle timeout", TEST_IDLE_TIMEOUT / 2, connection.getTransport().getRemoteIdleTimeout());
         }
      });

      AmqpConnection connection = addConnection(client.connect());
      assertNotNull(connection);

      connection.getStateInspector().assertValid();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testBrokerSendsHalfConfiguredIdleTimeoutWhenClientSendsTimeout() throws Exception {
      AmqpClient client = createAmqpClient();
      assertNotNull(client);

      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Connection connection) {
            assertEquals("Broker did not send half the idle timeout", TEST_IDLE_TIMEOUT / 2, connection.getTransport().getRemoteIdleTimeout());
         }
      });

      AmqpConnection connection = addConnection(client.createConnection());
      connection.setIdleTimeout(TEST_IDLE_TIMEOUT * 4);
      assertNotNull(connection);

      connection.connect();
      connection.getStateInspector().assertValid();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testClientWithoutHeartbeatsGetsDropped() throws Exception {

      final CountDownLatch disconnected = new CountDownLatch(1);

      AmqpClient client = createAmqpClient();
      assertNotNull(client);

      AmqpConnection connection = addConnection(client.createConnection());
      assertNotNull(connection);

      connection.setIdleProcessingDisabled(true);
      connection.setListener(new AmqpConnectionListener() {

         @Override
         public void onException(Throwable ex) {
            disconnected.countDown();
         }
      });

      connection.connect();

      assertEquals(1, server.getConnectionCount());
      assertTrue(disconnected.await(30, TimeUnit.SECONDS));

      connection.close();

      Wait.assertEquals(0, server::getConnectionCount);
   }

   @Test(timeout = 60000)
   public void testClientWithHeartbeatsStaysAlive() throws Exception {

      final CountDownLatch disconnected = new CountDownLatch(1);

      AmqpClient client = createAmqpClient();
      assertNotNull(client);

      AmqpConnection connection = addConnection(client.createConnection());
      assertNotNull(connection);

      connection.setListener(new AmqpConnectionListener() {

         @Override
         public void onException(Throwable ex) {
            disconnected.countDown();
         }
      });

      connection.connect();

      assertEquals(1, server.getConnectionCount());
      assertFalse(disconnected.await(5, TimeUnit.SECONDS));

      connection.close();

      Wait.assertEquals(0, server::getConnectionCount);
   }
}
