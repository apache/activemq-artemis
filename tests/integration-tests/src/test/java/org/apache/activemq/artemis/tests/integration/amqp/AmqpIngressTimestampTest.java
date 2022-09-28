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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AmqpIngressTimestampTest extends AmqpClientTestSupport {

   public int amqpMinLargeMessageSize = ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   @Parameterized.Parameters(name = "restart={0}, large={1}")
   public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][] {
         {true, true},
         {false, false},
         {true, false},
         {false, true}
      });
   }

   @Parameterized.Parameter(0)
   public boolean restart;

   @Parameterized.Parameter(1)
   public boolean large;

   @Test(timeout = 60000)
   public void testIngressTimestampSendCore() throws Exception {
      internalTestIngressTimestamp(Protocol.CORE);
   }

   @Test(timeout = 60000)
   public void testIngressTimestampSendAMQP() throws Exception {
      internalTestIngressTimestamp(Protocol.AMQP);
   }

   @Test(timeout = 60000)
   public void testIngressTimestampSendOpenWire() throws Exception {
      internalTestIngressTimestamp(Protocol.OPENWIRE);
   }

   private void internalTestIngressTimestamp(Protocol protocol) throws Exception {
      final String QUEUE_NAME = RandomUtil.randomString();
      server.createQueue(new QueueConfiguration(QUEUE_NAME).setRoutingType(RoutingType.ANYCAST));
      server.getAddressSettingsRepository().addMatch(QUEUE_NAME, new AddressSettings().setEnableIngressTimestamp(true));
      long beforeSend = System.currentTimeMillis();
      if (protocol == Protocol.CORE) {
         sendMessagesCore(QUEUE_NAME, 1, true, getMessagePayload());
      } else if (protocol == Protocol.OPENWIRE) {
         sendMessagesOpenWire(QUEUE_NAME, 1, true, getMessagePayload());
      } else {
         sendMessages(QUEUE_NAME, 1, true, getMessagePayload());
      }
      long afterSend = System.currentTimeMillis();

      if (restart) {
         server.stop();
         server.start();
         assertTrue(server.waitForActivation(3, TimeUnit.SECONDS));
      }

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(QUEUE_NAME);

      Queue queueView = getProxyToQueue(QUEUE_NAME);
      Wait.assertEquals(1L, queueView::getMessageCount, 2000, 100, false);

      receiver.flow(1);
      AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(receive);
      instanceLog.info("{}", receive);
      Object ingressTimestampHeader = receive.getMessageAnnotation(AMQPMessageSupport.X_OPT_INGRESS_TIME);
      assertNotNull(ingressTimestampHeader);
      assertTrue(ingressTimestampHeader instanceof Long);
      long ingressTimestamp = (Long) ingressTimestampHeader;
      assertTrue("Ingress timstamp " + ingressTimestamp + " should be >= " + beforeSend + " and <= " + afterSend,ingressTimestamp >= beforeSend && ingressTimestamp <= afterSend);
      receiver.close();

      assertEquals(1, queueView.getMessageCount());

      connection.close();
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   private enum Protocol {
      CORE, AMQP, OPENWIRE
   }

   @Override
   protected void setData(AmqpMessage amqpMessage) throws Exception {
      amqpMessage.setBytes(getMessagePayload());
   }

   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      params.put("amqpMinLargeMessageSize", ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
   }

   private byte[] getMessagePayload() {
      StringBuilder result = new StringBuilder();
      if (large) {
         for (int i = 0; i < ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 20; i++) {
            result.append("AB");
         }
      } else {
         result.append("AB");
      }

      return result.toString().getBytes();
   }
}
