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

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.DivertConfigurationRoutingType;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

public class AmqpMessageDivertsTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testQueueReceiverReadMessageWithDivert() throws Exception {
      runQueueReceiverReadMessageWithDivert(DivertConfigurationRoutingType.ANYCAST.toString());
   }

   @Test(timeout = 60000)
   public void testQueueReceiverReadMessageWithDivertDefaultRouting() throws Exception {
      runQueueReceiverReadMessageWithDivert(ActiveMQDefaultConfiguration.getDefaultDivertRoutingType());
   }

   public void runQueueReceiverReadMessageWithDivert(String routingType) throws Exception {
      final String forwardingAddress = getQueueName() + "Divert";
      final SimpleString simpleForwardingAddress = SimpleString.toSimpleString(forwardingAddress);
      server.createQueue(simpleForwardingAddress, RoutingType.ANYCAST, simpleForwardingAddress, null, true, false);
      server.getActiveMQServerControl().createDivert("name", "routingName", getQueueName(), forwardingAddress, true, null, null, routingType);

      sendMessages(getQueueName(), 1);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(forwardingAddress);

      Queue queueView = getProxyToQueue(forwardingAddress);
      assertEquals(1, queueView.getMessageCount());

      receiver.flow(1);
      assertNotNull(receiver.receive(5, TimeUnit.SECONDS));
      receiver.close();

      assertEquals(1, queueView.getMessageCount());

      connection.close();
   }
}
