/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.QUEUE_CAPABILITY;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TOPIC_CAPABILITY;


public class BrokerDefinedMulticastConsumerTest extends AmqpClientTestSupport  {

   SimpleString address = new SimpleString("testAddress");
   SimpleString queue1 = new SimpleString("queue1");
   SimpleString queue2 = new SimpleString("queue2");

   @Test(timeout = 60000)
   public void testConsumeFromSingleQueueOnAddressSameName() throws Exception {
      server.createAddressInfo(new AddressInfo(address, RoutingType.MULTICAST));
      server.createQueue(address, RoutingType.MULTICAST, address, null, true, false);

      sendMessages(1, address.toString());

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(address.toString() + "::" + address.toString());
      receiver.flow(1);
      AmqpMessage amqpMessage = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(amqpMessage);
      assertEquals(1, ((QueueImpl)server.getPostOffice().getBinding(address).getBindable()).getConsumerCount());

      receiver.close();
      connection.close();
   }

   @Test(timeout = 60000)
   public void testConsumeWhenOnlyAnycast() throws Exception {
      server.createAddressInfo(new AddressInfo(address, RoutingType.ANYCAST));

      sendMessages(1, address.toString());

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      AmqpSession session = connection.createSession();
      Source jmsSource = createJmsSource(true);
      jmsSource.setAddress(address.toString());
      try {
         session.createReceiver(jmsSource);
         fail("should throw exception");
      } catch (Exception e) {
         //ignore
      }
      connection.close();
   }

   @Test(timeout = 60000)
   public void testConsumeWhenNoAddressHasBothRoutingTypesButDefaultQueueIsAnyCast() throws Exception {
      AddressInfo addressInfo = new AddressInfo(address);
      addressInfo.getRoutingTypes().add(RoutingType.MULTICAST);
      addressInfo.getRoutingTypes().add(RoutingType.ANYCAST);
      server.createAddressInfo(addressInfo);
      server.createQueue(address, RoutingType.MULTICAST, address, null, true, false);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();
      try {
         session.createReceiver(address.toString());
         fail("expected exception");
      } catch (Exception e) {
         //ignore
      }
      connection.close();
   }


   protected Source createJmsSource(boolean topic) {

      Source source = new Source();
      // Set the capability to indicate the node type being created
      if (!topic) {
         source.setCapabilities(QUEUE_CAPABILITY);
      } else {
         source.setCapabilities(TOPIC_CAPABILITY);
      }

      return source;
   }

}
