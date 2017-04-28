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

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.Test;

public class AmqpMessageRoutingTest extends JMSClientTestSupport {

   @Override
   protected boolean isAutoCreateQueues() {
      return false;
   }

   @Override
   protected boolean isAutoCreateAddresses() {
      return false;
   }

   @Test(timeout = 60000)
   public void testAnycastMessageRoutingExclusivityUsingPrefix() throws Exception {
      final String addressA = "addressA";
      final String queueA = "queueA";
      final String queueB = "queueB";
      final String queueC = "queueC";

      ActiveMQServerControl serverControl = server.getActiveMQServerControl();
      serverControl.createAddress(addressA, RoutingType.ANYCAST.toString() + "," + RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueA, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueB, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueC, RoutingType.MULTICAST.toString());

      sendMessages("anycast://" + addressA, 1);

      assertEquals(1, server.locateQueue(SimpleString.toSimpleString(queueA)).getMessageCount() + server.locateQueue(SimpleString.toSimpleString(queueB)).getMessageCount());
      assertEquals(0, server.locateQueue(SimpleString.toSimpleString(queueC)).getMessageCount());
   }

   @Test(timeout = 60000)
   public void testAnycastMessageRoutingExclusivityUsingProperty() throws Exception {
      final String addressA = "addressA";
      final String queueA = "queueA";
      final String queueB = "queueB";
      final String queueC = "queueC";

      ActiveMQServerControl serverControl = server.getActiveMQServerControl();
      serverControl.createAddress(addressA, RoutingType.ANYCAST.toString() + "," + RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueA, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueB, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueC, RoutingType.MULTICAST.toString());

      sendMessages(addressA, 1, RoutingType.ANYCAST);

      assertEquals(1, server.locateQueue(SimpleString.toSimpleString(queueA)).getMessageCount() + server.locateQueue(SimpleString.toSimpleString(queueB)).getMessageCount());
      assertEquals(0, server.locateQueue(SimpleString.toSimpleString(queueC)).getMessageCount());
   }

   @Test(timeout = 60000)
   public void testMulticastMessageRoutingExclusivityUsingPrefix() throws Exception {
      final String addressA = "addressA";
      final String queueA = "queueA";
      final String queueB = "queueB";
      final String queueC = "queueC";

      ActiveMQServerControl serverControl = server.getActiveMQServerControl();
      serverControl.createAddress(addressA, RoutingType.ANYCAST.toString() + "," + RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueA, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueB, RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueC, RoutingType.MULTICAST.toString());

      sendMessages("multicast://" + addressA, 1);

      assertEquals(0, server.locateQueue(SimpleString.toSimpleString(queueA)).getMessageCount());
      assertEquals(2, server.locateQueue(SimpleString.toSimpleString(queueC)).getMessageCount() + server.locateQueue(SimpleString.toSimpleString(queueB)).getMessageCount());
   }

   @Test(timeout = 60000)
   public void testMulticastMessageRoutingExclusivityUsingProperty() throws Exception {
      final String addressA = "addressA";
      final String queueA = "queueA";
      final String queueB = "queueB";
      final String queueC = "queueC";

      ActiveMQServerControl serverControl = server.getActiveMQServerControl();
      serverControl.createAddress(addressA, RoutingType.ANYCAST.toString() + "," + RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueA, RoutingType.ANYCAST.toString());
      serverControl.createQueue(addressA, queueB, RoutingType.MULTICAST.toString());
      serverControl.createQueue(addressA, queueC, RoutingType.MULTICAST.toString());

      sendMessages(addressA, 1, RoutingType.MULTICAST);

      assertEquals(0, server.locateQueue(SimpleString.toSimpleString(queueA)).getMessageCount());
      assertEquals(2, server.locateQueue(SimpleString.toSimpleString(queueC)).getMessageCount() + server.locateQueue(SimpleString.toSimpleString(queueB)).getMessageCount());
   }

   /**
    * If we have an address configured with both ANYCAST and MULTICAST routing types enabled, we must ensure that any
    * messages sent specifically to MULTICAST (e.g. JMS TopicProducer) are only delivered to MULTICAST queues (e.g.
    * i.e. subscription queues) and **NOT** to ANYCAST queues (e.g. JMS Queue).
    *
    * @throws Exception
    */
   @Test(timeout = 60000)
   public void testRoutingExclusivity() throws Exception {

      // Create Address with both ANYCAST and MULTICAST enabled
      String testAddress = "testRoutingExclusivity-mixed-mode";
      SimpleString ssTestAddress = new SimpleString(testAddress);

      AddressInfo addressInfo = new AddressInfo(ssTestAddress);
      addressInfo.addRoutingType(RoutingType.MULTICAST);
      addressInfo.addRoutingType(RoutingType.ANYCAST);

      server.addAddressInfo(addressInfo);
      server.createQueue(ssTestAddress, RoutingType.ANYCAST, ssTestAddress, null, true, false);

      Connection connection = createConnection(UUIDGenerator.getInstance().generateStringUUID());

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Topic topic = session.createTopic(testAddress);
         javax.jms.Queue queue = session.createQueue(testAddress);

         MessageProducer producer = session.createProducer(topic);

         MessageConsumer queueConsumer = session.createConsumer(queue);
         MessageConsumer topicConsumer = session.createConsumer(topic);

         producer.send(session.createTextMessage("testMessage"));

         assertNotNull(topicConsumer.receive(1000));
         assertNull(queueConsumer.receive(1000));
      } finally {
         connection.close();
      }
   }
}
