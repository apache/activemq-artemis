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
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JMSSharedDurableConsumerTest extends JMSClientTestSupport {

   @Parameterized.Parameters(name = "{index}: amqpUseCoreSubscriptionNaming={0}")
   public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][] {
         {true}, {false}
      });
   }

   /* NOT private @see https://github.com/junit-team/junit4/wiki/parameterized-tests */
   @Parameterized.Parameter(0)
   public boolean amqpUseCoreSubscriptionNaming;

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      server.getConfiguration().setAmqpUseCoreSubscriptionNaming(amqpUseCoreSubscriptionNaming);
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   private void testSharedDurableConsumer(Connection connection1, Connection connection2) throws JMSException {
      try {
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Topic topic = session1.createTopic(getTopicName());
         Topic topic2 = session2.createTopic(getTopicName());

         final MessageConsumer consumer1 = session1.createSharedDurableConsumer(topic, "SharedConsumer");
         final MessageConsumer consumer2 = session2.createSharedDurableConsumer(topic2, "SharedConsumer");

         MessageProducer producer = session1.createProducer(topic);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         connection1.start();

         TextMessage message = session1.createTextMessage();
         message.setText("hello");
         producer.send(message);

         Message message1 = consumer1.receive(100);
         Message message2 = consumer2.receive(100);

         Message received = null;
         if (message1 != null) {
            assertNull("Message should only be delivered once per subscribtion but see twice", message2);
            received = message1;
         } else {
            received = message2;
         }
         assertNotNull("Should have received a message by now.", received);
         assertTrue("Should be an instance of TextMessage", received instanceof TextMessage);

         consumer1.close();
         consumer2.close();
         session1.unsubscribe("SharedConsumer");
      } finally {
         connection1.close();
         connection2.close();
      }
   }

   @Test(timeout = 30000)
   public void testSharedDurableConsumer() throws Exception {
      Connection connection = createConnection(); //AMQP
      Connection connection2 = createConnection(); //AMQP

      testSharedDurableConsumer(connection, connection2);
   }

   @Test(timeout = 30000)
   public void testSharedDurableConsumerWithArtemisClient() throws Exception {

      Connection connection = createCoreConnection(); //CORE
      Connection connection2 = createCoreConnection(); //CORE

      testSharedDurableConsumer(connection, connection2);

   }

   @Test(timeout = 30000)
   public void testSharedDurableConsumerWithAMQPClientAndArtemisClient() throws Exception {
      org.junit.Assume.assumeTrue(amqpUseCoreSubscriptionNaming);

      Connection connection = createConnection(); //AMQP
      Connection connection2 = createCoreConnection(); //CORE

      testSharedDurableConsumer(connection, connection2);

   }

   @Test(timeout = 30000)
   public void testSharedDurableConsumerWithArtemisClientAndAMQPClient() throws Exception {
      org.junit.Assume.assumeTrue(amqpUseCoreSubscriptionNaming);

      Connection connection = createCoreConnection(); //CORE
      Connection connection2 = createConnection(); //AMQP

      testSharedDurableConsumer(connection, connection2);

   }
}
