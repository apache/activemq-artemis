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

package org.apache.activemq.artemis.tests.smoke.retention;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplayTest extends SmokeTestBase {
   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT_0 = 1099;
   static String liveURI = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT_0 + "/jmxrmi";
   static ObjectNameBuilder liveNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), "replay", true);

   public static final String SERVER_NAME_0 = "replay/replay";

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      startServer(SERVER_NAME_0, 0, 30000);
      disableCheckThread();
   }

   @Test
   public void testReplayAMQP() throws Throwable {
      testReplay("AMQP", 300, 100);
   }

   @Test
   public void testReplayAMQPLarge() throws Throwable {
      testReplay("AMQP", 3, 200 * 1024);
   }

   @Test
   public void testReplayCore() throws Throwable {
      testReplay("CORE", 300, 100);
   }

   @Test
   public void testReplayCoreLarge() throws Throwable {
      testReplay("CORE", 3, 200 * 1024);
   }

   private void testReplay(String protocol, int NUMBER_OF_MESSAGES, int bodySize) throws Throwable {

      final String queueName = "RetentionTest";

      ActiveMQServerControl serverControl = getServerControl(liveURI, liveNameBuilder, 5000);

      String bufferStr;
      {
         StringBuffer buffer = new StringBuffer();
         for (int i = 0; i < bodySize; i++) {
            buffer.append("*");
         }
         bufferStr = RandomUtil.randomString() + buffer.toString();
      }

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         Queue queue = session.createQueue(queueName);

         MessageProducer producer = session.createProducer(null);


         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            Message message = session.createTextMessage(bufferStr);
            message.setIntProperty("i", i);
            producer.send(queue, message);
         }
         session.commit();

         connection.start();
         MessageConsumer consumer = session.createConsumer(queue);
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals(bufferStr, message.getText());
         }
         Assert.assertNull(consumer.receiveNoWait());
         session.commit();

         serverControl.replay(queueName, queueName, null);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals(bufferStr, message.getText());
         }
         Assert.assertNull(consumer.receiveNoWait());
         session.commit();

         serverControl.replay(queueName, queueName, "i=1");

         for (int i = 0; i < 2; i++) { // replay of a replay will give you 2 messages
            TextMessage message = (TextMessage)consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals(1, message.getIntProperty("i"));
            Assert.assertEquals(bufferStr, message.getText());
         }

         Assert.assertNull(consumer.receiveNoWait());
         session.commit();

         serverControl.replay(queueName, queueName, "foo='foo'");
         Assert.assertNull(consumer.receiveNoWait());
      }
   }

}
