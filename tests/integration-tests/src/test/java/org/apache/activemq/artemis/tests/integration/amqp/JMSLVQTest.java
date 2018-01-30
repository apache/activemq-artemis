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
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.junit.Test;

public class JMSLVQTest extends JMSClientTestSupport {

   private static final String LVQ_QUEUE_NAME = "LVQ";

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      server.getAddressSettingsRepository().addMatch(LVQ_QUEUE_NAME, new AddressSettings().setDefaultLastValueQueue(true));
   }
   @Override
   protected void createAddressAndQueues(ActiveMQServer server) throws Exception {
      super.createAddressAndQueues(server);
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(LVQ_QUEUE_NAME), RoutingType.ANYCAST));
      server.createQueue(SimpleString.toSimpleString(LVQ_QUEUE_NAME), RoutingType.ANYCAST, SimpleString.toSimpleString("LVQ"), null, true, false, -1, false, true);
   }


   @Test
   public void testLVQAMQPProducerAMQPConsumer() throws Exception {
      Connection producerConnection = createConnection();
      Connection consumerConnection = createConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   @Test
   public void testLVQCoreProducerCoreConsumer() throws Exception {
      Connection producerConnection = createCoreConnection();
      Connection consumerConnection = createCoreConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   @Test
   public void testLVQCoreProducerAMQPConsumer() throws Exception {
      Connection producerConnection = createCoreConnection();
      Connection consumerConnection = createConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   @Test
   public void testLVQAMQPProducerCoreConsumer() throws Exception {
      Connection producerConnection = createConnection();
      Connection consumerConnection = createCoreConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   @Test
   public void testLVQOpenWireProducerOpenWireConsumer() throws Exception {
      Connection producerConnection = createOpenWireConnection();
      Connection consumerConnection = createOpenWireConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   @Test
   public void testLVQCoreProducerOpenWireConsumer() throws Exception {
      Connection producerConnection = createCoreConnection();
      Connection consumerConnection = createOpenWireConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   @Test
   public void testLVQOpenWireProducerCoreConsumer() throws Exception {
      Connection producerConnection = createOpenWireConnection();
      Connection consumerConnection = createCoreConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   @Test
   public void testLVQAMQPProducerOpenWireConsumer() throws Exception {
      Connection producerConnection = createConnection();
      Connection consumerConnection = createOpenWireConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   @Test
   public void testLVQOpenWireProducerAMQPConsumer() throws Exception {
      Connection producerConnection = createOpenWireConnection();
      Connection consumerConnection = createConnection();
      testLVQ(producerConnection, consumerConnection);
   }

   public void testLVQ(Connection producerConnection, Connection consumerConnection) throws Exception {

      try {
         Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue1 = producerSession.createQueue(LVQ_QUEUE_NAME);
         MessageProducer p = producerSession.createProducer(null);

         TextMessage message1 = producerSession.createTextMessage();
         message1.setStringProperty(Message.HDR_LAST_VALUE_NAME.toString(), "KEY");
         message1.setText("hello");
         p.send(queue1, message1);

         TextMessage message2 = producerSession.createTextMessage();
         message2.setStringProperty(Message.HDR_LAST_VALUE_NAME.toString(), "KEY");
         message2.setText("how are you");
         p.send(queue1, message2);

         //Simulate a small pause, else both messages could be consumed if consumer is fast enough
         Thread.sleep(10);

         Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue consumerQueue = consumerSession.createQueue(LVQ_QUEUE_NAME);
         MessageConsumer consumer = consumerSession.createConsumer(consumerQueue);
         TextMessage msg = (TextMessage) consumer.receive(1000);
         assertNotNull(msg);
         assertEquals("KEY", msg.getStringProperty(AMQPMessage.HDR_LAST_VALUE_NAME.toString()));
         assertEquals("how are you", msg.getText());
         consumer.close();
      } finally {
         producerConnection.close();
         consumerConnection.close();
      }
   }
}