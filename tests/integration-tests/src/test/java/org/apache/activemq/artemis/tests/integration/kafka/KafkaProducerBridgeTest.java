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
package org.apache.activemq.artemis.tests.integration.kafka;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.integration.kafka.bridge.KafkaConstants;
import org.apache.activemq.artemis.integration.kafka.bridge.KafkaProducerBridgeFactory;
import org.apache.activemq.artemis.integration.kafka.protocol.core.jms.CoreJmsMessageDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

public class KafkaProducerBridgeTest extends KafkaTestSupport {

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Override
   protected void addConfiguration(ActiveMQServer server) {

      ConnectorServiceConfiguration csc = new ConnectorServiceConfiguration();
      csc.setName("bob");
      csc.setFactoryClassName(KafkaProducerBridgeFactory.class.getName());
      Map<String, Object> map = new HashMap<>();
      map.putAll(localKafkaBroker.producerConfig());
      map.put(KafkaConstants.TOPIC_NAME, "kafkatopic");
      map.put(KafkaConstants.QUEUE_NAME, "kafkaqueue");

      csc.setParams(map);
      server.getConfiguration().addConnectorServiceConfiguration(csc);

      CoreAddressConfiguration coreAddressConfiguration = new CoreAddressConfiguration();
      coreAddressConfiguration.addRoutingType(RoutingType.MULTICAST);
      coreAddressConfiguration.setName("kafkaaddress");
      CoreQueueConfiguration coreQueueConfiguration = new CoreQueueConfiguration();
      coreQueueConfiguration.setAddress("kafkaaddress");
      coreQueueConfiguration.setName("kafkaqueue");
      coreAddressConfiguration.addQueueConfiguration(coreQueueConfiguration);
      server.getConfiguration().getAddressConfigurations().add(coreAddressConfiguration);
   }

   private void sendJmsMessage(Connection connection1, MessageCreator messageCreator) throws JMSException {
      sendJmsMessage(connection1, messageCreator, 1);
   }


   private void sendJmsMessage(Connection connection1, MessageCreator messageCreator, int numMessagesToSend) throws JMSException {
      Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Topic topic = session1.createTopic("kafkaaddress");

      MessageProducer producer = session1.createProducer(topic);
      connection1.start();

      for (int i = 0; i < numMessagesToSend; i++) {
         Message message = messageCreator.create(session1);
         producer.send(message);
      }
   }

   interface MessageCreator {

      Message create(Session session) throws JMSException;
   }

   @Test(timeout = 30000)
   public void testCoreTextMessage() throws Exception {
      Connection connection = createCoreConnection(); //CORE

      KafkaConsumer<String, String> kc = createStringConsumer("kafkatopic");

      try {

         sendJmsMessage(connection, session -> session.createTextMessage("hello"));

         ConsumerRecord<String, String> consumerRecord = consumeOneRecord(kc);

         assertNotNull(consumerRecord);
         assertEquals("hello", consumerRecord.value());
      } finally {
         kc.close();
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testCoreBytesMessage() throws Exception {
      Connection connection = createCoreConnection(); //CORE

      KafkaConsumer<byte[], byte[]> kc = createByteConsumer("kafkatopic");

      try {
         byte[] bytes = "MyBytesString".getBytes();

         sendJmsMessage(connection, session -> {
            BytesMessage bm = session.createBytesMessage();
            bm.writeBytes(bytes);
            return bm;
         });

         ConsumerRecord<byte[], byte[]> consumerRecord = consumeOneRecord(kc);

         assertNotNull(consumerRecord);
         assertTrue(Arrays.equals(bytes, consumerRecord.value()));
      } finally {
         kc.close();
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testCoreJMSMessageWithHeaders() throws Exception {
      Connection connection = createCoreConnection(); //CORE

      KafkaConsumer<String, Message> kc = createConsumer("kafkatopic", new StringDeserializer(), new CoreJmsMessageDeserializer(), null);

      String textBody = "hello world";
      byte b = 3;

      try {
         sendJmsMessage(connection, session -> {
            TextMessage message = session.createTextMessage(textBody);
            message.setLongProperty("long", 5L);
            message.setIntProperty("int", 2);
            message.setStringProperty("string", "valueString");
            message.setBooleanProperty("boolean", true);
            message.setByteProperty("byte", b);
            return message;
         });

         ConsumerRecord<String, Message> consumerRecord = consumeOneRecord(kc);

         assertNotNull(consumerRecord);

         Message message = consumerRecord.value();

         assertTrue(message instanceof TextMessage);

         TextMessage textMessage = (TextMessage) message;

         assertEquals(textBody, textMessage.getText());

         assertEquals(5L, textMessage.getLongProperty("long"));
         assertEquals(2, textMessage.getIntProperty("int"));
         assertEquals(b, textMessage.getByteProperty("byte"));
         assertEquals(true, textMessage.getBooleanProperty("boolean"));
         assertEquals("valueString", textMessage.getStringProperty("string"));

      } finally {
         kc.close();
         connection.close();
      }
   }

   @Test(timeout = 120000)
   public void testSendWhileKafkaDown() throws Exception {
      Connection connection = createConnection(); //AMQP

      KafkaConsumer<String, String> kc = createStringConsumer("kafkatopic");
      try {
         sendJmsMessage(connection, session -> session.createTextMessage("hello"));

         ConsumerRecord<String, String> consumerRecord = consumeOneRecord(kc);

         assertNotNull(consumerRecord);
         assertEquals("hello", consumerRecord.value());

         localKafkaBroker.getKafkaServer().shutdown();
         localKafkaBroker.getKafkaServer().awaitShutdown();

         sendJmsMessage(connection, session -> session.createTextMessage("kafkadown"));

         localKafkaBroker.getKafkaServer().startup();

         Thread.sleep(10000);

         sendJmsMessage(connection, session -> session.createTextMessage("kafkaup"));

         List<ConsumerRecord<String, String>> consumerRecord2s = consumeRecords(kc, 2);

         assertEquals(2, consumerRecord2s.size());
         assertEquals("kafkadown", consumerRecord2s.get(0).value());
         assertEquals("kafkaup", consumerRecord2s.get(1).value());

      } finally {
         kc.close();
         connection.close();
      }
   }
}
