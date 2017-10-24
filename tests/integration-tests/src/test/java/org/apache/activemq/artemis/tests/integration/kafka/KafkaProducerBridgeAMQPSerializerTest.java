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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.integration.kafka.protocol.amqp.AmqpMessageSerializer;
import org.apache.activemq.artemis.integration.kafka.protocol.amqp.proton.ProtonMessageDeserializer;
import org.apache.activemq.artemis.integration.kafka.bridge.KafkaConstants;
import org.apache.activemq.artemis.integration.kafka.bridge.KafkaProducerBridgeFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.junit.Test;

public class KafkaProducerBridgeAMQPSerializerTest extends KafkaTestSupport {

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
      map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AmqpMessageSerializer.class.getName());

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
   public void testJMSTextMessage_KafkaProtonMessageConsumer() throws Exception {
      Connection connection = createConnection(); //AMQP

      KafkaConsumer<String, org.apache.qpid.proton.message.Message> kc =
          createConsumer("kafkatopic", new StringDeserializer(), new ProtonMessageDeserializer(), null);

      String textBody = "hello world";

      try {
         sendJmsMessage(connection, session -> {
            TextMessage message = session.createTextMessage(textBody);
            return message;
         });

         ConsumerRecord<String, org.apache.qpid.proton.message.Message> consumerRecord = consumeOneRecord(kc);

         assertNotNull(consumerRecord);

         org.apache.qpid.proton.message.Message message = consumerRecord.value();

         AmqpValue amqpValue = (AmqpValue) message.getBody();


         assertEquals(textBody, amqpValue.getValue());

      } finally {
         kc.close();
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testJMSBytesMessage_KafkaProtonMessageConsumer() throws Exception {
      Connection connection = createConnection(); //AMQP

      KafkaConsumer<String, org.apache.qpid.proton.message.Message> kc =
          createConsumer("kafkatopic", new StringDeserializer(), new ProtonMessageDeserializer(), null);

      byte[] bytes = "some bytes".getBytes();

      try {
         sendJmsMessage(connection, session -> {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(bytes);
            return message;
         });

         ConsumerRecord<String, org.apache.qpid.proton.message.Message> consumerRecord = consumeOneRecord(kc);

         assertNotNull(consumerRecord);

         org.apache.qpid.proton.message.Message message = consumerRecord.value();

         Section section = message.getBody();
         Binary binary;
         if (section instanceof Data) {
            binary = ((Data) section).getValue();
         } else {
            binary = (Binary) ((AmqpValue) section).getValue();
         }
         ByteBuffer byteBuffer = binary.asByteBuffer();
         byte[] binaryBytes = new byte[binary.getLength()];
         byteBuffer.get(binaryBytes);
         assertTrue(Arrays.equals(bytes, binaryBytes));

      } finally {
         kc.close();
         connection.close();
      }
   }
}
