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

package org.apache.activemq.artemis.tests.integration.crossprotocol;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.tests.integration.openwire.OpenWireTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.Arrays;

import static org.apache.activemq.artemis.tests.util.CFUtil.createConnectionFactory;

@RunWith(Parameterized.class)
public class MessageIDMultiProtocolTest extends OpenWireTestBase {

   String protocolSender;
   String protocolConsumer;
   ConnectionFactory senderCF;
   ConnectionFactory consumerCF;
   private static final SimpleString queueName = SimpleString.toSimpleString("MessageIDueueTest");

   public MessageIDMultiProtocolTest(String protocolSender, String protocolConsumer) {
      this.protocolSender = protocolSender;
      this.protocolConsumer = protocolConsumer;
   }

   @Parameterized.Parameters(name = "sender={0},consumer={1}")
   public static Iterable<Object[]> data() {
      return Arrays.asList(new Object[][]{
         {"OPENWIRE", "OPENWIRE"},
         {"OPENWIRE", "CORE"},
         {"OPENWIRE", "AMQP"},
         {"CORE", "OPENWIRE"},
         {"CORE", "CORE"},
         {"CORE", "AMQP"},
         {"AMQP", "OPENWIRE"},
         {"AMQP", "CORE"},
         {"AMQP", "AMQP"},
      });
   }


   @Before
   public void setupCF() {
      senderCF = createConnectionFactory(protocolSender, urlString);
      consumerCF = createConnectionFactory(protocolConsumer, urlString);
   }

   @Before
   public void setupQueue() throws Exception {
      Wait.assertTrue(server::isStarted);
      Wait.assertTrue(server::isActive);
      this.server.createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));
   }


   @Test
   public void testMessageIDNotNullCorrelationIDPreserved() throws Throwable {
      Connection senderConn = senderCF.createConnection();
      Connection consumerConn = consumerCF.createConnection();
      consumerConn.setClientID("consumer");

      try (Session senderSession = senderConn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
         Queue senderDestination = senderSession.createQueue(queueName.toString());
         MessageProducer senderProducer = senderSession.createProducer(senderDestination);
         Message sentMessage = senderSession.createMessage();
         sentMessage.setJMSCorrelationID("ID:MessageIDCorrelationId");
         senderProducer.send(sentMessage);
         senderConn.start();

         String sentMid = sentMessage.getJMSMessageID();

         try (Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Destination consumerDestination = consumerSess.createQueue(queueName.toString());
            MessageConsumer consumer = consumerSess.createConsumer(consumerDestination);
            consumerConn.start();

            Message receivedMessage = consumer.receive(3000);
            Assert.assertNotNull(receivedMessage);

            Assert.assertEquals(sentMessage.getJMSCorrelationID(), receivedMessage.getJMSCorrelationID());

            String messageId = receivedMessage.getJMSMessageID();
            Assert.assertNotNull(messageId);

            Assert.assertTrue(messageId.startsWith("ID:"));

            instanceLog.debug("[" + protocolSender + "][" + protocolConsumer + "] " + messageId);
            instanceLog.debug("[" + protocolSender + "][" + protocolConsumer + "] " + sentMid);

            if (protocolConsumer.equals(protocolSender)) {
               //only same protocol we guarantee the same JMSMessageID
               assertEquals(sentMid, messageId);
            }

            //specific case [CORE]->[AMQP]
            if ("CORE".equals(protocolSender) && "AMQP".equals(protocolConsumer)) {
               assertEquals(sentMid, messageId);
            }
         }
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            senderConn.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         try {
            consumerConn.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
      }
   }
}
