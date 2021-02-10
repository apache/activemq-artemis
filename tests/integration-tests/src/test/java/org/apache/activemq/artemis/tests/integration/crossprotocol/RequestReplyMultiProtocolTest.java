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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.integration.openwire.OpenWireTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.activemq.artemis.tests.util.CFUtil.createConnectionFactory;

@RunWith(Parameterized.class)
public class RequestReplyMultiProtocolTest extends OpenWireTestBase {

   String protocolSender;
   String protocolConsumer;
   ConnectionFactory senderCF;
   ConnectionFactory consumerCF;
   private static final SimpleString queueName = SimpleString.toSimpleString("RequestReplyQueueTest");
   private static final SimpleString topicName = SimpleString.toSimpleString("RequestReplyTopicTest");
   private static final SimpleString replyQueue = SimpleString.toSimpleString("ReplyOnRequestReplyQueueTest");

   public RequestReplyMultiProtocolTest(String protocolSender, String protocolConsumer) {
      this.protocolSender = protocolSender;
      this.protocolConsumer = protocolConsumer;
   }

   @Parameterized.Parameters(name = "senderProtocol={0},receiverProtocol={1}")
   public static Iterable<Object[]> data() {
      return Arrays.asList(new Object[][] {
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
      this.server.createQueue(new QueueConfiguration(replyQueue).setRoutingType(RoutingType.ANYCAST));
      AddressInfo info = new AddressInfo(topicName, RoutingType.MULTICAST);
      this.server.addAddressInfo(info);
   }


   @Test
   public void testReplyToUsingQueue() throws Throwable {
      testReplyTo(false);
   }

   @Test
   public void testReplyToUsingTopic() throws Throwable {
      testReplyTo(true);
   }

   private void testReplyTo(boolean useTopic) throws Throwable {
      Connection senderConn = senderCF.createConnection();
      Connection consumerConn = consumerCF.createConnection();
      consumerConn.setClientID("consumer");
      try {
         Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination consumerDestination;
         if (useTopic) {
            consumerDestination = consumerSess.createTopic(topicName.toString());
         }  else {
            consumerDestination = consumerSess.createQueue(queueName.toString());
         }
         MessageConsumer consumer;

         if (useTopic) {
            consumer = consumerSess.createDurableSubscriber((Topic) consumerDestination, "test");
         } else {
            consumer = consumerSess.createConsumer(consumerDestination);
         }
         consumerConn.start();


         Session senderSess = senderConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         List<Destination> replyToDestinations = new LinkedList<>();
         replyToDestinations.add(senderSess.createQueue(replyQueue.toString()));
         replyToDestinations.add(senderSess.createTopic(topicName.toString()));
         replyToDestinations.add(senderSess.createTemporaryQueue());
         replyToDestinations.add(senderSess.createTemporaryTopic());
         Destination senderDestination;

         if (useTopic) {
            senderDestination = senderSess.createTopic(topicName.toString());
         } else {
            senderDestination = senderSess.createQueue(queueName.toString());
         }
         MessageProducer sender = senderSess.createProducer(senderDestination);

         int i = 0;
         for (Destination destination : replyToDestinations) {
            TextMessage message = senderSess.createTextMessage("hello " + (i++));
            message.setJMSReplyTo(destination);
            sender.send(message);
         }


         i = 0;
         for (Destination destination : replyToDestinations) {
            TextMessage received = (TextMessage)consumer.receive(5000);

            Assert.assertNotNull(received);
            instanceLog.debug("Destination::" + received.getJMSDestination());

            if (useTopic) {
               Assert.assertTrue("JMSDestination type is " + received.getJMSDestination().getClass(),  received.getJMSDestination() instanceof Topic);
            } else {
               Assert.assertTrue("JMSDestination type is " + received.getJMSDestination().getClass(),  received.getJMSDestination() instanceof Queue);
            }

            Assert.assertNotNull(received.getJMSReplyTo());
            Assert.assertEquals("hello " + (i++), received.getText());

            instanceLog.debug("received " + received.getText() + " and " + received.getJMSReplyTo());

            if (destination instanceof Queue) {
               Assert.assertTrue("Type is " + received.getJMSReplyTo().getClass().toString(), received.getJMSReplyTo() instanceof Queue);
               Assert.assertEquals(((Queue) destination).getQueueName(), ((Queue)received.getJMSReplyTo()).getQueueName());
            }
            if (destination instanceof Topic) {
               Assert.assertTrue("Type is " + received.getJMSReplyTo().getClass().toString(), received.getJMSReplyTo() instanceof Topic);
               Assert.assertEquals(((Topic) destination).getTopicName(), ((Topic)received.getJMSReplyTo()).getTopicName());
            }
            if (destination instanceof TemporaryQueue) {
               Assert.assertTrue("Type is " + received.getJMSReplyTo().getClass().toString(), received.getJMSReplyTo() instanceof TemporaryQueue);
               Assert.assertEquals(((TemporaryQueue) destination).getQueueName(), ((TemporaryQueue)received.getJMSReplyTo()).getQueueName());
            }
            if (destination instanceof TemporaryTopic) {
               Assert.assertTrue("Type is " + received.getJMSReplyTo().getClass().toString(), received.getJMSReplyTo() instanceof TemporaryTopic);
               Assert.assertEquals(((TemporaryTopic) destination).getTopicName(), ((TemporaryTopic)received.getJMSReplyTo()).getTopicName());
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


