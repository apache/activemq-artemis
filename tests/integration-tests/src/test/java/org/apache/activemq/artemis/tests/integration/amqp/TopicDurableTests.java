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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicDurableTests extends JMSClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      server.getConfiguration().setAddressQueueScanPeriod(100);
   }

   @Test
   public void testMessageDurableSubscription() throws Exception {
      JmsConnectionFactory connectionFactory = new JmsConnectionFactory(getBrokerQpidJMSConnectionURI() + "?jms.clientID=jmsTopicClient");
      Connection connection = connectionFactory.createConnection();
      connection.start();

      logger.debug("testMessageDurableSubscription");
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic testTopic =  session.createTopic("jmsTopic");

      String sub1ID = "sub1DurSub";
      String sub2ID = "sub2DurSub";
      MessageConsumer subscriber1 = session.createDurableSubscriber(testTopic, sub1ID);
      MessageConsumer subscriber2 = session.createDurableSubscriber(testTopic, sub2ID);
      MessageProducer messageProducer = session.createProducer(testTopic);

      int count = 100;
      String batchPrefix = "First";
      List<Message> listMsgs = generateMessages(session, batchPrefix, count);
      sendMessages(messageProducer, listMsgs);
      logger.debug("First batch messages sent");

      List<Message> recvd1 = receiveMessages(subscriber1, count);
      List<Message> recvd2 = receiveMessages(subscriber2, count);

      assertThat(recvd1.size(), is(count));
      assertMessageContent(recvd1, batchPrefix);
      logger.debug("{} :First batch messages received", sub1ID);

      assertThat(recvd2.size(), is(count));
      assertMessageContent(recvd2, batchPrefix);
      logger.debug("{} :First batch messages received", sub2ID);

      subscriber1.close();
      logger.debug("{} : closed", sub1ID);

      batchPrefix = "Second";
      listMsgs = generateMessages(session, batchPrefix, count);
      sendMessages(messageProducer, listMsgs);
      logger.debug("Second batch messages sent");

      recvd2 = receiveMessages(subscriber2, count);
      assertThat(recvd2.size(), is(count));
      assertMessageContent(recvd2, batchPrefix);
      logger.debug("{} :Second batch messages received", sub2ID);

      subscriber1 = session.createDurableSubscriber(testTopic, sub1ID);
      logger.debug("{} :connected", sub1ID);

      recvd1 = receiveMessages(subscriber1, count);
      assertThat(recvd1.size(), is(count));
      assertMessageContent(recvd1, batchPrefix);
      logger.debug("{} :Second batch messages received", sub1ID);

      subscriber1.close();
      subscriber2.close();

      session.unsubscribe(sub1ID);
      session.unsubscribe(sub2ID);
   }


   @Test
   public void testSharedNonDurableSubscription() throws Exception {
      int iterations = 10;
      for (int i = 0; i < iterations; i++) {
         logger.debug("testSharedNonDurableSubscription; iteration: {}", i);
         //SETUP-START
         JmsConnectionFactory connectionFactory1 = new JmsConnectionFactory(getBrokerQpidJMSConnectionURI());
         Connection connection1 = connectionFactory1.createConnection();


         Hashtable env2 = new Hashtable<>();
         env2.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
         env2.put("connectionfactory.qpidConnectionFactory", "amqp://localhost:5672");
         env2.put("topic." + "jmsTopic", "jmsTopic");
         Context context2 = new InitialContext(env2);
         ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("qpidConnectionFactory");
         Connection connection2 = connectionFactory2.createConnection();

         connection1.start();
         connection2.start();

         Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic testTopic = session.createTopic("jmsTopic");
         //SETUP-END

         //BODY-S
         String subID = "sharedConsumerNonDurable123";
         MessageConsumer subscriber1 = session.createSharedConsumer(testTopic, subID);
         MessageConsumer subscriber2 = session2.createSharedConsumer(testTopic, subID);
         MessageConsumer subscriber3 = session2.createSharedConsumer(testTopic, subID);
         MessageProducer messageProducer = session.createProducer(testTopic);
         messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         int count = 10;
         List<Message> listMsgs = generateMessages(session, count);
         List<CompletableFuture<List<Message>>> results = receiveMessagesAsync(count, subscriber1, subscriber2, subscriber3);
         sendMessages(messageProducer, listMsgs);
         logger.debug("messages sent");

         assertThat("Each message should be received only by one consumer",
                    results.get(0).get(20, TimeUnit.SECONDS).size() +
                       results.get(1).get(20, TimeUnit.SECONDS).size() +
                       results.get(2).get(20, TimeUnit.SECONDS).size(),
                    is(count));
         logger.debug("messages received");
         //BODY-E

         //TEAR-DOWN-S
         connection1.stop();
         connection2.stop();
         subscriber1.close();
         subscriber2.close();
         session.close();
         session2.close();
         connection1.close();
         connection2.close();
         //TEAR-DOWN-E

         // ensure the topic is auto-deleted before continuing to the next iteration
         Wait.assertTrue(() -> server.getAddressInfo(SimpleString.of("jmsTopic")) == null, 2000, 100);
      }
   }


   private void sendMessages(MessageProducer producer, List<Message> messages) {
      messages.forEach(m -> {
         try {
            producer.send(m);
         } catch (JMSException e) {
            e.printStackTrace();
         }
      });
   }

   protected List<Message> receiveMessages(MessageConsumer consumer, int count) {
      return receiveMessages(consumer, count, 0);
   }

   protected List<Message> receiveMessages(MessageConsumer consumer, int count, long timeout) {
      List<Message> recvd = new ArrayList<>();
      IntStream.range(0, count).forEach(i -> {
         try {
            recvd.add(timeout > 0 ? consumer.receive(timeout) : consumer.receive());
         } catch (JMSException e) {
            e.printStackTrace();
         }
      });
      return recvd;
   }

   protected void assertMessageContent(List<Message> msgs, String content) {
      msgs.forEach(m -> {
         try {
            assertTrue(((TextMessage) m).getText().contains(content));
         } catch (JMSException e) {
            e.printStackTrace();
         }
      });
   }

   protected List<Message> generateMessages(Session session, int count) {
      return generateMessages(session, "", count);
   }

   protected List<Message> generateMessages(Session session, String prefix, int count) {
      List<Message> messages = new ArrayList<>();
      StringBuilder sb = new StringBuilder();
      IntStream.range(0, count).forEach(i -> {
         try {
            messages.add(session.createTextMessage(sb.append(prefix).append("testMessage").append(i).toString()));
            sb.setLength(0);
         } catch (JMSException e) {
            e.printStackTrace();
         }
      });
      return messages;
   }

   protected List<CompletableFuture<List<Message>>> receiveMessagesAsync(int count, MessageConsumer... consumer) throws JMSException {
      AtomicInteger totalCount = new AtomicInteger(count);
      List<CompletableFuture<List<Message>>> resultsList = new ArrayList<>();
      List<List<Message>> receivedResList = new ArrayList<>();

      for (int i = 0; i < consumer.length; i++) {
         final int index = i;
         resultsList.add(new CompletableFuture<>());
         receivedResList.add(new ArrayList<>());
         MessageListener myListener = message -> {
            logger.debug("Messages received{} count: {}", message, totalCount.get());
            receivedResList.get(index).add(message);
            if (totalCount.decrementAndGet() == 0) {
               for (int j = 0; j < consumer.length; j++) {
                  resultsList.get(j).complete(receivedResList.get(j));
               }
            }
         };
         consumer[i].setMessageListener(myListener);
      }
      return resultsList;
   }
}
