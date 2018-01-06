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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.junit.Assert;
import org.junit.Test;

public class JMSTopicConsumerTest extends JMSClientTestSupport {

   @Test(timeout = 60000)
   public void testSendAndReceiveOnTopic() throws Exception {
      Connection connection = createConnection("myClientId");

      try {
         TopicSession session = (TopicSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         TopicSubscriber consumer = session.createSubscriber(topic);
         TopicPublisher producer = session.createPublisher(topic);

         TextMessage message = session.createTextMessage("test-message");
         producer.send(message);

         producer.close();
         connection.start();

         message = (TextMessage) consumer.receive(1000);

         assertNotNull(message);
         assertNotNull(message.getText());
         assertEquals("test-message", message.getText());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testSendWithMultipleReceiversOnTopic() throws Exception {
      Connection connection = createConnection();

      try {
         TopicSession session = (TopicSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         TopicSubscriber consumer1 = session.createSubscriber(topic);
         TopicSubscriber consumer2 = session.createSubscriber(topic);
         TopicPublisher producer = session.createPublisher(topic);

         TextMessage message = session.createTextMessage("test-message");
         producer.send(message);

         producer.close();
         connection.start();

         message = (TextMessage) consumer1.receive(1000);

         assertNotNull(message);
         assertNotNull(message.getText());
         assertEquals("test-message", message.getText());

         message = (TextMessage) consumer2.receive(1000);

         assertNotNull(message);
         assertNotNull(message.getText());
         assertEquals("test-message", message.getText());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testDurableSubscriptionUnsubscribe() throws Exception {
      Connection connection = createConnection("myClientId");

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         TopicSubscriber myDurSub = session.createDurableSubscriber(topic, "myDurSub");
         session.close();
         connection.close();

         connection = createConnection("myClientId");
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         myDurSub = session.createDurableSubscriber(topic, "myDurSub");
         myDurSub.close();

         Assert.assertNotNull(server.getPostOffice().getBinding(SimpleString.toSimpleString("myClientId.myDurSub")));
         session.unsubscribe("myDurSub");
         Assert.assertNull(server.getPostOffice().getBinding(SimpleString.toSimpleString("myClientId.myDurSub")));
         session.close();
         connection.close();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testTemporarySubscriptionDeleted() throws Exception {
      Connection connection = createConnection();

      try {
         TopicSession session = (TopicSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         TopicSubscriber myNonDurSub = session.createSubscriber(topic);
         assertNotNull(myNonDurSub);

         Bindings bindingsForAddress = server.getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(getTopicName()));
         Assert.assertEquals(2, bindingsForAddress.getBindings().size());
         session.close();

         final CountDownLatch latch = new CountDownLatch(1);
         server.getRemotingService().getConnections().iterator().next().addCloseListener(new CloseListener() {
            @Override
            public void connectionClosed() {
               latch.countDown();
            }
         });

         connection.close();
         latch.await(5, TimeUnit.SECONDS);
         bindingsForAddress = server.getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(getTopicName()));
         Assert.assertEquals(1, bindingsForAddress.getBindings().size());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testMultipleDurableConsumersSendAndReceive() throws Exception {
      Connection connection = createConnection("myClientId");

      try {
         TopicSession session = (TopicSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());

         int numMessages = 100;
         TopicSubscriber sub1 = session.createDurableSubscriber(topic, "myPubId1");
         TopicSubscriber sub2 = session.createDurableSubscriber(topic, "myPubId2");
         TopicSubscriber sub3 = session.createDurableSubscriber(topic, "myPubId3");

         Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sendSession.createProducer(topic);
         connection.start();
         for (int i = 0; i < numMessages; i++) {
            producer.send(sendSession.createTextMessage("message:" + i));
         }

         for (int i = 0; i < numMessages; i++) {
            TextMessage receive = (TextMessage) sub1.receive(5000);
            Assert.assertNotNull(receive);
            Assert.assertEquals(receive.getText(), "message:" + i);
            receive = (TextMessage) sub2.receive(5000);
            Assert.assertNotNull(receive);
            Assert.assertEquals(receive.getText(), "message:" + i);
            receive = (TextMessage) sub3.receive(5000);
            Assert.assertNotNull(receive);
            Assert.assertEquals(receive.getText(), "message:" + i);
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testDurableSubscriptionReconnection() throws Exception {
      Connection connection = createConnection("myClientId");

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());

         int numMessages = 100;
         TopicSubscriber sub = session.createDurableSubscriber(topic, "myPubId");

         Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sendSession.createProducer(topic);
         connection.start();

         for (int i = 0; i < numMessages; i++) {
            producer.send(sendSession.createTextMessage("message:" + i));
         }

         for (int i = 0; i < numMessages; i++) {
            TextMessage receive = (TextMessage) sub.receive(5000);
            Assert.assertNotNull(receive);
            Assert.assertEquals(receive.getText(), "message:" + i);
         }

         connection.close();
         connection = createConnection("myClientId");
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
               exception.printStackTrace();
            }
         });
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         sub = session.createDurableSubscriber(topic, "myPubId");

         sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         producer = sendSession.createProducer(topic);
         connection.start();
         for (int i = 0; i < numMessages; i++) {
            producer.send(sendSession.createTextMessage("message:" + i));
         }
         for (int i = 0; i < numMessages; i++) {
            TextMessage receive = (TextMessage) sub.receive(5000);
            Assert.assertNotNull(receive);
            Assert.assertEquals(receive.getText(), "message:" + i);
         }
      } finally {
         connection.close();
      }
   }
}
