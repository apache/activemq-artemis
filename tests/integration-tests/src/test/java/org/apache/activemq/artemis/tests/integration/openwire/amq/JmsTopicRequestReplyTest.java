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
package org.apache.activemq.artemis.tests.integration.openwire.amq;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import java.util.List;
import java.util.Vector;

import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * adapted from: org.apache.activemq.JmsTopicRequestReplyTest
 */
public class JmsTopicRequestReplyTest extends BasicOpenWireTest implements MessageListener {

   protected boolean useAsyncConsume;
   private Connection serverConnection;
   private Connection clientConnection;
   private MessageProducer replyProducer;
   private Session serverSession;
   private Destination requestDestination;
   private List<JMSException> failures = new Vector<>();
   private boolean dynamicallyCreateProducer;
   private String clientSideClientID;

   @Test
   public void testSendAndReceive() throws Exception {
      clientConnection = createConnection();
      clientConnection.setClientID("ClientConnection:" + name.getMethodName());

      Session session = clientConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      clientConnection.start();

      Destination replyDestination = createTemporaryDestination(session);

      MessageConsumer replyConsumer = session.createConsumer(replyDestination);

      // lets test the destination
      clientSideClientID = clientConnection.getClientID();

      // TODO
      // String value = ActiveMQDestination.getClientId((ActiveMQDestination)
      // replyDestination);
      // assertEquals("clientID from the temporary destination must be the
      // same", clientSideClientID, value);

      /* build queues */

      /* build requestmessage */
      TextMessage requestMessage = session.createTextMessage("Olivier");
      requestMessage.setJMSReplyTo(replyDestination);

      MessageProducer requestProducer = session.createProducer(requestDestination);

      requestProducer.send(requestMessage);


      Message msg = replyConsumer.receive(5000);

      if (msg instanceof TextMessage) {
         TextMessage replyMessage = (TextMessage) msg;
         assertEquals("Wrong message content", "Hello: Olivier", replyMessage.getText());
      } else {
         fail("Should have received a reply by now");
      }
      replyConsumer.close();
      deleteTemporaryDestination(replyDestination);

      assertEquals("Should not have had any failures: " + failures, 0, failures.size());
   }

   @Test
   public void testSendAndReceiveWithDynamicallyCreatedProducer() throws Exception {
      dynamicallyCreateProducer = true;
      testSendAndReceive();
   }

   /**
    * Use the asynchronous subscription mechanism
    */
   @Override
   public void onMessage(Message message) {
      try {
         TextMessage requestMessage = (TextMessage) message;


         Destination replyDestination = requestMessage.getJMSReplyTo();

         // TODO
         // String value =
         // ActiveMQDestination.getClientId((ActiveMQDestination)
         // replyDestination);
         // assertEquals("clientID from the temporary destination must be the
         // same", clientSideClientID, value);

         TextMessage replyMessage = serverSession.createTextMessage("Hello: " + requestMessage.getText());

         replyMessage.setJMSCorrelationID(requestMessage.getJMSMessageID());

         if (dynamicallyCreateProducer) {
            replyProducer = serverSession.createProducer(replyDestination);
            replyProducer.send(replyMessage);
         } else {
            replyProducer.send(replyDestination, replyMessage);
         }

      } catch (JMSException e) {
         onException(e);
      }
   }

   /**
    * Use the synchronous subscription mechanism
    */
   protected void syncConsumeLoop(MessageConsumer requestConsumer) {
      try {
         Message message = requestConsumer.receive(5000);
         if (message != null) {
            onMessage(message);
         } else {
            System.err.println("No message received");
         }
      } catch (JMSException e) {
         onException(e);
      }
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      serverConnection = createConnection();
      serverConnection.setClientID("serverConnection:" + name.getMethodName());
      serverSession = serverConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      replyProducer = serverSession.createProducer(null);

      requestDestination = createDestination(serverSession);

      /* build queues */
      final MessageConsumer requestConsumer = serverSession.createConsumer(requestDestination);
      if (useAsyncConsume) {
         requestConsumer.setMessageListener(this);
      } else {
         Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
               syncConsumeLoop(requestConsumer);
            }
         });
         thread.start();
      }
      serverConnection.start();
   }

   @Override
   @After
   public void tearDown() throws Exception {

      serverConnection.close();
      clientConnection.stop();
      clientConnection.close();

      super.tearDown();
   }

   protected void onException(JMSException e) {
      System.out.println("Caught: " + e);
      e.printStackTrace();
      failures.add(e);
   }

   protected Destination createDestination(Session session) throws JMSException {
      if (topic) {
         return this.createDestination(session, ActiveMQDestination.TOPIC_TYPE);
      }
      return this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
   }

   protected Destination createTemporaryDestination(Session session) throws JMSException {
      if (topic) {
         return session.createTemporaryTopic();
      }
      return session.createTemporaryQueue();
   }

   protected void deleteTemporaryDestination(Destination dest) throws JMSException {
      if (topic) {
         ((TemporaryTopic) dest).delete();
      } else {
         ((TemporaryQueue) dest).delete();
      }
   }

}
