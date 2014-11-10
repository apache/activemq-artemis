/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.openwire.amq;

import java.util.List;
import java.util.Vector;

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

import org.apache.activemq.command.ActiveMQDestination;
import org.hornetq.tests.integration.openwire.BasicOpenWireTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * adapted from: org.apache.activemq.JmsTopicRequestReplyTest
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class JmsTopicRequestReplyTest extends BasicOpenWireTest implements MessageListener
{
   protected boolean useAsyncConsume;
   private Connection serverConnection;
   private Connection clientConnection;
   private MessageProducer replyProducer;
   private Session serverSession;
   private Destination requestDestination;
   private List<JMSException> failures = new Vector<JMSException>();
   private boolean dynamicallyCreateProducer;
   private String clientSideClientID;

   @Test
   public void testSendAndReceive() throws Exception
   {
      clientConnection = createConnection();
      clientConnection.setClientID("ClientConnection:" + name.getMethodName());

      Session session = clientConnection.createSession(false,
            Session.AUTO_ACKNOWLEDGE);

      clientConnection.start();

      Destination replyDestination = createTemporaryDestination(session);

      // lets test the destination
      clientSideClientID = clientConnection.getClientID();

      // TODO
      // String value = ActiveMQDestination.getClientId((ActiveMQDestination)
      // replyDestination);
      // assertEquals("clientID from the temporary destination must be the
      // same", clientSideClientID, value);
      System.out
            .println("Both the clientID and destination clientID match properly: "
                  + clientSideClientID);

      /* build queues */
      MessageProducer requestProducer = session
            .createProducer(requestDestination);
      MessageConsumer replyConsumer = session.createConsumer(replyDestination);

      /* build requestmessage */
      TextMessage requestMessage = session.createTextMessage("Olivier");
      requestMessage.setJMSReplyTo(replyDestination);
      requestProducer.send(requestMessage);

      System.out.println("Sent request.");
      System.out.println(requestMessage.toString());

      Message msg = replyConsumer.receive(5000);

      if (msg instanceof TextMessage)
      {
         TextMessage replyMessage = (TextMessage) msg;
         System.out.println("Received reply.");
         System.out.println(replyMessage.toString());
         assertEquals("Wrong message content", "Hello: Olivier",
               replyMessage.getText());
      }
      else
      {
         fail("Should have received a reply by now");
      }
      replyConsumer.close();
      deleteTemporaryDestination(replyDestination);

      assertEquals("Should not have had any failures: " + failures, 0,
            failures.size());
   }

   @Test
   public void testSendAndReceiveWithDynamicallyCreatedProducer() throws Exception
   {
      dynamicallyCreateProducer = true;
      testSendAndReceive();
   }

   /**
    * Use the asynchronous subscription mechanism
    */
   public void onMessage(Message message)
   {
      try
      {
         TextMessage requestMessage = (TextMessage) message;

         System.out.println("Received request.");
         System.out.println(requestMessage.toString());

         Destination replyDestination = requestMessage.getJMSReplyTo();

         // TODO
         // String value =
         // ActiveMQDestination.getClientId((ActiveMQDestination)
         // replyDestination);
         // assertEquals("clientID from the temporary destination must be the
         // same", clientSideClientID, value);

         TextMessage replyMessage = serverSession.createTextMessage("Hello: "
               + requestMessage.getText());

         replyMessage.setJMSCorrelationID(requestMessage.getJMSMessageID());

         if (dynamicallyCreateProducer)
         {
            replyProducer = serverSession.createProducer(replyDestination);
            replyProducer.send(replyMessage);
         }
         else
         {
            replyProducer.send(replyDestination, replyMessage);
         }

         System.out.println("Sent reply.");
         System.out.println(replyMessage.toString());
      }
      catch (JMSException e)
      {
         onException(e);
      }
   }

   /**
    * Use the synchronous subscription mechanism
    */
   protected void syncConsumeLoop(MessageConsumer requestConsumer)
   {
      try
      {
         Message message = requestConsumer.receive(5000);
         if (message != null)
         {
            onMessage(message);
         }
         else
         {
            System.err.println("No message received");
         }
      }
      catch (JMSException e)
      {
         onException(e);
      }
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      serverConnection = createConnection();
      serverConnection.setClientID("serverConnection:" + name.getMethodName());
      serverSession = serverConnection.createSession(false,
            Session.AUTO_ACKNOWLEDGE);

      replyProducer = serverSession.createProducer(null);

      requestDestination = createDestination(serverSession);

      /* build queues */
      final MessageConsumer requestConsumer = serverSession
            .createConsumer(requestDestination);
      if (useAsyncConsume)
      {
         requestConsumer.setMessageListener(this);
      }
      else
      {
         Thread thread = new Thread(new Runnable()
         {
            public void run()
            {
               syncConsumeLoop(requestConsumer);
            }
         });
         thread.start();
      }
      serverConnection.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {

      serverConnection.close();
      clientConnection.stop();
      clientConnection.close();

      super.tearDown();
   }

   protected void onException(JMSException e)
   {
      System.out.println("Caught: " + e);
      e.printStackTrace();
      failures.add(e);
   }

   protected Destination createDestination(Session session) throws JMSException
   {
      if (topic)
      {
         return this.createDestination(session, ActiveMQDestination.TOPIC_TYPE);
      }
      return this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
   }

   protected Destination createTemporaryDestination(Session session) throws JMSException
   {
      if (topic)
      {
         return session.createTemporaryTopic();
      }
      return session.createTemporaryQueue();
   }

   protected void deleteTemporaryDestination(Destination dest) throws JMSException
   {
      if (topic)
      {
         ((TemporaryTopic) dest).delete();
      }
      else
      {
         ((TemporaryQueue) dest).delete();
      }
   }

}
