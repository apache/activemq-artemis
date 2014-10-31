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
package org.hornetq.tests.integration.proton;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Random;

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonTest extends ServiceTestBase
{

   private HornetQServer server;
   private String address = "exampleQueue";
   private Connection connection;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = this.createServer(false, true);
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PORT_PROP_NAME, "5672");
      params.put(TransportConstants.PROTOCOL_PROP_NAME, "AMQP");
      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      server.getConfiguration().getAcceptorConfigurations().add(transportConfiguration);
      server.start();
      server.createQueue(new SimpleString(address), new SimpleString(address), null, false, false);
      server.createQueue(new SimpleString(address + "1"), new SimpleString(address + "1"), null, false, false);
      server.createQueue(new SimpleString(address + "2"), new SimpleString(address + "2"), null, false, false);
      server.createQueue(new SimpleString(address + "3"), new SimpleString(address + "3"), null, false, false);
      server.createQueue(new SimpleString(address + "4"), new SimpleString(address + "4"), null, false, false);
      server.createQueue(new SimpleString(address + "5"), new SimpleString(address + "5"), null, false, false);
      server.createQueue(new SimpleString(address + "6"), new SimpleString(address + "6"), null, false, false);
      server.createQueue(new SimpleString(address + "7"), new SimpleString(address + "7"), null, false, false);
      server.createQueue(new SimpleString(address + "8"), new SimpleString(address + "8"), null, false, false);
      server.createQueue(new SimpleString(address + "9"), new SimpleString(address + "9"), null, false, false);
      server.createQueue(new SimpleString(address + "10"), new SimpleString(address + "10"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic"), new SimpleString("amqp_testtopic"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "1"), new SimpleString("amqp_testtopic" + "1"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "2"), new SimpleString("amqp_testtopic" + "2"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "3"), new SimpleString("amqp_testtopic" + "3"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "4"), new SimpleString("amqp_testtopic" + "4"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "5"), new SimpleString("amqp_testtopic" + "5"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "6"), new SimpleString("amqp_testtopic" + "6"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "7"), new SimpleString("amqp_testtopic" + "7"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "8"), new SimpleString("amqp_testtopic" + "8"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "9"), new SimpleString("amqp_testtopic" + "9"), null, false, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "10"), new SimpleString("amqp_testtopic" + "10"), null, false, false);
      connection = createConnection();

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (connection != null)
      {
         connection.close();
      }
      server.stop();
      super.tearDown();
   }


   /*
   // Uncomment testLoopBrowser to validate the hunging on the test
   @Test
   public void testLoopBrowser() throws Throwable
   {
      for (int i = 0 ; i < 1000; i++)
      {
         System.out.println("#test " + i);
         testBrowser();
         tearDown();
         setUp();
      }
   } */


   /**
    * This test eventually fails because of: https://issues.apache.org/jira/browse/QPID-4901
    *
    * @throws Throwable
    */
   @Test
   public void testBrowser() throws Throwable
   {
      // As this test was hunging, we added a protection here to fail it instead.
      // it seems something on the qpid client, so this failure belongs to them and we can ignore it on
      // our side (HornetQ)
      runWithTimeout(new RunnerWithEX()
      {
         @Override
         public void run() throws Throwable
         {
            int numMessages = 50;
            QueueImpl queue = new QueueImpl(address);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = session.createProducer(queue);
            for (int i = 0; i < numMessages; i++)
            {
               TextMessage message = session.createTextMessage();
               message.setText("msg:" + i);
               p.send(message);
            }

            connection.close();
            Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(address)).getBindable();
            assertEquals(q.getMessageCount(), numMessages);

            connection = createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            int count = 0;
            while (enumeration.hasMoreElements())
            {
               Message msg = (Message) enumeration.nextElement();
               assertNotNull("" + count, msg);
               assertTrue("" + msg, msg instanceof TextMessage);
               String text = ((TextMessage) msg).getText();
               assertEquals(text, "msg:" + count++);
            }
            assertEquals(count, numMessages);
            connection.close();
            assertEquals(q.getMessageCount(), numMessages);
         }
      }, 5000);
   }

   @Test
   public void testMessagesSentTransactional() throws Exception
   {
      int numMessages = 1000;
      QueueImpl queue = new QueueImpl(address);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++)
      {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      session.commit();
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(address)).getBindable();
      assertEquals(q.getMessageCount(), numMessages);
   }

   @Test
   public void testMessagesSentTransactionalRolledBack() throws Exception
   {
      int numMessages = 1000;
      QueueImpl queue = new QueueImpl(address);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++)
      {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(address)).getBindable();
      assertEquals(q.getMessageCount(), 0);
   }

   @Test
   public void testCancelMessages() throws Exception
   {
      int numMessages = 10;
      long time = System.currentTimeMillis();
      QueueImpl queue = new QueueImpl(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++)
      {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(address)).getBindable();
      assertEquals(q.getMessageCount(), numMessages);
      //now create a new connection and receive
      connection = createConnection();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queue);
      Thread.sleep(1000);
      consumer.close();
      connection.close();
      assertEquals(numMessages, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testClientAckMessages() throws Exception
   {
      int numMessages = 10;
      long time = System.currentTimeMillis();
      QueueImpl queue = new QueueImpl(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++)
      {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(address)).getBindable();
      assertEquals(q.getMessageCount(), numMessages);
      //now create a new connection and receive
      connection = createConnection();
      session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < numMessages; i++)
      {
         Message msg = consumer.receive(5000);
         if (msg == null)
         {
            System.out.println("ProtonTest.testManyMessages");
         }
         assertNotNull("" + i, msg);
         assertTrue("" + msg, msg instanceof TextMessage);
         String text = ((TextMessage) msg).getText();
         //System.out.println("text = " + text);
         assertEquals(text, "msg:" + i);
         msg.acknowledge();
      }

      consumer.close();
      connection.close();
      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);

   }

   @Test
   public void testMessagesReceivedInParallel() throws Exception
   {
      final int numMessages = 1000;
      long time = System.currentTimeMillis();
      QueueImpl queue = new QueueImpl(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = session.createConsumer(queue);

      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            int count = numMessages;
            while (count > 0)
            {
               try
               {
                  Message m = consumer.receive(5000);
                  assertNotNull("" + count, m);
                  count--;
               }
               catch (JMSException e)
               {
                  break;
               }
            }
         }
      });
      t.start();
      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++)
      {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      t.join();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(address)).getBindable();

      connection.close();
      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSelector() throws Exception
   {
      QueueImpl queue = new QueueImpl(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      TextMessage message = session.createTextMessage();
      message.setText("msg:0");
      p.send(message);
      message = session.createTextMessage();
      message.setText("msg:1");
      message.setStringProperty("color", "RED");
      p.send(message);
      connection.start();
      MessageConsumer messageConsumer = session.createConsumer(queue, "color = 'RED'");
      TextMessage m = (TextMessage) messageConsumer.receive(5000);
      assertNotNull(m);
      assertEquals("msg:1", m.getText());
      assertEquals(m.getStringProperty("color"), "RED");
      connection.close();
   }

   @Test
   public void testProperties() throws Exception
   {
      QueueImpl queue = new QueueImpl(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      TextMessage message = session.createTextMessage();
      message.setText("msg:0");
      message.setBooleanProperty("true", true);
      message.setBooleanProperty("false", false);
      message.setStringProperty("foo", "bar");
      message.setDoubleProperty("double", 66.6);
      message.setFloatProperty("float", 56.789f);
      message.setIntProperty("int", 8);
      message.setByteProperty("byte", (byte) 10);
      p.send(message);
      connection.start();
      MessageConsumer messageConsumer = session.createConsumer(queue);
      TextMessage m = (TextMessage) messageConsumer.receive(5000);
      assertNotNull(m);
      assertEquals("msg:0", m.getText());
      assertEquals(m.getBooleanProperty("true"), true);
      assertEquals(m.getBooleanProperty("false"), false);
      assertEquals(m.getStringProperty("foo"), "bar");
      assertEquals(m.getDoubleProperty("double"), 66.6, 0.0001);
      assertEquals(m.getFloatProperty("float"), 56.789f, 0.0001);
      assertEquals(m.getIntProperty("int"), 8);
      assertEquals(m.getByteProperty("byte"), (byte) 10);
      connection.close();
   }

   private javax.jms.Connection createConnection() throws JMSException
   {
      final ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", 5672, "guest", "guest");
      final javax.jms.Connection connection = factory.createConnection();
      connection.setExceptionListener(new ExceptionListener()
      {
         @Override
         public void onException(JMSException exception)
         {
            exception.printStackTrace();
         }
      });
      connection.start();
      return connection;
   }
}
