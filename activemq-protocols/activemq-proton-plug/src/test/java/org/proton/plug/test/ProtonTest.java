/**
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
package org.proton.plug.test;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.proton.plug.AMQPClientConnectionContext;
import org.proton.plug.AMQPClientSenderContext;
import org.proton.plug.AMQPClientSessionContext;
import org.proton.plug.sasl.ClientSASLPlain;
import org.proton.plug.test.minimalclient.SimpleAMQPConnector;
import org.proton.plug.test.minimalserver.DumbServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.proton.plug.util.ByteUtil;

/**
 * This is simulating a JMS client against a simple server
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author Clebert Suconic
 */
@RunWith(Parameterized.class)
public class ProtonTest extends AbstractJMSTest
{

   protected Connection connection;

   @Parameterized.Parameters(name = "useHawt={0} sasl={1}")
   public static Collection<Object[]> data()
   {
      List<Object[]> list = Arrays.asList(new Object[][]{
         {Boolean.FALSE, Boolean.TRUE},
         {Boolean.FALSE, Boolean.FALSE}});

      System.out.println("Size = " + list.size());
      return list;
   }

   public ProtonTest(boolean useHawtJMS, boolean useSASL)
   {
      super(useHawtJMS, useSASL);
   }


   @Before
   public void setUp() throws Exception
   {
      DumbServer.clear();
      AbstractJMSTest.forceGC();
      server.start("127.0.0.1", Constants.PORT, true);
      connection = createConnection();

   }

   @After
   public void tearDown() throws Exception
   {
      if (connection != null)
      {
         connection.close();
      }

      super.tearDown();
   }

   @Test
   public void testMessagesReceivedInParallel() throws Throwable
   {
      final int numMessages = getNumberOfMessages();
      long time = System.currentTimeMillis();
      final Queue queue = createQueue();

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            Connection connectionConsumer = null;
            try
            {
               connectionConsumer = createConnection();
//               connectionConsumer = connection;
               connectionConsumer.start();
               Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
               final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

               int count = numMessages;
               while (count > 0)
               {
                  try
                  {
                     BytesMessage m = (BytesMessage) consumer.receive(1000);
                     if (count % 1000 == 0)
                     {
                        System.out.println("Count = " + count + ", property=" + m.getStringProperty("XX"));
                     }
                     Assert.assertNotNull("Could not receive message count=" + count + " on consumer", m);
                     count--;
                  }
                  catch (JMSException e)
                  {
                     break;
                  }
               }
            }
            catch (Throwable e)
            {
               exceptions.add(e);
               e.printStackTrace();
            }
            finally
            {
               try
               {
                  // if the createconnecion wasn't commented out
                  if (connectionConsumer != connection)
                  {
                     connectionConsumer.close();
                  }
               }
               catch (Throwable ignored)
               {
                  // NO OP
               }
            }
         }
      });

      Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

      t.start();

      MessageProducer p = session.createProducer(queue);
      p.setDeliveryMode(DeliveryMode.PERSISTENT);
      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage message = session.createBytesMessage();
         // TODO: this will break stuff if I use a large number
         message.writeBytes(new byte[5]);
         message.setIntProperty("count", i);
         message.setStringProperty("XX", "count" + i);
         p.send(message);
      }

      long taken = (System.currentTimeMillis() - time);
      System.out.println("taken on send = " + taken + " usehawt = " + useHawtJMS + " sasl = " + useSASL);
      t.join();

      for (Throwable e : exceptions)
      {
         throw e;
      }
      taken = (System.currentTimeMillis() - time);
      System.out.println("taken = " + taken + " usehawt = " + useHawtJMS + " sasl = " + useSASL);

      connection.close();
//      assertEquals(0, q.getMessageCount());
   }


   @Test
   public void testSimpleCreateSessionAndClose() throws Throwable
   {
      final QueueImpl queue = new QueueImpl(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Thread.sleep(1000);
      session.close();
      connection.close();
   }

   @Test
   public void testSimpleBinary() throws Throwable
   {
      final int numMessages = 5;
      long time = System.currentTimeMillis();
      final QueueImpl queue = new QueueImpl(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++)
      {
         bytes[i] = (byte) i;
      }


      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage message = session.createBytesMessage();

         message.writeBytes(bytes);
         message.setIntProperty("count", i);
         p.send(message);
      }


      session.close();


      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage m = (BytesMessage) consumer.receive(5000);

         System.out.println("length " + m.getBodyLength());
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         m.reset();

         long size = m.getBodyLength();
         byte[] bytesReceived = new byte[(int) size];
         m.readBytes(bytesReceived);


         System.out.println("Received " + ByteUtil.bytesToHex(bytesReceived, 1));

         Assert.assertArrayEquals(bytes, bytesReceived);
      }

//      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testMapMessage() throws Exception
   {
      Queue queue = createQueue();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < 10; i++)
      {
         MapMessage message = session.createMapMessage();
         message.setInt("x", i);
         message.setString("str", "str" + i);
         p.send(message);
      }
      MessageConsumer messageConsumer = session.createConsumer(queue);
      for (int i = 0; i < 10; i++)
      {
         MapMessage m = (MapMessage) messageConsumer.receive(5000);
         Assert.assertNotNull(m);
         Assert.assertEquals(i, m.getInt("x"));
         Assert.assertEquals("str" + i, m.getString("str"));
      }

      Assert.assertNull(messageConsumer.receiveNoWait());
   }

   @Test
   public void testProperties() throws Exception
   {
      Queue queue = createQueue();
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
      p.send(message);
      connection.start();
      MessageConsumer messageConsumer = session.createConsumer(queue);
      TextMessage m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      Assert.assertEquals("msg:0", m.getText());
      Assert.assertEquals(m.getBooleanProperty("true"), true);
      Assert.assertEquals(m.getBooleanProperty("false"), false);
      Assert.assertEquals(m.getStringProperty("foo"), "bar");
      Assert.assertEquals(m.getDoubleProperty("double"), 66.6, 0.0001);
      Assert.assertEquals(m.getFloatProperty("float"), 56.789f, 0.0001);
      Assert.assertEquals(m.getIntProperty("int"), 8);
      Assert.assertEquals(m.getByteProperty("byte"), (byte) 10);
      m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      connection.close();
   }

   //   @Test
   public void testSendWithSimpleClient() throws Exception
   {
      SimpleAMQPConnector connector = new SimpleAMQPConnector();
      connector.start();
      AMQPClientConnectionContext clientConnection = connector.connect("127.0.0.1", Constants.PORT);

      clientConnection.clientOpen(new ClientSASLPlain("aa", "aa"));

      AMQPClientSessionContext session = clientConnection.createClientSession();
      AMQPClientSenderContext clientSender = session.createSender(address, true);


      Properties props = new Properties();
      for (int i = 0; i < 1; i++)
      {
         MessageImpl message = (MessageImpl) Message.Factory.create();

         HashMap map = new HashMap();

         map.put("i", i);
         AmqpValue value = new AmqpValue(map);
         message.setBody(value);
         message.setProperties(props);
         clientSender.send(message);
      }

      Session clientSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      MessageConsumer consumer = clientSession.createConsumer(createQueue());
      for (int i = 0; i < 1; i++)
      {
         MapMessage msg = (MapMessage) consumer.receive(5000);
         System.out.println("Msg " + msg);
         Assert.assertNotNull(msg);

         System.out.println("Receive message " + i);

         Assert.assertEquals(0, msg.getInt("i"));
      }
   }


   protected int getNumberOfMessages()
   {
      return 10000;
   }

}
