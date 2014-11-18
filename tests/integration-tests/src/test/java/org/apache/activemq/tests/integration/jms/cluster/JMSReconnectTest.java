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
package org.apache.activemq.tests.integration.jms.cluster;
import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQNotConnectedException;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.junit.Assert;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.jms.HornetQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.client.impl.ClientSessionInternal;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.remoting.impl.invm.InVMRegistry;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServers;
import org.apache.activemq.jms.client.HornetQConnectionFactory;
import org.apache.activemq.jms.client.HornetQDestination;
import org.apache.activemq.jms.client.HornetQSession;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;

/**
 *
 * A JMSReconnectTest
 *
 * @author Tim Fox
 *
 *
 */
public class JMSReconnectTest extends UnitTestCase
{

   private HornetQServer liveService;

   //In this test we re-attach to the same node without restarting the server
   @Test
   public void testReattachSameNode() throws Exception
   {
      testReconnectOrReattachSameNode(true);
   }

   //In this test, we reconnect to the same node without restarting the server
   @Test
   public void testReconnectSameNode() throws Exception
   {
      testReconnectOrReattachSameNode(false);
   }

   private void testReconnectOrReattachSameNode(boolean reattach) throws Exception
   {
      HornetQConnectionFactory jbcf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory"));

      jbcf.setBlockOnDurableSend(true);
      jbcf.setBlockOnNonDurableSend(true);

      jbcf.setReconnectAttempts(-1);

      if (reattach)
      {
         jbcf.setConfirmationWindowSize(1024 * 1024);
      }

      // Note we set consumer window size to a value so we can verify that consumer credit re-sending
      // works properly on failover
      // The value is small enough that credits will have to be resent several time

      final int numMessages = 10;

      final int bodySize = 1000;

      jbcf.setConsumerWindowSize(numMessages * bodySize / 10);

      Connection conn = jbcf.createConnection();

      MyExceptionListener listener = new MyExceptionListener();

      conn.setExceptionListener(listener);

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      RemotingConnection coreConn = ((ClientSessionInternal)coreSession).getConnection();

      SimpleString jmsQueueName = new SimpleString(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + "myqueue");

      coreSession.createQueue(jmsQueueName, jmsQueueName, null, true);

      Queue queue = sess.createQueue("myqueue");

      MessageProducer producer = sess.createProducer(queue);

      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      MessageConsumer consumer = sess.createConsumer(queue);

      byte[] body = RandomUtil.randomBytes(bodySize);

      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage bm = sess.createBytesMessage();

         bm.writeBytes(body);

         producer.send(bm);
      }

      conn.start();

      Thread.sleep(2000);

      ActiveMQException me = new ActiveMQNotConnectedException();

      coreConn.fail(me);

      //It should reconnect to the same node

      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage bm = (BytesMessage)consumer.receive(1000);

         Assert.assertNotNull(bm);

         Assert.assertEquals(body.length, bm.getBodyLength());
      }

      TextMessage tm = (TextMessage)consumer.receiveNoWait();

      Assert.assertNull(tm);

      conn.close();



      Assert.assertNotNull(listener.e);

      Assert.assertTrue(me == listener.e.getCause());
   }

   @Test
   public void testReconnectSameNodeServerRestartedWithNonDurableSub() throws Exception
   {
      testReconnectSameNodeServerRestartedWithNonDurableSubOrTempQueue(true);
   }

   @Test
   public void testReconnectSameNodeServerRestartedWithTempQueue() throws Exception
   {
      testReconnectSameNodeServerRestartedWithNonDurableSubOrTempQueue(false);
   }

   //Test that non durable JMS sub gets recreated in auto reconnect
   private void testReconnectSameNodeServerRestartedWithNonDurableSubOrTempQueue(final boolean nonDurableSub) throws Exception
   {
      HornetQConnectionFactory jbcf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory"));

      jbcf.setReconnectAttempts(-1);

      Connection conn = jbcf.createConnection();

      MyExceptionListener listener = new MyExceptionListener();

      conn.setExceptionListener(listener);

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      Destination dest;

      if (nonDurableSub)
      {
         coreSession.createQueue(HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX + "mytopic", "blahblah", null, false);

         dest = HornetQJMSClient.createTopic("mytopic");
      }
      else
      {
         dest = sess.createTemporaryQueue();
      }

      MessageProducer producer = sess.createProducer(dest);

      //Create a non durable subscriber
      MessageConsumer consumer = sess.createConsumer(dest);

      this.liveService.stop();

      this.liveService.start();

      //Allow client some time to reconnect
      Thread.sleep(3000);

      final int numMessages = 100;

      byte[] body = RandomUtil.randomBytes(1000);

      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage bm = sess.createBytesMessage();

         bm.writeBytes(body);

         producer.send(bm);
      }

      conn.start();

      for (int i = 0; i < numMessages; i++)
      {
         BytesMessage bm = (BytesMessage)consumer.receive(1000);

         Assert.assertNotNull(bm);

         Assert.assertEquals(body.length, bm.getBodyLength());
      }

      TextMessage tm = (TextMessage)consumer.receiveNoWait();

      Assert.assertNull(tm);

      conn.close();

      Assert.assertNotNull(listener.e);
   }

   //If the server is shutdown after a non durable sub is created, then close on the connection should proceed normally
   @Test
   public void testNoReconnectCloseAfterFailToReconnectWithTopicConsumer() throws Exception
   {
      HornetQConnectionFactory jbcf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory"));

      jbcf.setReconnectAttempts(0);

      Connection conn = jbcf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      coreSession.createQueue(HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX + "mytopic", "blahblah", null, false);

      Topic topic = HornetQJMSClient.createTopic("mytopic");

      //Create a non durable subscriber
      sess.createConsumer(topic);

      Thread.sleep(2000);

      this.liveService.stop();

      this.liveService.start();

      sess.close();

      conn.close();
   }

   //If server is shutdown, and then connection is closed, after a temp queue has been created, the close should complete normally
   @Test
   public void testNoReconnectCloseAfterFailToReconnectWithTempQueue() throws Exception
   {
      HornetQConnectionFactory jbcf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory"));

      jbcf.setReconnectAttempts(0);

      Connection conn = jbcf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      sess.createTemporaryQueue();

      Thread.sleep(2000);

      this.liveService.stop();

      this.liveService.start();

      sess.close();

      conn.close();
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration liveConf = createBasicConfig()
         .setJournalType(getDefaultJournalType())
         .addAcceptorConfiguration(new TransportConfiguration("org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory"))
         .setBindingsDirectory(getBindingsDir())
         .setJournalMinFiles(2)
         .setJournalDirectory(getJournalDir())
         .setPagingDirectory(getPageDir())
         .setLargeMessagesDirectory(getLargeMessagesDir());

      liveService = HornetQServers.newHornetQServer(liveConf, true);
      liveService.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      liveService.stop();

      Assert.assertEquals(0, InVMRegistry.instance.size());

      liveService = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static class MyExceptionListener implements ExceptionListener
   {
      volatile JMSException e;

      public void onException(final JMSException e)
      {
         this.e = e;
      }
   }

}
