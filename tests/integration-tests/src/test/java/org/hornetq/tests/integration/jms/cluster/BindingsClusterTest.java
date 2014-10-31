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
package org.hornetq.tests.integration.jms.cluster;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.impl.BridgeImpl;
import org.hornetq.core.server.cluster.impl.ClusterConnectionImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.JMSClusteredTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A BindingsClusterTest
 */
@RunWith(value = Parameterized.class)
public class BindingsClusterTest extends JMSClusteredTestBase
{
   private final boolean crash;

   public BindingsClusterTest(boolean crash)
   {
      this.crash = crash;
   }

   @Parameterized.Parameters
   public static Collection getParameters()
   {
      return Arrays.asList(new Object[][]{{true}, {false}});
   }

   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      jmsServer1.getHornetQServer().setIdentity("Server 1");
      jmsServer1.getHornetQServer().getConfiguration().getHAPolicy().setFailoverOnServerShutdown(true);
      jmsServer2.getHornetQServer().setIdentity("Server 2");
      jmsServer2.getHornetQServer().getConfiguration().getHAPolicy().setFailoverOnServerShutdown(true);
   }

   @Override
   protected boolean enablePersistence()
   {
      return true;
   }

   @Test
   public void testSendToSingleDisconnectedBinding() throws Exception
   {
      Connection conn1 = cf1.createConnection();

      conn1.setClientID("someClient1");

      Connection conn2 = cf2.createConnection();

      conn2.setClientID("someClient2");

      conn1.start();

      conn2.start();

      try
      {

         Topic topic1 = createTopic("t1", true);

         Topic topic2 = (Topic) context1.lookup("topic/t1");

         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons2 = session2.createDurableSubscriber(topic2, "sub2");

         cons2.close();

         session2.close();

         conn2.close();

         Thread.sleep(500);

         MessageProducer prod1 = session1.createProducer(topic1);

         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);


         prod1.send(session1.createTextMessage("m1"));


         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");
         printBindings(jmsServer2.getHornetQServer(), "jms.topic.t1");

         crash();

         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");

         prod1.send(session1.createTextMessage("m2"));

         restart();

         Thread.sleep(2000);

         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");
         printBindings(jmsServer2.getHornetQServer(), "jms.topic.t1");

         prod1.send(session1.createTextMessage("m3"));

         cf2 = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName(),
               generateInVMParams(1)));

         conn2 = cf2.createConnection();

         conn2.setClientID("someClient2");

         session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         cons2 = session2.createDurableSubscriber(topic2, "sub2");

         conn2.start();

         TextMessage received = (TextMessage) cons2.receive(5000);

         assertNotNull(received);

         assertEquals("m1", received.getText());

         received = (TextMessage) cons2.receive(5000);

         assertNotNull(received);

         assertEquals("m2", received.getText());

         received = (TextMessage) cons2.receive(5000);

         assertNotNull(received);

         assertEquals("m3", received.getText());

         cons2.close();
      }
      finally
      {
         conn1.close();
         conn2.close();
      }

      jmsServer1.destroyTopic("t1");
      jmsServer2.destroyTopic("t1");
   }

   @Test
   public void testSendToSingleDisconnectedBindingWhenLocalAvailable() throws Exception
   {
      Connection conn1 = cf1.createConnection();

      conn1.setClientID("someClient2");

      Connection conn2 = cf2.createConnection();

      conn2.setClientID("someClient2");

      conn1.start();

      conn2.start();

      try
      {

         Topic topic1 = createTopic("t1", true);

         Topic topic2 = (Topic) context1.lookup("topic/t1");

         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons1 = session1.createDurableSubscriber(topic1, "sub2");

         MessageConsumer cons2 = session2.createDurableSubscriber(topic2, "sub2");

         cons2.close();

         session2.close();

         conn2.close();

         Thread.sleep(500);

         MessageProducer prod1 = session1.createProducer(topic1);

         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);


         prod1.send(session1.createTextMessage("m1"));


         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");
         printBindings(jmsServer2.getHornetQServer(), "jms.topic.t1");

         crash();

         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");

         //send a few messages while the binding is disconnected
         prod1.send(session1.createTextMessage("m2"));
         prod1.send(session1.createTextMessage("m3"));
         prod1.send(session1.createTextMessage("m4"));

         restart();

         Thread.sleep(2000);

         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");
         printBindings(jmsServer2.getHornetQServer(), "jms.topic.t1");

         prod1.send(session1.createTextMessage("m5"));
         prod1.send(session1.createTextMessage("m6"));

         cf2 = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName(),
               generateInVMParams(1)));

         conn2 = cf2.createConnection();

         conn2.setClientID("someClient2");

         session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         cons2 = session2.createDurableSubscriber(topic2, "sub2");

         conn2.start();

         TextMessage received = (TextMessage) cons2.receiveNoWait();

         assertNull(received);

         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m1", received.getText());

         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m2", received.getText());


         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m3", received.getText());

         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m4", received.getText());

         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m5", received.getText());

         cons2.close();
      }
      finally
      {
         conn1.close();
         conn2.close();
      }

      jmsServer1.destroyTopic("t1");
      jmsServer2.destroyTopic("t1");
   }

   @Test
   public void testRemoteBindingRemovedOnReconnectLocalAvailable() throws Exception
   {
      Connection conn1 = cf1.createConnection();

      conn1.setClientID("someClient2");

      Connection conn2 = cf2.createConnection();

      conn2.setClientID("someClient2");

      conn1.start();

      conn2.start();

      try
      {

         Topic topic1 = createTopic("t1", true);

         Topic topic2 = (Topic) context1.lookup("topic/t1");

         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons1 = session1.createSharedConsumer(topic1, "sub2");

         MessageConsumer cons2 = session2.createSharedConsumer(topic2, "sub2");

         Thread.sleep(500);

         MessageProducer prod1 = session1.createProducer(topic1);

         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);


         prod1.send(session1.createTextMessage("m1"));
         prod1.send(session1.createTextMessage("m2"));


         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");
         printBindings(jmsServer2.getHornetQServer(), "jms.topic.t1");

         crash();

         //this may or may not be closed, if the server was crashed then it would have been closed on failure.
         cons2.close();

         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");

         //send a few messages while the binding is disconnected
         prod1.send(session1.createTextMessage("m3"));
         prod1.send(session1.createTextMessage("m4"));
         prod1.send(session1.createTextMessage("m5"));

         restart();

         Thread.sleep(2000);

         printBindings(jmsServer1.getHornetQServer(), "jms.topic.t1");
         printBindings(jmsServer2.getHornetQServer(), "jms.topic.t1");

         prod1.send(session1.createTextMessage("m6"));
         prod1.send(session1.createTextMessage("m7"));

         TextMessage received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m1", received.getText());

         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m3", received.getText());


         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m4", received.getText());

         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m5", received.getText());

         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m6", received.getText());

         received = (TextMessage) cons1.receive(5000);

         assertNotNull(received);

         assertEquals("m7", received.getText());

         cons2.close();
      }
      finally
      {
         conn1.close();
         conn2.close();
      }

      jmsServer1.destroyTopic("t1");
      jmsServer2.destroyTopic("t1");
   }

   private void crash() throws Exception
   {
      if (crash)
      {
         jmsServer2.stop();
      }
      else
      {
         final CountDownLatch latch = new CountDownLatch(1);
         ClusterConnectionImpl next = (ClusterConnectionImpl) server1.getClusterManager().getClusterConnections().iterator().next();
         BridgeImpl bridge = (BridgeImpl) next.getRecords().values().iterator().next().getBridge();
         RemotingConnection forwardingConnection = getForwardingConnection(bridge);
         forwardingConnection.addFailureListener(new FailureListener()
         {
            @Override
            public void connectionFailed(HornetQException exception, boolean failedOver)
            {
               latch.countDown();
            }

            @Override
            public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
            {
               connectionFailed(me, failedOver);
            }
         });
         forwardingConnection.fail(new HornetQNotConnectedException());
         assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
      }
   }

   private void restart() throws Exception
   {
      if (crash)
      {
         jmsServer2.start();
      }
   }

   private RemotingConnection getForwardingConnection(final Bridge bridge) throws Exception
   {
      long start = System.currentTimeMillis();

      do
      {
         RemotingConnection forwardingConnection = ((BridgeImpl) bridge).getForwardingConnection();

         if (forwardingConnection != null)
         {
            return forwardingConnection;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < 50000);

      throw new IllegalStateException("Failed to get forwarding connection");
   }
}
