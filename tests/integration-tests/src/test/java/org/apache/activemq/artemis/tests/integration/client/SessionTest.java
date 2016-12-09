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
package org.apache.activemq.artemis.tests.integration.client;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSession.QueueQuery;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CountDownSessionFailureListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This test covers the API for ClientSession although XA tests are tested separately.
 */
public class SessionTest extends ActiveMQTestBase {

   private final String queueName = "ClientSessionTestQ";

   private ServerLocator locator;
   private ActiveMQServer server;
   private ClientSessionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      locator = createInVMNonHALocator();
      server = createServer(false);
      server.start();
      waitForServerToStart(server);
   }

   @Test
   public void testFailureListener() throws Exception {

      cf = createSessionFactory(locator);
      ClientSession clientSession = addClientSession(cf.createSession(false, true, true));
      CountDownSessionFailureListener listener = new CountDownSessionFailureListener(clientSession);
      clientSession.addFailureListener(listener);
      // Make sure failure listener is called if server is stopped without session being closed first
      server.stop();
      Assert.assertTrue(listener.getLatch().await(5, TimeUnit.SECONDS));
   }

   @Test
   public void testFailureListenerRemoved() throws Exception {
      cf = createSessionFactory(locator);
      try {
         ClientSession clientSession = cf.createSession(false, true, true);
         class MyFailureListener implements SessionFailureListener {

            boolean called = false;

            @Override
            public void connectionFailed(final ActiveMQException me, boolean failedOver) {
               called = true;
            }

            @Override
            public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
               connectionFailed(me, failedOver);
            }

            @Override
            public void beforeReconnect(final ActiveMQException me) {
            }
         }

         MyFailureListener listener = new MyFailureListener();
         clientSession.addFailureListener(listener);

         Assert.assertTrue(clientSession.removeFailureListener(listener));
         clientSession.close();
         server.stop();
         Assert.assertFalse(listener.called);
      } finally {
         ((ClientSessionFactoryInternal) cf).causeExit();
         cf.close();
      }
   }

   // Closing a session if the underlying remoting connection is dead should cleanly
   // release all resources
   @Test
   public void testCloseSessionOnDestroyedConnection() throws Exception {
      // Make sure we have a short connection TTL so sessions will be quickly closed on the server
      server.stop();
      long ttl = 500;
      server.getConfiguration().setConnectionTTLOverride(ttl);
      server.start();
      cf = createSessionFactory(locator);
      ClientSessionInternal clientSession = (ClientSessionInternal) cf.createSession(false, true, true);
      clientSession.createQueue(queueName, queueName, false);
      /** keep unused variables in order to maintain references to both objects */
      @SuppressWarnings("unused")
      ClientProducer producer = clientSession.createProducer();
      @SuppressWarnings("unused")
      ClientConsumer consumer = clientSession.createConsumer(queueName);

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      RemotingConnection rc = clientSession.getConnection();

      rc.fail(new ActiveMQInternalErrorException());

      clientSession.close();

      long start = System.currentTimeMillis();

      while (true) {
         int cons = server.getRemotingService().getConnections().size();

         if (cons == 0) {
            break;
         }

         long now = System.currentTimeMillis();

         if (now - start > 10000) {
            throw new Exception("Timed out waiting for connections to close");
         }

         Thread.sleep(50);
      }
   }

   @Test
   public void testBindingQuery() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      clientSession.createQueue("a1", "q1", false);
      clientSession.createQueue("a1", "q2", false);
      clientSession.createQueue("a2", "q3", false);
      clientSession.createQueue("a2", "q4", false);
      clientSession.createQueue("a2", "q5", false);
      ClientSession.AddressQuery resp = clientSession.addressQuery(new SimpleString("a"));
      List<SimpleString> queues = resp.getQueueNames();
      Assert.assertTrue(queues.isEmpty());
      resp = clientSession.addressQuery(new SimpleString("a1"));
      queues = resp.getQueueNames();
      Assert.assertEquals(queues.size(), 2);
      Assert.assertTrue(queues.contains(new SimpleString("q1")));
      Assert.assertTrue(queues.contains(new SimpleString("q2")));
      resp = clientSession.addressQuery(new SimpleString("a2"));
      queues = resp.getQueueNames();
      Assert.assertEquals(queues.size(), 3);
      Assert.assertTrue(queues.contains(new SimpleString("q3")));
      Assert.assertTrue(queues.contains(new SimpleString("q4")));
      Assert.assertTrue(queues.contains(new SimpleString("q5")));
      clientSession.close();
   }

   @Test
   public void testQueueQuery() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      clientSession.createQueue("a1", queueName, false);
      clientSession.createConsumer(queueName);
      clientSession.createConsumer(queueName);
      ClientProducer cp = clientSession.createProducer("a1");
      cp.send(clientSession.createMessage(true));
      cp.send(clientSession.createMessage(true));

      flushQueue();

      QueueQuery resp = clientSession.queueQuery(new SimpleString(queueName));
      Assert.assertEquals(new SimpleString("a1"), resp.getAddress());
      Assert.assertEquals(2, resp.getConsumerCount());
      Assert.assertEquals(2, resp.getMessageCount());
      Assert.assertEquals(null, resp.getFilterString());
      clientSession.close();
   }

   private void flushQueue() throws Exception {
      Queue queue = server.locateQueue(SimpleString.toSimpleString(queueName));
      assertNotNull(queue);
      queue.flushExecutor();
   }

   @Test
   public void testQueueQueryWithFilter() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      clientSession.createQueue("a1", queueName, "foo=bar", false);
      clientSession.createConsumer(queueName);
      clientSession.createConsumer(queueName);

      QueueQuery resp = clientSession.queueQuery(new SimpleString(queueName));
      Assert.assertEquals(new SimpleString("a1"), resp.getAddress());
      Assert.assertEquals(2, resp.getConsumerCount());
      Assert.assertEquals(0, resp.getMessageCount());
      Assert.assertEquals(new SimpleString("foo=bar"), resp.getFilterString());
      clientSession.close();
   }

   @Test
   public void testQueueQueryNoQ() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setAutoCreateQueues(false));
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      QueueQuery resp = clientSession.queueQuery(new SimpleString(queueName));
      Assert.assertFalse(resp.isExists());
      Assert.assertFalse(resp.isAutoCreateJmsQueues());
      Assert.assertEquals(null, resp.getAddress());
      clientSession.close();
   }

   @Test
   public void testClose() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      clientSession.createQueue(queueName, queueName, false);
      ClientProducer p = clientSession.createProducer();
      ClientProducer p1 = clientSession.createProducer(queueName);
      ClientConsumer c = clientSession.createConsumer(queueName);
      ClientConsumer c1 = clientSession.createConsumer(queueName);
      clientSession.close();
      Assert.assertTrue(clientSession.isClosed());
      Assert.assertTrue(p.isClosed());
      Assert.assertTrue(p1.isClosed());
      Assert.assertTrue(c.isClosed());
      Assert.assertTrue(c1.isClosed());
   }

   @Test
   public void testCreateMessageNonDurable() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      ClientMessage clientMessage = clientSession.createMessage(false);
      Assert.assertFalse(clientMessage.isDurable());
      clientSession.close();
   }

   @Test
   public void testCreateMessageDurable() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      ClientMessage clientMessage = clientSession.createMessage(true);
      Assert.assertTrue(clientMessage.isDurable());
      clientSession.close();
   }

   @Test
   public void testCreateMessageType() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      ClientMessage clientMessage = clientSession.createMessage((byte) 99, false);
      Assert.assertEquals((byte) 99, clientMessage.getType());
      clientSession.close();
   }

   @Test
   public void testCreateMessageOverrides() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      ClientMessage clientMessage = clientSession.createMessage((byte) 88, false, 100L, 300L, (byte) 33);
      Assert.assertEquals((byte) 88, clientMessage.getType());
      Assert.assertEquals(100L, clientMessage.getExpiration());
      Assert.assertEquals(300L, clientMessage.getTimestamp());
      Assert.assertEquals((byte) 33, clientMessage.getPriority());
      clientSession.close();
   }

   @Test
   public void testGetVersion() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      Assert.assertEquals(server.getVersion().getIncrementingVersion(), clientSession.getVersion());
      clientSession.close();
   }

   @Test
   public void testStart() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      clientSession.createQueue(queueName, queueName, false);
      clientSession.start();
      clientSession.close();
   }

   @Test
   public void testStop() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, true, true);
      clientSession.createQueue(queueName, queueName, false);
      clientSession.start();
      clientSession.stop();
      clientSession.close();
   }

   @Test
   public void testCommitWithSend() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, false, true);
      clientSession.createQueue(queueName, queueName, false);
      ClientProducer cp = clientSession.createProducer(queueName);
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
      Assert.assertEquals(0, getMessageCount(q));
      clientSession.commit();
      Assert.assertEquals(10, getMessageCount(q));
      clientSession.close();
   }

   @Test
   public void testRollbackWithSend() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = cf.createSession(false, false, true);
      clientSession.createQueue(queueName, queueName, false);
      ClientProducer cp = clientSession.createProducer(queueName);
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
      Assert.assertEquals(0, getMessageCount(q));
      clientSession.rollback();
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      clientSession.commit();
      Assert.assertEquals(2, getMessageCount(q));
      clientSession.close();
   }

   @Test
   public void testCommitWithReceive() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);
      cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientProducer cp = sendSession.createProducer(queueName);
      ClientSession clientSession = cf.createSession(false, true, false);
      clientSession.createQueue(queueName, queueName, false);
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
      Assert.assertEquals(10, getMessageCount(q));
      ClientConsumer cc = clientSession.createConsumer(queueName);
      clientSession.start();
      ClientMessage m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      clientSession.commit();
      Assert.assertEquals(0, getMessageCount(q));
      clientSession.close();
      sendSession.close();
   }

   @Test
   public void testRollbackWithReceive() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);
      cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientProducer cp = sendSession.createProducer(queueName);
      ClientSession clientSession = cf.createSession(false, true, false);
      clientSession.createQueue(queueName, queueName, false);
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      cp.send(clientSession.createMessage(false));
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(queueName)).getBindable();
      Assert.assertEquals(10, getMessageCount(q));
      ClientConsumer cc = clientSession.createConsumer(queueName);
      clientSession.start();
      ClientMessage m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      m = cc.receive(5000);
      Assert.assertNotNull(m);
      m.acknowledge();
      clientSession.rollback();
      Assert.assertEquals(10, getMessageCount(q));
      clientSession.close();
      sendSession.close();
   }

   @Test
   public void testGetNodeId() throws Exception {
      cf = createSessionFactory(locator);
      ClientSession clientSession = addClientSession(cf.createSession(false, true, true));
      String nodeId = ((ClientSessionInternal) clientSession).getNodeId();
      assertNotNull(nodeId);
   }
}
