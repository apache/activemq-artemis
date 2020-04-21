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
package org.apache.activemq.artemis.tests.extras.byteman;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.ActiveMQTransactionOutcomeUnknownException;
import org.apache.activemq.artemis.api.core.ActiveMQTransactionRolledBackException;
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTestBase;
import org.apache.activemq.artemis.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class BMFailoverTest extends FailoverTestBase {
   private static final Logger log = Logger.getLogger(BMFailoverTest.class);

   private ServerLocator locator;
   private ClientSessionFactoryInternal sf;
   private ClientSessionFactoryInternal sf2;
   public static TestableServer serverToStop;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      stopped = false;
      locator = getServerLocator();
   }

   private static boolean stopped = false;

   public static void stopAndThrow() throws ActiveMQUnBlockedException {
      if (!stopped) {
         try {
            serverToStop.getServer().fail(true);
         } catch (Exception e) {
            e.printStackTrace();
         }
         try {
            Thread.sleep(2000);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         stopped = true;
         throw ActiveMQClientMessageBundle.BUNDLE.unblockingACall(null);
      }
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "trace ActiveMQSessionContext xaEnd",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQSessionContext",
         targetMethod = "xaEnd",
         targetLocation = "AT EXIT",
         action = "org.apache.activemq.artemis.tests.extras.byteman.BMFailoverTest.stopAndThrow()")})
   //https://bugzilla.redhat.com/show_bug.cgi?id=1152410
   public void testFailOnEndAndRetry() throws Exception {
      serverToStop = liveServer;

      createSessionFactory();

      ClientSession session = createSession(sf, true, false, false);

      session.createQueue(new QueueConfiguration(FailoverTestBase.ADDRESS));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      for (int i = 0; i < 100; i++) {
         producer.send(createMessage(session, i, true));
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      Xid xid = RandomUtil.randomXid();

      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      // Receive MSGs but don't ack!
      for (int i = 0; i < 100; i++) {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull(message);

         assertMessageBody(i, message);

         Assert.assertEquals(i, message.getIntProperty("counter").intValue());
      }
      try {
         //top level prepare
         session.end(xid, XAResource.TMSUCCESS);
      } catch (XAException e) {
         try {
            //top level abort
            session.end(xid, XAResource.TMFAIL);
         } catch (XAException e1) {
            try {
               //rollback
               session.rollback(xid);
            } catch (XAException e2) {
            }
         }
      }
      xid = RandomUtil.randomXid();
      session.start(xid, XAResource.TMNOFLAGS);

      for (int i = 0; i < 50; i++) {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull(message);

         assertMessageBody(i, message);

         Assert.assertEquals(i, message.getIntProperty("counter").intValue());
      }
      session.end(xid, XAResource.TMSUCCESS);
      session.commit(xid, true);
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "trace clientsessionimpl commit",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientSessionImpl",
         targetMethod = "start(javax.transaction.xa.Xid, int)",
         targetLocation = "AT EXIT",
         action = "org.apache.activemq.artemis.tests.extras.byteman.BMFailoverTest.serverToStop.getServer().stop(true)")})
   public void testFailoverOnCommit2() throws Exception {
      serverToStop = liveServer;
      locator = getServerLocator();
      SimpleString inQueue = new SimpleString("inQueue");
      SimpleString outQueue = new SimpleString("outQueue");
      createSessionFactory();
      createSessionFactory2();

      // closeable will take care of closing it
      try (ClientSession session = sf.createSession(false, true, true);
           ClientProducer sendInitialProducer = session.createProducer();) {
         session.createQueue(new QueueConfiguration(inQueue));
         session.createQueue(new QueueConfiguration(outQueue));
         sendInitialProducer.send(inQueue, createMessage(session, 0, true));
      }

      ClientSession xaSessionRec = addClientSession(sf.createSession(true, false, false));

      ClientConsumer consumer = addClientConsumer(xaSessionRec.createConsumer(inQueue));

      byte[] globalTransactionId = UUIDGenerator.getInstance().generateStringUUID().getBytes();
      Xid xidRec = new XidImpl("xa2".getBytes(), 1, globalTransactionId);

      xaSessionRec.start();

      xaSessionRec.getXAResource().start(xidRec, XAResource.TMNOFLAGS);

      //failover is now occurring, receive, ack and end will be called whilst this is happening.

      ClientMessageImpl m = (ClientMessageImpl) consumer.receive(5000);

      assertNotNull(m);

      log.debug("********************" + m.getIntProperty("counter"));
      //the mdb would ack the message before calling onMessage()
      m.acknowledge();

      try {
         //this may fail but thats ok, it depends on the race and when failover actually happens
         xaSessionRec.end(xidRec, XAResource.TMSUCCESS);
      } catch (XAException ignore) {
      }

      //we always reset the client on the RA
      ((ClientSessionInternal) xaSessionRec).resetIfNeeded();

      // closeable will take care of closing it
      try (ClientSession session = sf.createSession(false, true, true);
           ClientProducer sendInitialProducer = session.createProducer();) {
         sendInitialProducer.send(inQueue, createMessage(session, 0, true));
      }

      //now receive and send a message successfully

      globalTransactionId = UUIDGenerator.getInstance().generateStringUUID().getBytes();
      xidRec = new XidImpl("xa4".getBytes(), 1, globalTransactionId);
      xaSessionRec.getXAResource().start(xidRec, XAResource.TMNOFLAGS);

      Binding binding = backupServer.getServer().getPostOffice().getBinding(inQueue);
      Queue inQ = (Queue) binding.getBindable();

      m = (ClientMessageImpl) consumer.receive(5000);

      assertNotNull(m);
      //the mdb would ack the message before calling onMessage()
      m.acknowledge();

      log.debug("********************" + m.getIntProperty("counter"));

      xaSessionRec.getXAResource().end(xidRec, XAResource.TMSUCCESS);
      xaSessionRec.getXAResource().prepare(xidRec);
      xaSessionRec.getXAResource().commit(xidRec, false);

      //let's close the consumer so anything pending is handled
      consumer.close();

      assertEquals(1, getMessageCount(inQ));
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "trace clientsessionimpl commit",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientSessionImpl",
         targetMethod = "commit",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.BMFailoverTest.serverToStop.getServer().stop(true)")})
   public void testFailoverOnCommit() throws Exception {
      serverToStop = liveServer;
      locator = getServerLocator();
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientProducer producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(session, producer, 10);
      try {
         session.commit();
         fail("should have thrown an exception");
      } catch (ActiveMQTransactionOutcomeUnknownException e) {
         //pass
      }
      sendMessages(session, producer, 10);
      session.commit();
      Queue bindable = (Queue) backupServer.getServer().getPostOffice().getBinding(FailoverTestBase.ADDRESS).getBindable();
      assertEquals(10, getMessageCount(bindable));
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "trace clientsessionimpl commit",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientSessionImpl",
         targetMethod = "commit",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.BMFailoverTest.serverToStop.getServer().stop(true)")})
   public void testFailoverOnReceiveCommit() throws Exception {
      serverToStop = liveServer;
      locator = getServerLocator();
      createSessionFactory();
      ClientSession session = createSessionAndQueue();

      ClientSession sendSession = createSession(sf, true, true);

      ClientProducer producer = addClientProducer(sendSession.createProducer(FailoverTestBase.ADDRESS));

      sendMessages(sendSession, producer, 10);

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      session.start();
      for (int i = 0; i < 10; i++) {
         ClientMessage m = consumer.receive(500);
         assertNotNull(m);
         m.acknowledge();
      }
      try {
         session.commit();
         fail("should have thrown an exception");
      } catch (ActiveMQTransactionOutcomeUnknownException e) {
         //pass
      } catch (ActiveMQTransactionRolledBackException e1) {
         //pass
      }
      Queue bindable = (Queue) backupServer.getServer().getPostOffice().getBinding(FailoverTestBase.ADDRESS).getBindable();
      assertEquals(10, getMessageCount(bindable));

   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return getNettyAcceptorTransportConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return getNettyConnectorTransportConfiguration(live);
   }

   private ClientSession createSessionAndQueue() throws Exception {
      ClientSession session = createSession(sf, false, false);

      session.createQueue(new QueueConfiguration(FailoverTestBase.ADDRESS));
      return session;
   }

   private ClientSession createXASessionAndQueue() throws Exception {
      ClientSession session = addClientSession(sf.createSession(true, true, true));

      session.createQueue(new QueueConfiguration(FailoverTestBase.ADDRESS));
      return session;
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks));
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception {
      return addClientSession(sf1.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   private void createSessionFactory() throws Exception {
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setReconnectAttempts(15);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);
   }

   private void createSessionFactory2() throws Exception {
      sf2 = createSessionFactoryAndWaitForTopology(locator, 2);
   }
}
