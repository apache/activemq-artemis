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

import javax.jms.Message;
import javax.resource.ResourceException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.ra.inflow.ActiveMQActivationSpec;
import org.apache.activemq.artemis.tests.integration.ra.ActiveMQRATestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class ActiveMQMessageHandlerTest extends ActiveMQRATestBase {

   @Override
   protected boolean usePersistence() {
      return true;
   }

   @Override
   public boolean useSecurity() {
      return false;
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "interrupt",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQSessionContext",
         targetMethod = "xaEnd",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.ActiveMQMessageHandlerTest.interrupt();")})
   public void testSimpleMessageReceivedOnQueue() throws Exception {
      ActiveMQResourceAdapter qResourceAdapter = newResourceAdapter();
      resourceAdapter = qResourceAdapter;

      MyBootstrapContext ctx = new MyBootstrapContext();

      qResourceAdapter.setConnectorClassName(NETTY_CONNECTOR_FACTORY);
      qResourceAdapter.start(ctx);

      ActiveMQActivationSpec spec = new ActiveMQActivationSpec();
      spec.setMaxSession(1);
      spec.setCallTimeout(1000L);
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      CountDownLatch latch = new CountDownLatch(1);

      XADummyEndpoint endpoint = new XADummyEndpoint(latch, false);

      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, true);

      qResourceAdapter.endpointActivation(endpointFactory, spec);

      ClientSession session = locator.createSessionFactory().createSession();

      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);

      ClientMessage message = session.createMessage(true);

      message.getBodyBuffer().writeString("teststring");

      clientProducer.send(message);

      session.close();

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "teststring");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();

      Binding binding = server.getPostOffice().getBinding(SimpleString.toSimpleString(MDBQUEUEPREFIXED));
      assertEquals(1, getMessageCount(((Queue) binding.getBindable())));

      server.stop();
      server.start();

      ClientSessionFactory factory = locator.createSessionFactory();
      session = factory.createSession(true, true);

      session.start();
      ClientConsumer consumer = session.createConsumer(MDBQUEUEPREFIXED);
      assertNotNull(consumer.receive(5000));
      session.close();
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "interrupt",
         targetClass = "org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQSessionContext",
         targetMethod = "xaEnd",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.ActiveMQMessageHandlerTest.interrupt();")})
   public void testSimpleMessageReceivedOnQueueTwoPhase() throws Exception {
      ActiveMQResourceAdapter qResourceAdapter = newResourceAdapter();
      resourceAdapter = qResourceAdapter;

      MyBootstrapContext ctx = new MyBootstrapContext();

      qResourceAdapter.setConnectorClassName(NETTY_CONNECTOR_FACTORY);
      qResourceAdapter.start(ctx);

      ActiveMQActivationSpec spec = new ActiveMQActivationSpec();
      spec.setMaxSession(1);
      spec.setCallTimeout(1000L);
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);

      CountDownLatch latch = new CountDownLatch(1);

      XADummyEndpoint endpoint = new XADummyEndpoint(latch, true);

      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, true);

      qResourceAdapter.endpointActivation(endpointFactory, spec);

      ClientSession session = locator.createSessionFactory().createSession();

      ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);

      ClientMessage message = session.createMessage(true);

      message.getBodyBuffer().writeString("teststring");

      clientProducer.send(message);

      session.close();

      latch.await(5, TimeUnit.SECONDS);

      assertNotNull(endpoint.lastMessage);
      assertEquals(endpoint.lastMessage.getCoreMessage().getBodyBuffer().readString(), "teststring");

      qResourceAdapter.endpointDeactivation(endpointFactory, spec);

      qResourceAdapter.stop();

      Binding binding = server.getPostOffice().getBinding(SimpleString.toSimpleString(MDBQUEUEPREFIXED));
      assertEquals(1, getMessageCount(((Queue) binding.getBindable())));

      server.stop();
      server.start();

      ClientSessionFactory factory = locator.createSessionFactory();
      session = factory.createSession(true, true);

      session.start();
      ClientConsumer consumer = session.createConsumer(MDBQUEUEPREFIXED);
      assertNotNull(consumer.receive(5000));
      session.close();
   }

   static volatile ActiveMQResourceAdapter resourceAdapter;
   static boolean resourceAdapterStopped = false;

   public static void interrupt() throws InterruptedException {
      //Thread.currentThread().interrupt();
      if (!resourceAdapterStopped) {
         resourceAdapter.stop();
         resourceAdapterStopped = true;
         throw new InterruptedException("foo");
      }
      //Thread.currentThread().interrupt();
   }

   Transaction currentTX;

   public class XADummyEndpoint extends DummyMessageEndpoint {

      final boolean twoPhase;
      ClientSession session;
      int afterDeliveryCounts = 0;

      public XADummyEndpoint(CountDownLatch latch, boolean twoPhase) throws SystemException {
         super(latch);
         this.twoPhase = twoPhase;
         try {
            session = locator.createSessionFactory().createSession(true, false, false);
         } catch (Throwable e) {
            throw new RuntimeException(e);
         }
      }

      @Override
      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException {
         super.beforeDelivery(method);
         try {
            DummyTMLocator.tm.begin();
            currentTX = DummyTMLocator.tm.getTransaction();
            currentTX.enlistResource(xaResource);
            if (twoPhase) {
               currentTX.enlistResource(new DummyXAResource());
            }
         } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

      @Override
      public void onMessage(Message message) {
         super.onMessage(message);
         //         try
         //         {
         //            lastMessage = (ActiveMQMessage) message;
         //            currentTX.enlistResource(session);
         //            ClientProducer prod = session.createProducer()
         //         }
         //         catch (Exception e)
         //         {
         //            e.printStackTrace();
         //         }

      }

      @Override
      public void afterDelivery() throws ResourceException {
         afterDeliveryCounts++;
         try {
            currentTX.commit();
         } catch (Throwable e) {
            //its unsure as to whether the EJB/JCA layer will handle this or throw it to us,
            // either way we don't do anything else so its fine just to throw.
            // NB this will only happen with 2 phase commit
            throw new RuntimeException(e);
         }
         super.afterDelivery();
      }
   }

   @Override
   @Before
   public void setUp() throws Exception {
      resourceAdapter = null;
      resourceAdapterStopped = false;
      super.setUp();
      DummyTMLocator.startTM();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      DummyTMLocator.stopTM();
      super.tearDown();
   }

   public static class DummyTMLocator {

      public static TransactionManagerImple tm;

      public static void stopTM() {
         try {
            TransactionReaper.terminate(true);
            TxControl.disable(true);
         } catch (Exception e) {
            e.printStackTrace();
         }
         tm = null;
      }

      public static void startTM() {
         tm = new TransactionManagerImple();
         TxControl.enable();
      }

      public TransactionManager getTM() {
         return tm;
      }
   }

   static class DummyXAResource implements XAResource {

      @Override
      public void commit(Xid xid, boolean b) throws XAException {

      }

      @Override
      public void end(Xid xid, int i) throws XAException {

      }

      @Override
      public void forget(Xid xid) throws XAException {

      }

      @Override
      public int getTransactionTimeout() throws XAException {
         return 0;
      }

      @Override
      public boolean isSameRM(XAResource xaResource) throws XAException {
         return false;
      }

      @Override
      public int prepare(Xid xid) throws XAException {
         return 0;
      }

      @Override
      public Xid[] recover(int i) throws XAException {
         return new Xid[0];
      }

      @Override
      public void rollback(Xid xid) throws XAException {

      }

      @Override
      public boolean setTransactionTimeout(int i) throws XAException {
         return false;
      }

      @Override
      public void start(Xid xid, int i) throws XAException {

      }
   }
}
