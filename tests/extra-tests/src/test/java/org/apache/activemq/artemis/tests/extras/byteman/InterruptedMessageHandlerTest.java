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
import com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
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
public class InterruptedMessageHandlerTest extends ActiveMQRATestBase {

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
         name = "throw ActiveMQException(CONNETION_TIMEOUT) during rollback",
         targetClass = "org.apache.activemq.artemis.core.client.impl.ClientSessionImpl",
         targetMethod = "flushAcks",
         targetLocation = "AFTER INVOKE flushAcks",
         action = "org.apache.activemq.artemis.tests.extras.byteman.InterruptedMessageHandlerTest.throwActiveMQQExceptionConnectionTimeout();"), @BMRule(
         name = "check that outcome of XA transaction is TwoPhaseOutcome.FINISH_ERROR=8",
         targetClass = "com.arjuna.ats.internal.jta.resources.arjunacore.XAResourceRecord",
         targetMethod = "topLevelAbort",
         targetLocation = "AT EXIT",
         action = "org.apache.activemq.artemis.tests.extras.byteman.InterruptedMessageHandlerTest.assertTxOutComeIsOfStatusFinishedError($!);")})
   public void testSimpleMessageReceivedOnQueueTwoPhaseFailPrepareByConnectionTimout() throws Exception {
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

      XADummyEndpointWithDummyXAResourceFailEnd endpoint = new XADummyEndpointWithDummyXAResourceFailEnd(latch, true);

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

      server.stop();

      assertEquals("Two phase outcome must be of TwoPhaseOutcome.FINISH_ERROR.", TwoPhaseOutcome.FINISH_ERROR, txTwoPhaseOutCome.intValue());

   }

   static volatile ActiveMQResourceAdapter resourceAdapter;
   static boolean resourceAdapterStopped = false;

   public static void interrupt() throws InterruptedException {
      if (!resourceAdapterStopped) {
         resourceAdapter.stop();
         resourceAdapterStopped = true;
         throw new InterruptedException("foo");
      }
   }

   public static void throwActiveMQQExceptionConnectionTimeout() throws ActiveMQException, XAException {
      StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
      for (StackTraceElement element : stackTraceElements) {
         if (element.getClassName().contains("ClientSessionImpl") && element.getMethodName().contains("rollback")) {
            throw new ActiveMQException(ActiveMQExceptionType.CONNECTION_TIMEDOUT);
         }
      }
   }

   static Integer txTwoPhaseOutCome = null;

   public static void assertTxOutComeIsOfStatusFinishedError(int txOutCome) {
      // check only first trigger of byteman rule
      if (txTwoPhaseOutCome == null) {
         txTwoPhaseOutCome = Integer.valueOf(txOutCome);
      }
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
      }

      @Override
      public void afterDelivery() throws ResourceException {
         afterDeliveryCounts++;
         try {
            currentTX.commit();
         } catch (Throwable e) {
            // its unsure as to whether the EJB/JCA layer will handle this or throw it to us,
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

   public class XADummyEndpointWithDummyXAResourceFailEnd extends XADummyEndpoint {

      public XADummyEndpointWithDummyXAResourceFailEnd(CountDownLatch latch, boolean twoPhase) throws SystemException {
         super(latch, twoPhase);
      }

      @Override
      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException {
         try {
            DummyTMLocator.tm.begin();
            currentTX = DummyTMLocator.tm.getTransaction();
            currentTX.enlistResource(xaResource);
            if (twoPhase) {
               currentTX.enlistResource(new DummyXAResourceFailEnd());
            }
         } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

   }

   static class DummyXAResourceFailEnd extends DummyXAResource {

      @Override
      public void end(Xid xid, int i) throws XAException {
         throw new XAException(XAException.XAER_RMFAIL);
      }
   }
}
