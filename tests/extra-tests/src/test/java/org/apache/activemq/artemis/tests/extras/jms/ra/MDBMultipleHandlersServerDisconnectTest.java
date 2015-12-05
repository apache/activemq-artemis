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
package org.apache.activemq.artemis.tests.extras.jms.ra;

import javax.jms.Message;
import javax.resource.ResourceException;
import javax.resource.spi.LocalTransactionException;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.ra.inflow.ActiveMQActivation;
import org.apache.activemq.artemis.ra.inflow.ActiveMQActivationSpec;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.ra.ActiveMQRATestBase;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Simulates several messages being received over multiple instances with reconnects during the process.
 */
public class MDBMultipleHandlersServerDisconnectTest extends ActiveMQRATestBase {

   final ConcurrentHashMap<Integer, AtomicInteger> mapCounter = new ConcurrentHashMap<Integer, AtomicInteger>();

   volatile ActiveMQResourceAdapter resourceAdapter;

   ServerLocator nettyLocator;

   private volatile boolean playTXTimeouts = true;
   private volatile boolean playServerClosingSession = true;
   private volatile boolean playServerClosingConsumer = true;

   @Override
   @Before
   public void setUp() throws Exception {
      nettyLocator = createNettyNonHALocator();
      nettyLocator.setRetryInterval(10);
      nettyLocator.setReconnectAttempts(-1);
      mapCounter.clear();
      resourceAdapter = null;
      super.setUp();
      createQueue(true, "outQueue");
      DummyTMLocator.startTM();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      DummyTMLocator.stopTM();
      super.tearDown();
   }

   @Override
   protected boolean usePersistence() {
      return true;
   }

   @Override
   public boolean useSecurity() {
      return false;
   }

   @Test
   public void testReconnectMDBNoMessageLoss() throws Exception {
      AddressSettings settings = new AddressSettings();
      settings.setRedeliveryDelay(100);
      settings.setMaxDeliveryAttempts(-1);
      server.getAddressSettingsRepository().clear();
      server.getAddressSettingsRepository().addMatch("#", settings);
      ActiveMQResourceAdapter qResourceAdapter = newResourceAdapter();
      resourceAdapter = qResourceAdapter;

      //      qResourceAdapter.setTransactionManagerLocatorClass(DummyTMLocator.class.getName());
      //      qResourceAdapter.setTransactionManagerLocatorMethod("getTM");

      MyBootstrapContext ctx = new MyBootstrapContext();

      qResourceAdapter.setConnectorClassName(NETTY_CONNECTOR_FACTORY);
      qResourceAdapter.start(ctx);

      final int NUMBER_OF_SESSIONS = 10;

      ActiveMQActivationSpec spec = new ActiveMQActivationSpec();
      spec.setMaxSession(NUMBER_OF_SESSIONS);
      spec.setTransactionTimeout(1);
      spec.setReconnectAttempts(-1);
      spec.setConfirmationWindowSize(-1);
      spec.setReconnectInterval(1000);
      spec.setCallTimeout(1000L);
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setConsumerWindowSize(1024 * 1024);

      TestEndpointFactory endpointFactory = new TestEndpointFactory(true);
      qResourceAdapter.endpointActivation(endpointFactory, spec);

      Assert.assertEquals(1, resourceAdapter.getActivations().values().size());
      ActiveMQActivation activation = resourceAdapter.getActivations().values().toArray(new ActiveMQActivation[1])[0];

      final int NUMBER_OF_MESSAGES = 1000;

      Thread producer = new Thread() {
         @Override
         public void run() {
            try {
               ServerLocator locator = createInVMLocator(0);
               ClientSessionFactory factory = locator.createSessionFactory();
               ClientSession session = factory.createSession(false, false);

               ClientProducer clientProducer = session.createProducer(MDBQUEUEPREFIXED);

               for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {

                  ClientMessage message = session.createMessage(true);

                  message.getBodyBuffer().writeString("teststring " + i);
                  message.putIntProperty("i", i);

                  clientProducer.send(message);

                  if (i % 100 == 0) {
                     session.commit();
                  }
               }
               session.commit();
            }
            catch (Exception e) {
               e.printStackTrace();
            }

         }
      };

      producer.start();

      final AtomicBoolean metaDataFailed = new AtomicBoolean(false);

      // This thread will keep bugging the handlers.
      // if they behave well with XA, the test pass!
      final AtomicBoolean running = new AtomicBoolean(true);

      Thread buggerThread = new Thread() {
         @Override
         public void run() {
            while (running.get()) {
               try {
                  Thread.sleep(RandomUtil.randomInterval(100, 200));
               }
               catch (InterruptedException intex) {
                  intex.printStackTrace();
                  return;
               }

               List<ServerSession> serverSessions = lookupServerSessions("resource-adapter");

               System.err.println("Contains " + serverSessions.size() + " RA sessions");

               if (serverSessions.size() != NUMBER_OF_SESSIONS) {
                  System.err.println("the server was supposed to have " + NUMBER_OF_MESSAGES + " RA Sessions but it only contained accordingly to the meta-data");
                  metaDataFailed.set(true);
               }
               else if (serverSessions.size() == NUMBER_OF_SESSIONS) {
                  // it became the same after some reconnect? which would be acceptable
                  metaDataFailed.set(false);
               }

               if (playServerClosingSession && serverSessions.size() > 0) {

                  int randomBother = RandomUtil.randomInterval(0, serverSessions.size() - 1);
                  System.out.println("bugging session " + randomBother);

                  ServerSession serverSession = serverSessions.get(randomBother);

                  if (playServerClosingConsumer && RandomUtil.randomBoolean()) {
                     // will play this randomly, only half of the times
                     for (ServerConsumer consumer : serverSession.getServerConsumers()) {
                        try {
                           // Simulating a rare race that could happen in production
                           // where the consumer is closed while things are still happening
                           consumer.close(true);
                           Thread.sleep(100);
                        }
                        catch (Exception e) {
                           e.printStackTrace();
                        }
                     }
                  }

                  RemotingConnection connection = serverSession.getRemotingConnection();

                  connection.fail(new ActiveMQException("failed at random " + randomBother));
               }
            }

         }
      };

      buggerThread.start();

      ServerLocator locator = createInVMLocator(0);
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, false);
      session.start();

      ClientConsumer consumer = session.createConsumer("jms.queue.outQueue");

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         ClientMessage message = consumer.receive(60000);
         if (message == null) {
            break;
         }

         if (i == NUMBER_OF_MESSAGES * 0.90) {
            System.out.println("Disabled failures at " + i);
            playTXTimeouts = false;
            playServerClosingSession = false;
            playServerClosingConsumer = false;

         }

         System.out.println("Received " + i + " messages");

         Assert.assertNotNull(message);
         message.acknowledge();

         Integer value = message.getIntProperty("i");
         AtomicInteger mapCount = new AtomicInteger(1);

         mapCount = mapCounter.putIfAbsent(value, mapCount);

         if (mapCount != null) {
            mapCount.incrementAndGet();
         }

         if (i % 200 == 0) {
            System.out.println("received " + i);
            session.commit();
         }
      }

      session.commit();
      Assert.assertNull(consumer.receiveImmediate());

      StringWriter writer = new StringWriter();
      PrintWriter out = new PrintWriter(writer);

      boolean failed = false;
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         AtomicInteger atomicInteger = mapCounter.get(Integer.valueOf(i));

         if (atomicInteger == null) {
            out.println("didn't receive message with i=" + i);
            failed = true;
         }
         else if (atomicInteger.get() > 1) {
            out.println("message with i=" + i + " received " + atomicInteger.get() + " times");
            failed = true;
         }
      }

      running.set(false);

      buggerThread.join();
      producer.join();

      qResourceAdapter.stop();

      session.close();

      if (failed) {
         for (int i = 0; i < 10; i++) {
            System.out.println("----------------------------------------------------");
         }
         System.out.println(writer.toString());
      }

      Assert.assertFalse(failed);

      System.out.println("Received " + NUMBER_OF_MESSAGES + " messages");

      Assert.assertFalse("There was meta-data failures, some sessions didn't reconnect properly", metaDataFailed.get());

   }

   private List<ServerSession> lookupServerSessions(String parameter) {
      List<ServerSession> serverSessions = new LinkedList<ServerSession>();

      for (ServerSession session : server.getSessions()) {
         if (session.getMetaData(parameter) != null) {
            serverSessions.add(session);
         }
      }
      return serverSessions;
   }

   protected class TestEndpointFactory implements MessageEndpointFactory {

      private final boolean isDeliveryTransacted;

      public TestEndpointFactory(boolean deliveryTransacted) {
         isDeliveryTransacted = deliveryTransacted;
      }

      @Override
      public MessageEndpoint createEndpoint(XAResource xaResource) throws UnavailableException {
         TestEndpoint retEnd = new TestEndpoint();
         if (xaResource != null) {
            retEnd.setXAResource(xaResource);
         }
         return retEnd;
      }

      @Override
      public boolean isDeliveryTransacted(Method method) throws NoSuchMethodException {
         return isDeliveryTransacted;
      }
   }

   public class TestEndpoint extends DummyMessageEndpoint {

      ClientSessionFactory factory;
      ClientSession endpointSession;
      ClientProducer producer;

      Transaction currentTX;

      public TestEndpoint() {
         super(null);
         try {
            factory = nettyLocator.createSessionFactory();
            //            buggingList.add(factory);
            endpointSession = factory.createSession(true, false, false);
            producer = endpointSession.createProducer("jms.queue.outQueue");
         }
         catch (Throwable e) {
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
         }
         catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
         }

      }

      @Override
      public void onMessage(Message message) {
         Integer value = 0;

         try {
            value = message.getIntProperty("i");
         }
         catch (Exception e) {

         }

         super.onMessage(message);

         try {
            currentTX.enlistResource(endpointSession);
            ClientMessage message1 = endpointSession.createMessage(true);
            message1.putIntProperty("i", value);
            producer.send(message1);
            currentTX.delistResource(endpointSession, XAResource.TMSUCCESS);

            if (playTXTimeouts) {
               if (RandomUtil.randomInterval(0, 5) == 3) {
                  Thread.sleep(2000);
               }
            }
         }
         catch (Exception e) {
            e.printStackTrace();
            try {
               currentTX.setRollbackOnly();
            }
            catch (Exception ex) {
            }
            e.printStackTrace();
            //            throw new RuntimeException(e);
         }
      }

      @Override
      public void afterDelivery() throws ResourceException {
         // This is a copy & paste of what the Application server would do here
         try {
            if (currentTX.getStatus() == Status.STATUS_MARKED_ROLLBACK) {
               DummyTMLocator.tm.rollback();
            }
            else {
               DummyTMLocator.tm.commit();
            }
         }
         catch (HeuristicMixedException e) {
            throw new LocalTransactionException(e);
         }
         catch (SystemException e) {
            throw new LocalTransactionException(e);
         }
         catch (HeuristicRollbackException e) {
            throw new LocalTransactionException(e);
         }
         catch (RollbackException e) {
            throw new LocalTransactionException(e);
         }
         super.afterDelivery();
      }
   }

   public static class DummyTMLocator {

      public static TransactionManagerImple tm;

      public static void stopTM() {
         try {
            TransactionReaper.terminate(true);
            TxControl.disable(true);
         }
         catch (Exception e) {
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
}
