/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.byteman;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This test will force two consumers to be waiting on close and introduce a race I saw in a production system
 */
@RunWith(BMUnitRunner.class)
public class ConcurrentDeliveryCancelTest extends JMSTestBase {
   private static final Logger log = Logger.getLogger(ConcurrentDeliveryCancelTest.class);

   // used to wait the thread to align at the same place and create the race
   private static final ReusableLatch latchEnter = new ReusableLatch(2);
   // used to start
   private static final ReusableLatch latchFlag = new ReusableLatch(1);

   public static void enterCancel() {
      try {
         latchEnter.countDown();
         latchFlag.await(1, TimeUnit.SECONDS);
      } catch (Throwable ignored) {
         ignored.printStackTrace();
      }
   }

   public static void resetLatches(int numberOfThreads) {
      latchEnter.setCount(numberOfThreads);
      latchFlag.setCount(1);
   }

   @Override
   protected boolean usePersistence() {
      return true;
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "enterCancel-holdThere",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl",
         targetMethod = "close",
         targetLocation = "ENTRY",
         action = "org.apache.activemq.artemis.tests.extras.byteman.ConcurrentDeliveryCancelTest.enterCancel();")})
   public void testConcurrentCancels() throws Exception {

      log.debug(server.getConfiguration().getJournalLocation().toString());
      server.getAddressSettingsRepository().clear();
      AddressSettings settings = new AddressSettings();
      settings.setMaxDeliveryAttempts(-1);
      server.getAddressSettingsRepository().addMatch("#", settings);
      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactory("tcp://localhost:61616", "test");
      cf.setReconnectAttempts(0);
      cf.setRetryInterval(10);

      log.debug(".....");
      for (ServerSession srvSess : server.getSessions()) {
         log.debug(srvSess);
      }

      String queueName = RandomUtil.randomString();
      Queue queue = createQueue(queueName);

      int numberOfMessages = 100;

      {
         Connection connection = cf.createConnection();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(queue);

         for (int i = 0; i < numberOfMessages; i++) {
            TextMessage msg = session.createTextMessage("message " + i);
            msg.setIntProperty("i", i);
            producer.send(msg);
         }
         session.commit();

         connection.close();
      }

      for (int i = 0; i < 10; i++) {
         XAConnectionFactory xacf = ActiveMQJMSClient.createConnectionFactory("tcp://localhost:61616", "test");

         final XAConnection connection = xacf.createXAConnection();
         final XASession theSession = connection.createXASession();
         ((ActiveMQSession) theSession).getCoreSession().addMetaData("theSession", "true");

         connection.start();

         final MessageConsumer consumer = theSession.createConsumer(queue);

         XidImpl xid = newXID();
         theSession.getXAResource().start(xid, XAResource.TMNOFLAGS);
         theSession.getXAResource().setTransactionTimeout(1); // I'm setting a small timeout just because I'm lazy to call end myself

         for (int msg = 0; msg < 11; msg++) {
            Assert.assertNotNull(consumer.receiveNoWait());
         }

         log.debug(".....");

         final List<ServerSession> serverSessions = new LinkedList<>();

         // We will force now the failure simultaneously from several places
         for (ServerSession srvSess : server.getSessions()) {
            if (srvSess.getMetaData("theSession") != null) {
               log.debug(srvSess);
               serverSessions.add(srvSess);
            }
         }
         latchFlag.countDown();


         latchFlag.countUp();
         latchEnter.countUp();
         latchEnter.countUp();

         List<Thread> threads = new LinkedList<>();

         threads.add(new Thread("ConsumerCloser") {
            @Override
            public void run() {
               try {
                  log.debug(Thread.currentThread().getName() + " closing consumer");
                  consumer.close();
                  log.debug(Thread.currentThread().getName() + " closed consumer");
               } catch (Exception e) {
                  e.printStackTrace();
               }
            }
         });

         threads.add(new Thread("SessionCloser") {
            @Override
            public void run() {
               for (ServerSession sess : serverSessions) {
                  log.debug("Thread " + Thread.currentThread().getName() + " starting");
                  try {
                     // A session.close could sneak in through failover or some other scenarios.
                     // a call to RemotingConnection.fail wasn't replicating the issue.
                     // I needed to call Session.close() directly to replicate what was happening in production
                     sess.close(true);
                  } catch (Exception e) {
                     e.printStackTrace();
                  }
                  log.debug("Thread " + Thread.currentThread().getName() + " done");
               }
            }
         });

         for (Thread t : threads) {
            t.start();
         }

         latchEnter.await(1, TimeUnit.SECONDS);
         latchFlag.countDown();

         for (Thread t : threads) {
            t.join(5000);
            Assert.assertFalse(t.isAlive());
         }

         connection.close();
      }

      Connection connection = cf.createConnection();
      try {
         connection.setClientID("myID");

         Thread.sleep(5000); // I am too lazy to call end on all the transactions
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);
         HashMap<Integer, AtomicInteger> mapCount = new HashMap<>();

         while (true) {
            TextMessage message = (TextMessage) consumer.receiveNoWait();
            if (message == null) {
               break;
            }

            Integer value = message.getIntProperty("i");

            AtomicInteger count = mapCount.get(value);
            if (count == null) {
               count = new AtomicInteger(0);
               mapCount.put(message.getIntProperty("i"), count);
            }

            count.incrementAndGet();
         }

         boolean failed = false;
         for (int i = 0; i < numberOfMessages; i++) {
            AtomicInteger count = mapCount.get(i);
            if (count == null) {
               log.debug("Message " + i + " not received");
               failed = true;
            } else if (count.get() > 1) {
               log.debug("Message " + i + " received " + count.get() + " times");
               failed = true;
            }
         }

         Assert.assertFalse("test failed, look at the system.out of the test for more information", failed);
      } finally {
         connection.close();
      }

   }
}
