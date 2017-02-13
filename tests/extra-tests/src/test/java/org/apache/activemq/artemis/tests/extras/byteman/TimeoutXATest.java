/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
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
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class TimeoutXATest extends ActiveMQTestBase {

   protected ActiveMQServer server = null;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(createDefaultNettyConfig());
      server.getConfiguration().setTransactionTimeout(1000);
      server.getConfiguration().setTransactionTimeoutScanPeriod(1100);
      server.getConfiguration().setJournalSyncNonTransactional(false);
      server.start();
      server.createQueue(SimpleString.toSimpleString("Queue1"), RoutingType.ANYCAST, SimpleString.toSimpleString("Queue1"), null, true, false);

      removingTXEntered0 = new CountDownLatch(1);
      removingTXAwait0 = new CountDownLatch(1);
      removingTXEntered1 = new CountDownLatch(1);
      removingTXAwait1 = new CountDownLatch(1);
      entered = 0;

      enteredRollback = 0;
      enteredRollbackLatch = new CountDownLatch(1);
      waitingRollbackLatch = new CountDownLatch(1);
   }

   static int entered;
   static CountDownLatch removingTXEntered0;
   static CountDownLatch removingTXAwait0;
   static CountDownLatch removingTXEntered1;
   static CountDownLatch removingTXAwait1;

   static int enteredRollback;
   static CountDownLatch enteredRollbackLatch;
   static CountDownLatch waitingRollbackLatch;

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "removing TX",
         targetClass = "org.apache.activemq.artemis.core.transaction.impl.ResourceManagerImpl",
         targetMethod = "removeTransaction(javax.transaction.xa.Xid)",
         targetLocation = "ENTRY",
         helper = "org.apache.activemq.artemis.tests.extras.byteman.TimeoutXATest",
         action = "removingTX()"), @BMRule(
         name = "afterRollback TX",
         targetClass = "org.apache.activemq.artemis.core.transaction.impl.TransactionImpl",
         targetMethod = "afterRollback",
         targetLocation = "ENTRY",
         helper = "org.apache.activemq.artemis.tests.extras.byteman.TimeoutXATest",
         action = "afterRollback()")})
   public void testTimeoutOnTX2() throws Exception {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
      XAConnection connection = connectionFactory.createXAConnection();

      Connection connction2 = connectionFactory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Queue queue = session.createQueue("Queue1");

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         producer.send(session.createTextMessage("hello " + 1));
      }

      session.commit();

      final XASession xaSession = connection.createXASession();
      final Xid xid = newXID();

      xaSession.getXAResource().start(xid, XAResource.TMNOFLAGS);
      MessageConsumer consumer = xaSession.createConsumer(queue);
      connection.start();
      for (int i = 0; i < 10; i++) {
         Assert.assertNotNull(consumer.receive(5000));
      }
      xaSession.getXAResource().end(xid, XAResource.TMSUCCESS);

      final CountDownLatch latchStore = new CountDownLatch(1000);

      Thread storingThread = new Thread() {
         @Override
         public void run() {
            try {
               for (int i = 0; i < 100000; i++) {
                  latchStore.countDown();
                  server.getStorageManager().storeDuplicateID(SimpleString.toSimpleString("crebis"), new byte[]{1}, server.getStorageManager().generateID());
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };

      removingTXEntered0.await();

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               xaSession.getXAResource().rollback(xid);
            } catch (Exception e) {
               e.printStackTrace();
            }

         }
      };

      t.start();

      removingTXEntered1.await();

      storingThread.start();
      latchStore.await();

      removingTXAwait1.countDown();

      Thread.sleep(1000);
      removingTXAwait0.countDown();

      Assert.assertTrue(enteredRollbackLatch.await(10, TimeUnit.SECONDS));

      waitingRollbackLatch.countDown();

      t.join();

      consumer.close();

      consumer = session.createConsumer(queue);
      for (int i = 0; i < 10; i++) {
         Assert.assertNotNull(consumer.receive(5000));
      }
      Assert.assertNull(consumer.receiveNoWait());

      connection.close();
      connction2.close();

   }

   public void afterRollback() {
      if (enteredRollback++ == 0) {
         enteredRollbackLatch.countDown();
         try {
            waitingRollbackLatch.await();
         } catch (Throwable e) {

         }
      }

   }

   public void removingTX() {
      int xent = entered++;

      if (xent == 0) {
         removingTXEntered0.countDown();
         try {
            removingTXAwait0.await();
         } catch (Throwable ignored) {
            ignored.printStackTrace();
         }
      } else if (xent == 1) {
         removingTXEntered1.countDown();
         try {
            removingTXAwait1.await();
         } catch (Throwable ignored) {
            ignored.printStackTrace();
         }
      }

   }
}
