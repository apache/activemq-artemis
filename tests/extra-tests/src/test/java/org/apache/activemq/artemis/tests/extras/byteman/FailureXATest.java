/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.byteman;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class FailureXATest extends ActiveMQTestBase {

   protected ActiveMQServer server = null;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(createDefaultNettyConfig());
      server.start();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (server != null) {
         server.stop();
      }
      super.tearDown();
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "Crash after onephase committed",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ServerSessionImpl",
         targetMethod = "xaCommit(javax.transaction.xa.Xid, boolean)",
         targetLocation = "EXIT",
         action = "throw new RuntimeException()")})
   public void testCrashServerAfterOnePhaseCommit() throws Exception {
      doTestCrashServerAfterXACommit(true);
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "Crash after onephase committed",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ServerSessionImpl",
         targetMethod = "xaCommit(javax.transaction.xa.Xid, boolean)",
         targetLocation = "EXIT",
         //helper = "org.apache.activemq.artemis.tests.extras.byteman.FailureXATest",
         action = "throw new RuntimeException()")})
   public void testCrashServerAfterTwoPhaseCommit() throws Exception {
      doTestCrashServerAfterXACommit(false);
   }

   private void doTestCrashServerAfterXACommit(boolean onePhase) throws Exception {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
      XAConnection connection = connectionFactory.createXAConnection();

      try {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("Queue1");
         final XASession xaSession = connection.createXASession();
         MessageConsumer consumer = xaSession.createConsumer(queue);

         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("hello " + 1));
         session.commit();

         XAResource xaResource = xaSession.getXAResource();
         final Xid xid = newXID();
         xaResource.start(xid, XAResource.TMNOFLAGS);

         connection.start();
         Assert.assertNotNull(consumer.receive(5000));

         xaResource.end(xid, XAResource.TMSUCCESS);

         try {
            xaResource.commit(xid, onePhase);
            Assert.fail("didn't get expected exception!");
         } catch (XAException xae) {
            if (onePhase) {
               //expected error code is XAER_RMFAIL
               Assert.assertEquals(XAException.XAER_RMFAIL, xae.errorCode);
            } else {
               //expected error code is XA_RETRY
               Assert.assertEquals(XAException.XA_RETRY, xae.errorCode);
            }
         }
      } finally {
         connection.close();
      }
   }

}
