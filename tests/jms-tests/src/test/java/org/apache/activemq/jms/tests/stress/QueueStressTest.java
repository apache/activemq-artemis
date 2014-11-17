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
package org.apache.activemq.jms.tests.stress;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A QueueStressTest.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2349 $</tt>
 */

public class QueueStressTest extends JMSStressTestBase
{
   @BeforeClass
   public static void stressTestsEnabled()
   {
      org.junit.Assume.assumeTrue(JMSStressTestBase.STRESS_TESTS_ENABLED);
   }

   /*
    * Stress a queue with transational, non transactional and 2pc senders sending both persistent
    * and non persistent messages
    * Transactional senders go through a cycle of sending and rolling back
    *
    */
   @Test
   public void testQueueMultipleSenders() throws Exception
   {
      Connection conn1 = cf.createConnection();

      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess2 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess3 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess4 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess5 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess6 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess7 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess8 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Session sess9 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess10 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess11 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess12 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess13 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess14 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess15 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess16 = conn1.createSession(true, Session.SESSION_TRANSACTED);

      XASession xaSess1 = ((XAConnection) conn1).createXASession();
      tweakXASession(xaSess1);
      XASession xaSess2 = ((XAConnection) conn1).createXASession();
      tweakXASession(xaSess2);
      XASession xaSess3 = ((XAConnection) conn1).createXASession();
      tweakXASession(xaSess3);
      XASession xaSess4 = ((XAConnection) conn1).createXASession();
      tweakXASession(xaSess4);
      XASession xaSess5 = ((XAConnection) conn1).createXASession();
      tweakXASession(xaSess5);
      XASession xaSess6 = ((XAConnection) conn1).createXASession();
      tweakXASession(xaSess6);
      XASession xaSess7 = ((XAConnection) conn1).createXASession();
      tweakXASession(xaSess7);
      XASession xaSess8 = ((XAConnection) conn1).createXASession();
      tweakXASession(xaSess8);

      Session sess17 = xaSess1.getSession();
      Session sess18 = xaSess2.getSession();
      Session sess19 = xaSess3.getSession();
      Session sess20 = xaSess4.getSession();
      Session sess21 = xaSess5.getSession();
      Session sess22 = xaSess6.getSession();
      Session sess23 = xaSess7.getSession();
      Session sess24 = xaSess8.getSession();

      MessageProducer prod1 = sess1.createProducer(destinationQueue1);
      prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod2 = sess2.createProducer(destinationQueue1);
      prod2.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod3 = sess3.createProducer(destinationQueue1);
      prod3.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod4 = sess4.createProducer(destinationQueue1);
      prod4.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod5 = sess5.createProducer(destinationQueue1);
      prod5.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod6 = sess6.createProducer(destinationQueue1);
      prod6.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod7 = sess7.createProducer(destinationQueue1);
      prod7.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod8 = sess8.createProducer(destinationQueue1);
      prod8.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod9 = sess9.createProducer(destinationQueue1);
      prod9.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod10 = sess10.createProducer(destinationQueue1);
      prod10.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod11 = sess11.createProducer(destinationQueue1);
      prod11.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod12 = sess12.createProducer(destinationQueue1);
      prod12.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod13 = sess13.createProducer(destinationQueue1);
      prod13.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod14 = sess14.createProducer(destinationQueue1);
      prod14.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod15 = sess15.createProducer(destinationQueue1);
      prod15.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod16 = sess16.createProducer(destinationQueue1);
      prod16.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod17 = sess17.createProducer(destinationQueue1);
      prod17.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod18 = sess18.createProducer(destinationQueue1);
      prod18.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod19 = sess19.createProducer(destinationQueue1);
      prod19.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod20 = sess20.createProducer(destinationQueue1);
      prod20.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod21 = sess21.createProducer(destinationQueue1);
      prod21.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod22 = sess22.createProducer(destinationQueue1);
      prod22.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod23 = sess23.createProducer(destinationQueue1);
      prod23.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod24 = sess24.createProducer(destinationQueue1);
      prod24.setDeliveryMode(DeliveryMode.PERSISTENT);

      Connection conn2 = cf.createConnection();
      conn2.start();
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = sessReceive.createConsumer(destinationQueue1);

      Runner[] runners = new Runner[]{
         new Sender("prod1", sess1, prod1, JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES),
         new Sender("prod2", sess2, prod2, JMSStressTestBase.NUM_PERSISTENT_MESSAGES),
         new Sender("prod3", sess3, prod3, JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES),
         new Sender("prod4", sess4, prod4, JMSStressTestBase.NUM_PERSISTENT_MESSAGES),
         new Sender("prod5", sess5, prod5, JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES),
         new Sender("prod6", sess6, prod6, JMSStressTestBase.NUM_PERSISTENT_MESSAGES),
         new Sender("prod7", sess7, prod7, JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES),
         new Sender("prod8", sess8, prod8, JMSStressTestBase.NUM_PERSISTENT_MESSAGES),
         new TransactionalSender("prod9",
                                 sess9,
                                 prod9,
                                 JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES,
                                 1,
                                 1),
         new TransactionalSender("prod10",
                                 sess10,
                                 prod10,
                                 JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                                 1,
                                 1),
         new TransactionalSender("prod11",
                                 sess11,
                                 prod11,
                                 JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES,
                                 10,
                                 7),
         new TransactionalSender("prod12",
                                 sess12,
                                 prod12,
                                 JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                                 10,
                                 7),
         new TransactionalSender("prod13",
                                 sess13,
                                 prod13,
                                 JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES,
                                 50,
                                 21),
         new TransactionalSender("prod14",
                                 sess14,
                                 prod14,
                                 JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                                 50,
                                 21),
         new TransactionalSender("prod15",
                                 sess15,
                                 prod15,
                                 JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES,
                                 100,
                                 67),
         new TransactionalSender("prod16",
                                 sess16,
                                 prod16,
                                 JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                                 100,
                                 67),
         new Transactional2PCSender("prod17",
                                    xaSess1,
                                    prod17,
                                    JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES,
                                    1,
                                    1),
         new Transactional2PCSender("prod18",
                                    xaSess2,
                                    prod18,
                                    JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                                    1,
                                    1),
         new Transactional2PCSender("prod19",
                                    xaSess3,
                                    prod19,
                                    JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES,
                                    10,
                                    7),
         new Transactional2PCSender("prod20",
                                    xaSess4,
                                    prod20,
                                    JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                                    10,
                                    7),
         new Transactional2PCSender("prod21",
                                    xaSess5,
                                    prod21,
                                    JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES,
                                    50,
                                    21),
         new Transactional2PCSender("prod22",
                                    xaSess6,
                                    prod22,
                                    JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                                    50,
                                    21),
         new Transactional2PCSender("prod23",
                                    xaSess7,
                                    prod23,
                                    JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES,
                                    100,
                                    67),
         new Transactional2PCSender("prod24",
                                    xaSess8,
                                    prod24,
                                    JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                                    100,
                                    67),
         new Receiver(sessReceive,
                      cons,
                      12 * JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + 12 * JMSStressTestBase.NUM_PERSISTENT_MESSAGES,
                      false)};

      runRunners(runners);

      conn1.close();

      conn2.close();
   }

}
