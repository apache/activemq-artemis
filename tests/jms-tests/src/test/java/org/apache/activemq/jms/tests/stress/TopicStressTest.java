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

import org.junit.Test;

/**
 * A TopicStressTest.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2349 $</tt>
 */
public class TopicStressTest extends JMSStressTestBase
{
   /*
    * Stress a topic with with many non transactional, transactional and 2pc receivers.
    * Non transactional receivers use ack modes of auto, dups and client ack.
    * Client ack receivers go through a cycle of receiving a batch, acking and recovering
    * Transactional receivers go through a cycle of receiving committing and rolling back.
    * Half the consumers are durable and half non durable.
    *
    */
   @Test
   public void testTopicMultipleReceivers() throws Exception
   {
      Connection conn1 = cf.createConnection();

      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess2 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod1 = sess1.createProducer(topic1);
      prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      MessageProducer prod2 = sess2.createProducer(topic1);
      prod2.setDeliveryMode(DeliveryMode.PERSISTENT);

      Connection conn2 = cf.createConnection();
      conn2.setClientID("clientid1");
      conn2.start();

      // 4 auto ack
      Session rsess1 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session rsess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session rsess3 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session rsess4 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // 4 dups
      Session rsess5 = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      Session rsess6 = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      Session rsess7 = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      Session rsess8 = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

      // 4 client
      Session rsess9 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session rsess10 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session rsess11 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session rsess12 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      // 4 transactional
      Session rsess13 = conn2.createSession(true, Session.SESSION_TRANSACTED);
      Session rsess14 = conn2.createSession(true, Session.SESSION_TRANSACTED);
      Session rsess15 = conn2.createSession(true, Session.SESSION_TRANSACTED);
      Session rsess16 = conn2.createSession(true, Session.SESSION_TRANSACTED);

      // 4 2pc transactional
      XASession rxaSess1 = ((XAConnection) conn2).createXASession();
      tweakXASession(rxaSess1);
      XASession rxaSess2 = ((XAConnection) conn2).createXASession();
      tweakXASession(rxaSess2);
      XASession rxaSess3 = ((XAConnection) conn2).createXASession();
      tweakXASession(rxaSess3);
      XASession rxaSess4 = ((XAConnection) conn2).createXASession();
      tweakXASession(rxaSess4);

      Session rsess17 = rxaSess1.getSession();
      Session rsess18 = rxaSess2.getSession();
      Session rsess19 = rxaSess3.getSession();
      Session rsess20 = rxaSess4.getSession();

      MessageConsumer cons1 = rsess1.createConsumer(topic1);
      MessageConsumer cons2 = rsess2.createDurableSubscriber(topic1, "sub1");
      MessageConsumer cons3 = rsess3.createConsumer(topic1);
      MessageConsumer cons4 = rsess4.createDurableSubscriber(topic1, "sub2");
      MessageConsumer cons5 = rsess5.createConsumer(topic1);
      MessageConsumer cons6 = rsess6.createDurableSubscriber(topic1, "sub3");
      MessageConsumer cons7 = rsess7.createConsumer(topic1);
      MessageConsumer cons8 = rsess8.createDurableSubscriber(topic1, "sub4");
      MessageConsumer cons9 = rsess9.createConsumer(topic1);
      MessageConsumer cons10 = rsess10.createDurableSubscriber(topic1, "sub5");
      MessageConsumer cons11 = rsess11.createConsumer(topic1);
      MessageConsumer cons12 = rsess12.createDurableSubscriber(topic1, "sub6");
      MessageConsumer cons13 = rsess13.createConsumer(topic1);
      MessageConsumer cons14 = rsess14.createDurableSubscriber(topic1, "sub7");
      MessageConsumer cons15 = rsess15.createConsumer(topic1);
      MessageConsumer cons16 = rsess16.createDurableSubscriber(topic1, "sub8");
      MessageConsumer cons17 = rsess17.createConsumer(topic1);
      MessageConsumer cons18 = rsess18.createDurableSubscriber(topic1, "sub9");
      MessageConsumer cons19 = rsess19.createConsumer(topic1);
      MessageConsumer cons20 = rsess20.createDurableSubscriber(topic1, "sub10");

      // To make sure paging occurs first send some messages before receiving

      Runner[] runners = new Runner[]{
         new Sender("prod1", sess1, prod1, JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND),
         new Sender("prod2", sess2, prod2, JMSStressTestBase.NUM_PERSISTENT_PRESEND)};

      runRunners(runners);

      runners = new Runner[]{
         // 4 auto ack
         new Receiver(rsess1,
                      cons1,
                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                      false),
         new Receiver(rsess2,
                      cons2,
                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                      true),
         new Receiver(rsess3,
                      cons3,
                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                      false),
         new Receiver(rsess4,
                      cons4,
                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                      true),

         // 4 dups ok
         new Receiver(rsess5,
                      cons5,
                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                      false),
         new Receiver(rsess6,
                      cons6,
                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                      true),
         new Receiver(rsess7,
                      cons7,
                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                      false),
         new Receiver(rsess8,
                      cons8,
                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                      true),

         // 4 client ack
         new RecoveringReceiver(rsess9,
                                cons9,
                                JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                   JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                   JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                1,
                                1,
                                false),
         new RecoveringReceiver(rsess10,
                                cons10,
                                JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                   JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                   JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                10,
                                7,
                                true),
         new RecoveringReceiver(rsess11,
                                cons11,
                                JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                   JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                   JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                50,
                                21,
                                false),
         new RecoveringReceiver(rsess12,
                                cons12,
                                JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                   JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                   JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                100,
                                67,
                                true),

         // 4 transactional

         new TransactionalReceiver(rsess13,
                                   cons13,
                                   JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                      JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                      JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                   1,
                                   1,
                                   false),
         new TransactionalReceiver(rsess14,
                                   cons14,
                                   JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                      JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                      JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                   10,
                                   7,
                                   true),
         new TransactionalReceiver(rsess15,
                                   cons15,
                                   JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                      JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                      JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                   50,
                                   21,
                                   false),
         new TransactionalReceiver(rsess16,
                                   cons16,
                                   JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                      JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                      JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                   100,
                                   67,
                                   true),

         // 4 2pc transactional
         new Transactional2PCReceiver(rxaSess1,
                                      cons17,
                                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                      1,
                                      1,
                                      false),
         new Transactional2PCReceiver(rxaSess2,
                                      cons18,
                                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                      10,
                                      7,
                                      true),
         new Transactional2PCReceiver(rxaSess3,
                                      cons19,
                                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                      50,
                                      21,
                                      false),
         new Transactional2PCReceiver(rxaSess4,
                                      cons20,
                                      JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES + JMSStressTestBase.NUM_PERSISTENT_MESSAGES +
                                         JMSStressTestBase.NUM_NON_PERSISTENT_PRESEND +
                                         JMSStressTestBase.NUM_PERSISTENT_PRESEND,
                                      100,
                                      67,
                                      true),

         new Sender("prod3", sess1, prod1, JMSStressTestBase.NUM_NON_PERSISTENT_MESSAGES),
         new Sender("prod4", sess2, prod2, JMSStressTestBase.NUM_PERSISTENT_MESSAGES)};

      runRunners(runners);

      conn1.close();

      conn2.close();
   }

}
