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
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.apache.activemq.jms.tests.ActiveMQServerTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A stress test written to investigate http://jira.jboss.org/jira/browse/JBMESSAGING-362
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 */
public class CorruptMessageStressTest extends ActiveMQServerTestCase
{
   @BeforeClass
   public static void stressTestsEnabled()
   {
      org.junit.Assume.assumeTrue(JMSStressTestBase.STRESS_TESTS_ENABLED);
   }

   public static int PRODUCER_COUNT = 30;

   public static int MESSAGE_COUNT = 10000;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContext ic;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testMultipleSenders() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/StressTestQueue");
      drainDestination(cf, queue);

      Connection conn = cf.createConnection();

      Session[] sessions = new Session[CorruptMessageStressTest.PRODUCER_COUNT];
      MessageProducer[] producers = new MessageProducer[CorruptMessageStressTest.PRODUCER_COUNT];

      for (int i = 0; i < CorruptMessageStressTest.PRODUCER_COUNT; i++)
      {
         sessions[i] = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         producers[i] = sessions[i].createProducer(queue);
         producers[i].setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      }

      Thread[] threads = new Thread[CorruptMessageStressTest.PRODUCER_COUNT];

      for (int i = 0; i < CorruptMessageStressTest.PRODUCER_COUNT; i++)
      {
         threads[i] = new Thread(new Sender(sessions[i], producers[i]), "Sender Thread #" + i);
         threads[i].start();
      }

      // wait for the threads to finish

      for (int i = 0; i < CorruptMessageStressTest.PRODUCER_COUNT; i++)
      {
         threads[i].join();
      }

      conn.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      // ServerManagement.start("all");
      ic = getInitialContext();
      createQueue("StressTestQueue");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
      destroyQueue("StressTestQueue");
      ic.close();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class Sender implements Runnable
   {
      private final Session session;

      private final MessageProducer producer;

      private int count = 0;

      public Sender(final Session session, final MessageProducer producer)
      {
         this.session = session;
         this.producer = producer;
      }

      public void run()
      {
         while (true)
         {
            if (count == CorruptMessageStressTest.MESSAGE_COUNT)
            {
               break;
            }

            try
            {
               Message m = session.createMessage();
               m.setStringProperty("XXX", "XXX-VALUE");
               m.setStringProperty("YYY", "YYY-VALUE");
               producer.send(m);
               count++;
            }
            catch (Exception e)
            {
               log.error("Sender thread failed", e);
               break;
            }
         }
      }
   }
}
