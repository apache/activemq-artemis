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
package org.apache.activemq6.jms.tests.stress;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.apache.activemq6.jms.tests.HornetQServerTestCase;
import org.apache.activemq6.jms.tests.JmsTestLogger;
import org.apache.activemq6.jms.tests.util.ProxyAssertSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Send messages to a topic with selector1, consumer them with multiple consumers and relay them
 * back to the topic with a different selector, then consume that with more consumers.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class RelayStressTest extends HornetQServerTestCase
{
   @BeforeClass
   public static void stressTestsEnabled()
   {
      org.junit.Assume.assumeTrue(JMSStressTestBase.STRESS_TESTS_ENABLED);
   }

   // Constants -----------------------------------------------------

   private static JmsTestLogger log = JmsTestLogger.LOGGER;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContext ic;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      // ServerManagement.start("all");
      ic = getInitialContext();
      createTopic("StressTestTopic");

      RelayStressTest.log.debug("setup done");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      destroyTopic("StressTestTopic");
      ic.close();
   }

   @Test
   public void testRelay() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Topic topic = (Topic)ic.lookup("/topic/StressTestTopic");

      final int numMessages = 20000;

      final int numRelayers = 5;

      final int numConsumers = 20;

      Connection conn = cf.createConnection();

      class Relayer implements MessageListener
      {
         boolean done;

         boolean failed;

         int count;

         MessageProducer prod;

         Relayer(final MessageProducer prod)
         {
            this.prod = prod;
         }

         public void onMessage(final Message m)
         {
            try
            {
               m.clearProperties();
               m.setStringProperty("name", "Tim");

               prod.send(m);

               count++;

               if (count == numMessages)
               {
                  synchronized (this)
                  {
                     done = true;
                     notify();
                  }
               }
            }
            catch (JMSException e)
            {
               e.printStackTrace();
               synchronized (this)
               {
                  done = true;
                  failed = true;
                  notify();
               }
            }
         }
      }

      class Consumer implements MessageListener
      {
         boolean failed;

         boolean done;

         int count;

         public void onMessage(final Message m)
         {
            count++;

            if (count == numMessages * numRelayers)
            {
               synchronized (this)
               {
                  done = true;
                  notify();
               }
            }
         }
      }

      Relayer[] relayers = new Relayer[numRelayers];

      Consumer[] consumers = new Consumer[numConsumers];

      for (int i = 0; i < numRelayers; i++)
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sess.createConsumer(topic, "name = 'Watt'");
         // MessageConsumer cons = sess.createConsumer(topic);

         MessageProducer prod = sess.createProducer(topic);

         relayers[i] = new Relayer(prod);

         cons.setMessageListener(relayers[i]);
      }

      for (int i = 0; i < numConsumers; i++)
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sess.createConsumer(topic, "name = 'Tim'");

         consumers[i] = new Consumer();

         cons.setMessageListener(consumers[i]);
      }

      conn.start();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = sess.createProducer(topic);

      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      for (int i = 0; i < numMessages; i++)
      {
         Message m = sess.createMessage();

         m.setStringProperty("name", "Watt");

         prod.send(m);
      }

      for (int i = 0; i < numRelayers; i++)
      {
         synchronized (relayers[i])
         {
            if (!relayers[i].done)
            {
               relayers[i].wait();
            }
         }
      }

      for (int i = 0; i < numConsumers; i++)
      {
         synchronized (consumers[i])
         {
            if (!consumers[i].done)
            {
               consumers[i].wait();
            }
         }
      }

      conn.close();

      for (int i = 0; i < numRelayers; i++)
      {
         ProxyAssertSupport.assertFalse(relayers[i].failed);
      }

      for (int i = 0; i < numConsumers; i++)
      {
         ProxyAssertSupport.assertFalse(consumers[i].failed);
      }

   }
}
