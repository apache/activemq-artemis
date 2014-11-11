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
package org.apache.activemq6.jms.tests;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.api.jms.JMSFactoryType;
import org.apache.activemq6.jms.client.HornetQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Safeguards for previously detected TCK failures.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 *
 */
public class CTSMiscellaneousTest extends JMSTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   protected static HornetQConnectionFactory cf;

   private static final String ORG_JBOSS_MESSAGING_SERVICE_LBCONNECTION_FACTORY = "StrictTCKConnectionFactory";

   // Constructors --------------------------------------------------
   @Override
   @Before
   public void setUp() throws Exception
   {
      try
      {
         super.setUp();
         // Deploy a connection factory with load balancing but no failover on node0
         List<String> bindings = new ArrayList<String>();
         bindings.add("StrictTCKConnectionFactory");

         getJmsServerManager().createConnectionFactory("StrictTCKConnectionFactory",
                                                       false,
                                                       JMSFactoryType.CF,
                                                       NETTY_CONNECTOR,
                                                       null,
                                                       HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                       HornetQClient.DEFAULT_CONNECTION_TTL,
                                                       HornetQClient.DEFAULT_CALL_TIMEOUT,
                                                       HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                                                       HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                                       HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                       HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES,
                                                       HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                       HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                                       HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                       HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                                       HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                                       true,
                                                       true,
                                                       true,
                                                       HornetQClient.DEFAULT_AUTO_GROUP,
                                                       HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                                       HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                       HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                       HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                       HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                                       HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                       HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                                       HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                       HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                       HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                       HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                                       HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                                       null,
                                                       "/StrictTCKConnectionFactory");

         CTSMiscellaneousTest.cf = (HornetQConnectionFactory)getInitialContext().lookup("/StrictTCKConnectionFactory");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   // Public --------------------------------------------------------

   /* By default we send non persistent messages asynchronously for performance reasons
    * when running with strictTCK we send them synchronously
    */
   @Test
   public void testNonPersistentMessagesSentSynchronously() throws Exception
   {
      Connection c = null;

      try
      {
         c = CTSMiscellaneousTest.cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer p = s.createProducer(queue1);

         p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         final int numMessages = 100;

         assertRemainingMessages(0);

         for (int i = 0; i < numMessages; i++)
         {
            p.send(s.createMessage());
         }

         assertRemainingMessages(numMessages);
      }
      finally
      {
         if (c != null)
         {
            c.close();
         }

         removeAllMessages(queue1.getQueueName(), true);
      }
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
      HornetQServerTestCase.undeployConnectionFactory(CTSMiscellaneousTest.ORG_JBOSS_MESSAGING_SERVICE_LBCONNECTION_FACTORY);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
