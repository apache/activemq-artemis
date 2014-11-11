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
package org.apache.activemq6.tests.integration.jms.cluster;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq6.tests.util.JMSClusteredTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A TopicClusterTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class TopicClusterTest extends JMSClusteredTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }


   @Before
   public void setUp() throws Exception
   {
      super.setUp();
   }

   @Test
   public void testDeleteTopicAfterClusteredSend() throws Exception
   {
      Connection conn1 = cf1.createConnection();

      conn1.setClientID("someClient1");

      Connection conn2 = cf2.createConnection();

      conn2.setClientID("someClient2");

      conn1.start();

      conn2.start();

      try
      {

         Topic topic1 = createTopic("t1");

         Topic topic2 = (Topic) context1.lookup("topic/t1");

         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // topic1 and 2 should be the same.
         // Using a different instance here just to make sure it is implemented correctly
         MessageConsumer cons2 = session2.createDurableSubscriber(topic2, "sub2");
         Thread.sleep(500);
         MessageProducer prod1 = session1.createProducer(topic1);

         prod1.setDeliveryMode(DeliveryMode.PERSISTENT);


         for (int i = 0; i < 2; i++)
         {
            prod1.send(session1.createTextMessage("someMessage"));
         }

         TextMessage received = (TextMessage) cons2.receive(5000);

         assertNotNull(received);

         assertEquals("someMessage", received.getText());

         cons2.close();
      }
      finally
      {
         conn1.close();
         conn2.close();
      }

      jmsServer1.destroyTopic("t1");
      jmsServer2.destroyTopic("t1");


   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
