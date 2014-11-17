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
package org.apache.activemq.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.jms.client.HornetQConnectionFactory;
import org.apache.activemq.tests.util.JMSTestBase;
import org.junit.Test;

/**
 * A SessionTest
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class SessionTest extends JMSTestBase
{

   @Test
   public void testIillegalStateException() throws Exception
   {
      Connection defaultConn = null;
      QueueConnection qConn = null;
      Connection connClientID = null;
      HornetQConnectionFactory hqCF = (HornetQConnectionFactory) cf;
      try
      {
         String clientID = "somethingElse" + name.getMethodName();
         defaultConn = cf.createConnection();
         qConn = hqCF.createQueueConnection();

         connClientID = cf.createConnection();
         connClientID.setClientID(clientID);

         Topic topic = createTopic("topic");

         QueueSession qSess = qConn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         try
         {
            qSess.createDurableConsumer(topic, "mySub1");
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         try
         {
            qSess.createDurableConsumer(topic, "mySub1", "TEST = 'test'", false);
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         try
         {
            qSess.createSharedConsumer(topic, "mySub1");
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         try
         {
            qSess.createSharedConsumer(topic, "mySub1", "TEST = 'test'");
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         try
         {
            qSess.createSharedDurableConsumer(topic, "mySub1");
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         try
         {
            qSess.createSharedDurableConsumer(topic, "mySub1", "TEST = 'test'");
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         Session defaultSess = defaultConn.createSession();

         try
         {
            defaultSess.createDurableSubscriber(topic, "mySub1");
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         try
         {
            defaultSess.createDurableSubscriber(topic, "mySub1", "TEST = 'test'", true);
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         try
         {
            defaultSess.createDurableConsumer(topic, "mySub1");
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

         try
         {
            defaultSess.createDurableConsumer(topic, "mySub1", "TEST = 'test'", true);
         }
         catch (javax.jms.IllegalStateException ex)
         {
            //ok expected.
         }

      }
      finally
      {
         if (defaultConn != null)
         {
            defaultConn.close();
         }

         if (qConn != null)
         {
            qConn.close();
         }
         if (connClientID != null)
         {
            connClientID.close();
         }
      }
   }
}
