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
package org.apache.activemq.jms.tests.message;

import org.junit.Test;

import java.io.Serializable;

import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.jms.tests.HornetQServerTestCase;
import org.apache.activemq.jms.tests.util.ProxyAssertSupport;

/**
 *
 * A ObjectMessageDeliveryTest
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 *
 *
 */
public class ObjectMessageDeliveryTest extends HornetQServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   static class TestObject implements Serializable
   {
      private static final long serialVersionUID = -340663970717491155L;

      String text;
   }

   /**
    *
    */
   @Test
   public void testTopic() throws Exception
   {
      TopicConnection conn = getTopicConnectionFactory().createTopicConnection();

      try
      {
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         TopicPublisher publisher = s.createPublisher(HornetQServerTestCase.topic1);
         TopicSubscriber sub = s.createSubscriber(HornetQServerTestCase.topic1);
         conn.start();

         // Create 3 object messages with different bodies

         TestObject to1 = new TestObject();
         to1.text = "hello1";

         TestObject to2 = new TestObject();
         to1.text = "hello2";

         TestObject to3 = new TestObject();
         to1.text = "hello3";

         ObjectMessage om1 = s.createObjectMessage();
         om1.setObject(to1);

         ObjectMessage om2 = s.createObjectMessage();
         om2.setObject(to2);

         ObjectMessage om3 = s.createObjectMessage();
         om3.setObject(to3);

         // send to topic
         publisher.send(om1);

         publisher.send(om2);

         publisher.send(om3);

         ObjectMessage rm1 = (ObjectMessage)sub.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ObjectMessage rm2 = (ObjectMessage)sub.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ObjectMessage rm3 = (ObjectMessage)sub.receive(HornetQServerTestCase.MAX_TIMEOUT);

         ProxyAssertSupport.assertNotNull(rm1);

         TestObject ro1 = (TestObject)rm1.getObject();

         ProxyAssertSupport.assertEquals(to1.text, ro1.text);
         ProxyAssertSupport.assertNotNull(rm1);

         TestObject ro2 = (TestObject)rm2.getObject();

         ProxyAssertSupport.assertEquals(to2.text, ro2.text);

         ProxyAssertSupport.assertNotNull(rm2);

         TestObject ro3 = (TestObject)rm3.getObject();

         ProxyAssertSupport.assertEquals(to3.text, ro3.text);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
