/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.client;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AutoCreateJmsQueueTest extends JMSTestBase
{
   @Test
   public void testAutoCreateOnSendToQueue() throws Exception
   {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue("test");

      MessageProducer producer = session.createProducer(queue);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(mess);
      }

      producer.close();

      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < numMessages; i++)
      {
         Message m = messageConsumer.receive(5000);
         Assert.assertNotNull(m);
      }

      connection.close();
   }

   @Test
   public void testAutoCreateOnSendToQueueAnonymousProducer() throws Exception
   {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue("test");

      MessageProducer producer = session.createProducer(null);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage mess = session.createTextMessage("msg" + i);
         producer.send(queue, mess);
      }

      producer.close();

      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < numMessages; i++)
      {
         Message m = messageConsumer.receive(5000);
         Assert.assertNotNull(m);
      }

      connection.close();
   }

   @Test
   public void testAutoCreateOnSendToQueueSecurity() throws Exception
   {
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().addUser("guest", "guest");
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().setDefaultUser("guest");
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().addRole("guest", "rejectAll");
      Role role = new Role("rejectAll", false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      server.getSecurityRepository().addMatch("#", roles);
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue("test");

      try
      {
         MessageProducer producer = session.createProducer(queue);
         Assert.fail("Creating a producer here should throw a JMSSecurityException");
      }
      catch (Exception e)
      {
         Assert.assertTrue(e instanceof JMSSecurityException);
      }

      connection.close();
   }

   @Test
   public void testAutoCreateOnSendToTopic() throws Exception
   {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Topic topic = ActiveMQJMSClient.createTopic("test");

      try
      {
         MessageProducer producer = session.createProducer(topic);
         Assert.fail("Creating a producer here should throw an exception");
      }
      catch (Exception e)
      {
         Assert.assertTrue(e instanceof InvalidDestinationException);
      }

      connection.close();
   }

   @Test
   public void testAutoCreateOnConsumeFromQueue() throws Exception
   {
      Connection connection = null;
      connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = ActiveMQJMSClient.createQueue("test");

      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();

      Message m = messageConsumer.receive(500);
      Assert.assertNull(m);

      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString("jms.queue.test")).getBindable();
      Assert.assertEquals(0, q.getMessageCount());
      Assert.assertEquals(0, q.getMessagesAdded());
      connection.close();
   }

   @Test
   public void testAutoCreateOnConsumeFromTopic() throws Exception
   {
      Connection connection = null;
      connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Topic topic = ActiveMQJMSClient.createTopic("test");

      try
      {
         MessageConsumer messageConsumer = session.createConsumer(topic);
         Assert.fail("Creating a consumer here should throw an exception");
      }
      catch (Exception e)
      {
         Assert.assertTrue(e instanceof InvalidDestinationException);
      }

      connection.close();
   }

   @Before
   @Override
   public void setUp() throws Exception
   {
      super.setUp();
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().addUser("guest", "guest");
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().setDefaultUser("guest");
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().addRole("guest", "allowAll");
      Role role = new Role("allowAll", true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      server.getSecurityRepository().addMatch("#", roles);
   }

   protected boolean useSecurity()
   {
      return true;
   }
}
