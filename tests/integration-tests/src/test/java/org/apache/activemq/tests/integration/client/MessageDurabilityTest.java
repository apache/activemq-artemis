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
package org.apache.activemq6.tests.integration.client;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.HornetQExceptionType;
import org.apache.activemq6.api.core.HornetQNonExistentQueueException;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientConsumer;
import org.apache.activemq6.api.core.client.ClientProducer;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.tests.util.RandomUtil;
import org.apache.activemq6.tests.util.ServiceTestBase;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A MessagDurabilityTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class MessageDurabilityTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testNonDurableMessageOnNonDurableQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, !durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(!durable));

      restart();

      session.start();
      try
      {
         session.createConsumer(queue);
      }
      catch (HornetQNonExistentQueueException neqe)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testNonDurableMessageOnDurableQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(!durable));

      restart();

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      Assert.assertNull(consumer.receiveImmediate());

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testDurableMessageOnDurableQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));

      restart();

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      Assert.assertNotNull(consumer.receive(500));

      consumer.close();
      session.deleteQueue(queue);
   }

   /**
    * we can send a durable msg to a non durable queue but the msg won't be persisted
    */
   @Test
   public void testDurableMessageOnNonDurableQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = RandomUtil.randomSimpleString();
      final SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, !durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));

      restart();

      session.start();

      UnitTestCase.expectHornetQException(HornetQExceptionType.QUEUE_DOES_NOT_EXIST, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createConsumer(queue);
         }
      });
   }

   /**
    * we can send a durable msg to a temp queue but the msg won't be persisted
    */
   @Test
   public void testDurableMessageOnTemporaryQueue() throws Exception
   {
      boolean durable = true;

      SimpleString address = RandomUtil.randomSimpleString();
      final SimpleString queue = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));

      restart();

      session.start();
      UnitTestCase.expectHornetQException(HornetQExceptionType.QUEUE_DOES_NOT_EXIST, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createConsumer(queue);
         }
      });
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      server = createServer(true);
      server.start();
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }

   // Private -------------------------------------------------------

   private void restart() throws Exception
   {
      session.close();

      server.stop();
      server.start();

      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
   }
   // Inner classes -------------------------------------------------

}
