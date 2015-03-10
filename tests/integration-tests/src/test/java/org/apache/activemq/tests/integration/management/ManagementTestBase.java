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
package org.apache.activemq.tests.integration.management;
import org.apache.activemq.api.core.management.QueueControl;
import org.apache.activemq.api.jms.management.JMSQueueControl;
import org.junit.Before;
import org.junit.After;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.junit.Assert;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.tests.util.ServiceTestBase;

/**
 * A ManagementTestBase
 */
public abstract class ManagementTestBase extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MBeanServer mbeanServer;

   // Static --------------------------------------------------------

   protected static void consumeMessages(final int expected, final ClientSession session, final SimpleString queue) throws Exception
   {
      ClientConsumer consumer = null;
      try
      {
         consumer = session.createConsumer(queue);
         ClientMessage m = null;
         for (int i = 0; i < expected; i++)
         {
            m = consumer.receive(500);
            Assert.assertNotNull("expected to received " + expected + " messages, got only " + i, m);
            m.acknowledge();
         }
         session.commit();
         m = consumer.receiveImmediate();
         Assert.assertNull("received one more message than expected (" + expected + ")", m);
      }
      finally
      {
         if (consumer != null)
         {
            consumer.close();
         }
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      mbeanServer = MBeanServerFactory.createMBeanServer();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      MBeanServerFactory.releaseMBeanServer(mbeanServer);

      mbeanServer = null;

      super.tearDown();
   }

   protected void checkNoResource(final ObjectName on)
   {
      Assert.assertFalse("unexpected resource for " + on, mbeanServer.isRegistered(on));
   }

   protected void checkResource(final ObjectName on)
   {
      Assert.assertTrue("no resource for " + on, mbeanServer.isRegistered(on));
   }

   protected QueueControl createManagementControl(final String address, final String queue) throws Exception
   {
      return createManagementControl(SimpleString.toSimpleString(address), SimpleString.toSimpleString(queue));
   }

   protected QueueControl createManagementControl(final SimpleString address, final SimpleString queue) throws Exception
   {
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, queue, mbeanServer);

      return queueControl;
   }

   protected long getMessageCount(JMSQueueControl control) throws Exception
   {
      control.flushExecutor();
      return control.getMessageCount();
   }

   protected long getMessagesAdded(JMSQueueControl control) throws Exception
   {
      control.flushExecutor();
      return control.getMessagesAdded();
   }

   protected long getMessageCount(QueueControl control) throws Exception
   {
      control.flushExecutor();
      return control.getMessageCount();
   }

   protected long getMessagesAdded(QueueControl control) throws Exception
   {
      control.flushExecutor();
      return control.getMessagesAdded();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
