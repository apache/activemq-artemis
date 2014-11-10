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
package org.hornetq.tests.integration.jms.server.management;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.api.jms.management.JMSManagementHelper;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.JournalType;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.integration.management.ManagementTestBase;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A JMSServerControlRestartTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSServerControlRestartTest extends ManagementTestBase
{

   protected InVMNamingContext context;

   private JMSServerManager serverManager;

   @Test
   public void testCreateDurableQueueUsingJMXAndRestartServer() throws Exception
   {
      String queueName = RandomUtil.randomString();
      String binding = RandomUtil.randomString();

      UnitTestCase.checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = ManagementControlHelper.createJMSServerControl(mbeanServer);
      control.createQueue(queueName, binding);

      Object o = UnitTestCase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue) o;
      assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      serverManager.stop();

      checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      serverManager = createJMSServer();
      serverManager.start();

      o = UnitTestCase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));
   }

   @Test
   public void testCreateDurableQueueUsingJMSAndRestartServer() throws Exception
   {
      String queueName = RandomUtil.randomString();
      String binding = RandomUtil.randomString();

      UnitTestCase.checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      TransportConfiguration config = new TransportConfiguration(InVMConnectorFactory.class.getName());
      Connection connection = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, config).createConnection();
      connection.start();
      Queue managementQueue = HornetQJMSClient.createQueue("hornetq.management");
      QueueSession session = (QueueSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      QueueRequestor requestor = new QueueRequestor(session, managementQueue);
      Message message = session.createMessage();
      JMSManagementHelper.putOperationInvocation(message, "jms.server", "createQueue", queueName, binding);
      Message reply = requestor.request(message);
      assertTrue(JMSManagementHelper.hasOperationSucceeded(reply));
      connection.close();

      Object o = UnitTestCase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue) o;
      assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      serverManager.stop();

      checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      serverManager = createJMSServer();
      serverManager.start();

      o = UnitTestCase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      serverManager = createJMSServer();
      serverManager.start();
   }

   private JMSServerManager createJMSServer() throws Exception
   {
      Configuration conf = createDefaultConfig()
         .setSecurityEnabled(false)
         .setJMXManagementEnabled(true)
         .setPersistenceEnabled(true)
         .setJournalType(JournalType.NIO)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      HornetQServer server = HornetQServers.newHornetQServer(conf, mbeanServer);

      context = new InVMNamingContext();

      serverManager = new JMSServerManagerImpl(server);
      serverManager.setContext(context);
      return serverManager;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      serverManager.stop();
      serverManager = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
