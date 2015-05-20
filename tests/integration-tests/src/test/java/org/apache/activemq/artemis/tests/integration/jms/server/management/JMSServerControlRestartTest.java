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
package org.apache.activemq.artemis.tests.integration.jms.server.management;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.integration.management.ManagementTestBase;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ServiceTestBase;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JMSServerControlRestartTest extends ManagementTestBase
{

   protected InVMNamingContext context;

   private JMSServerManager serverManager;

   @Test
   public void testCreateDurableQueueUsingJMXAndRestartServer() throws Exception
   {
      String queueName = RandomUtil.randomString();
      String binding = RandomUtil.randomString();

      ServiceTestBase.checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = ManagementControlHelper.createJMSServerControl(mbeanServer);
      control.createQueue(queueName, binding);

      Object o = ServiceTestBase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      serverManager.stop();

      ServiceTestBase.checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      serverManager = createJMSServer();
      serverManager.start();

      o = ServiceTestBase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));
   }

   @Test
   public void testCreateDurableQueueUsingJMSAndRestartServer() throws Exception
   {
      String queueName = RandomUtil.randomString();
      String binding = RandomUtil.randomString();

      ServiceTestBase.checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      TransportConfiguration config = new TransportConfiguration(InVMConnectorFactory.class.getName());
      Connection connection = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, config).createConnection();
      connection.start();
      Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");
      QueueSession session = (QueueSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      QueueRequestor requestor = new QueueRequestor(session, managementQueue);
      Message message = session.createMessage();
      JMSManagementHelper.putOperationInvocation(message, "jms.server", "createQueue", queueName, binding);
      Message reply = requestor.request(message);
      Assert.assertTrue(JMSManagementHelper.hasOperationSucceeded(reply));
      connection.close();

      Object o = ServiceTestBase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      Queue queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      serverManager.stop();

      ServiceTestBase.checkNoBinding(context, binding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      serverManager = createJMSServer();
      serverManager.start();

      o = ServiceTestBase.checkBinding(context, binding);
      Assert.assertTrue(o instanceof Queue);
      queue = (Queue) o;
      Assert.assertEquals(queueName, queue.getQueueName());
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
      ActiveMQServer server = ActiveMQServers.newActiveMQServer(conf, mbeanServer);

      context = new InVMNamingContext();

      serverManager = new JMSServerManagerImpl(server);
      serverManager.setRegistry(new JndiBindingRegistry(context));
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
