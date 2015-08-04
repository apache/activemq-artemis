/*
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

import java.util.ArrayList;
import java.util.List;

import javax.management.Notification;

import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.integration.management.ManagementTestBase;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.api.jms.management.ConnectionFactoryControl;
import org.apache.activemq.artemis.api.jms.management.JMSServerControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.jms.server.management.JMSNotificationType;

/**
 * A Connection Factory Control Test
 */
public class ConnectionFactoryControlTest extends ManagementTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private JMSServerManagerImpl jmsServerManager;

   private InVMNamingContext ctx;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCreateCF() throws Exception {
      JMSServerControl control = createJMSControl();
      control.createConnectionFactory("test", false, false, 0, "invm", "test");

      ConnectionFactoryControl controlCF = createCFControl("test");

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("test");

      Assert.assertFalse(cf.isCompressLargeMessage());

      controlCF.setCompressLargeMessages(true);

      cf = (ActiveMQConnectionFactory) ctx.lookup("test");
      Assert.assertTrue(cf.isCompressLargeMessage());

      server.stop();

      waitForServerToStop(server);

      startServer();

      cf = (ActiveMQConnectionFactory) ctx.lookup("test");
      Assert.assertTrue(cf.isCompressLargeMessage());

   }

   //make sure notifications are always received no matter whether
   //a CF is created via JMSServerControl or by JMSServerManager directly.
   @Test
   public void testCreateCFNotification() throws Exception {
      JMSUtil.JMXListener listener = new JMSUtil.JMXListener();
      this.mbeanServer.addNotificationListener(ObjectNameBuilder.DEFAULT.getJMSServerObjectName(), listener, null, null);

      List<String> connectors = new ArrayList<String>();
      connectors.add("invm");

      this.jmsServerManager.createConnectionFactory("NewCF", false, JMSFactoryType.CF, connectors, "/NewConnectionFactory");

      Notification notif = listener.getNotification();

      Assert.assertEquals(JMSNotificationType.CONNECTION_FACTORY_CREATED.toString(), notif.getType());
      Assert.assertEquals("NewCF", notif.getMessage());

      this.jmsServerManager.destroyConnectionFactory("NewCF");

      notif = listener.getNotification();
      Assert.assertEquals(JMSNotificationType.CONNECTION_FACTORY_DESTROYED.toString(), notif.getType());
      Assert.assertEquals("NewCF", notif.getMessage());

      JMSServerControl control = createJMSControl();

      control.createConnectionFactory("test", false, false, 0, "invm", "test");

      notif = listener.getNotification();
      Assert.assertEquals(JMSNotificationType.CONNECTION_FACTORY_CREATED.toString(), notif.getType());
      Assert.assertEquals("test", notif.getMessage());

      control.destroyConnectionFactory("test");

      notif = listener.getNotification();
      Assert.assertEquals(JMSNotificationType.CONNECTION_FACTORY_DESTROYED.toString(), notif.getType());
      Assert.assertEquals("test", notif.getMessage());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      startServer();

   }

   /**
    * @throws Exception
    */
   protected void startServer() throws Exception {
      Configuration config = createDefaultInVMConfig().addConnectorConfiguration("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY)).setJMXManagementEnabled(true);
      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, true));
      server.start();

      jmsServerManager = new JMSServerManagerImpl(server);
      jmsServerManager.start();

      ctx = new InVMNamingContext();

      jmsServerManager.setRegistry(new JndiBindingRegistry(ctx));
      jmsServerManager.activated();
   }

   protected ConnectionFactoryControl createCFControl(String name) throws Exception {
      return ManagementControlHelper.createConnectionFactoryControl(name, mbeanServer);
   }

   protected JMSServerControl createJMSControl() throws Exception {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
