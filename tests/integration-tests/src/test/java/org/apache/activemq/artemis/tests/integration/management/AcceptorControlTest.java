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
package org.apache.activemq.artemis.tests.integration.management;

import java.util.HashMap;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.AcceptorControl;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.tests.integration.SimpleNotificationService;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class AcceptorControlTest extends ManagementTestBase {
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public boolean usingCore() {
      return false;
   }

   @Test
   public void testAttributes() throws Exception {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(), new HashMap<String, Object>(), RandomUtil.randomString());

      Configuration config = createBasicConfig().addAcceptorConfiguration(acceptorConfig);
      ActiveMQServer service = createServer(false, config);
      service.setMBeanServer(mbeanServer);
      service.start();

      AcceptorControl acceptorControl = createManagementControl(acceptorConfig.getName());

      Assert.assertEquals(acceptorConfig.getName(), acceptorControl.getName());
      Assert.assertEquals(acceptorConfig.getFactoryClassName(), acceptorControl.getFactoryClassName());
   }

   @Test
   public void testStartStop() throws Exception {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(), new HashMap<String, Object>(), RandomUtil.randomString());
      Configuration config = createBasicConfig().addAcceptorConfiguration(acceptorConfig);
      ActiveMQServer service = createServer(false, config);
      service.setMBeanServer(mbeanServer);
      service.start();

      AcceptorControl acceptorControl = createManagementControl(acceptorConfig.getName());

      // started by the server
      Assert.assertTrue(acceptorControl.isStarted());
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);
      Assert.assertNotNull(session);
      session.close();

      acceptorControl.stop();

      Assert.assertFalse(acceptorControl.isStarted());

      try {
         sf.createSession(false, true, true);
         Assert.fail("acceptor must not accept connections when stopped accepting");
      } catch (ActiveMQException e) {
      }

      acceptorControl.start();

      Assert.assertTrue(acceptorControl.isStarted());

      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
      Assert.assertNotNull(session);
      session.close();

      acceptorControl.stop();

      Assert.assertFalse(acceptorControl.isStarted());

      try {
         sf.createSession(false, true, true);
         Assert.fail("acceptor must not accept connections when stopped accepting");
      } catch (ActiveMQException e) {
      }

   }

   @Test
   public void testNotifications() throws Exception {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(), new HashMap<String, Object>(), RandomUtil.randomString());
      TransportConfiguration acceptorConfig2 = new TransportConfiguration(NettyAcceptorFactory.class.getName(), new HashMap<String, Object>(), RandomUtil.randomString());
      Configuration config = createBasicConfig().addAcceptorConfiguration(acceptorConfig).addAcceptorConfiguration(acceptorConfig2);
      ActiveMQServer service = createServer(false, config);
      service.setMBeanServer(mbeanServer);
      service.start();

      AcceptorControl acceptorControl = createManagementControl(acceptorConfig2.getName());

      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();

      service.getManagementService().addNotificationListener(notifListener);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      acceptorControl.stop();

      Assert.assertEquals(usingCore() ? 5 : 1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(usingCore() ? 2 : 0);
      Assert.assertEquals(CoreNotificationType.ACCEPTOR_STOPPED, notif.getType());
      Assert.assertEquals(NettyAcceptorFactory.class.getName(), notif.getProperties().getSimpleStringProperty(new SimpleString("factory")).toString());

      acceptorControl.start();

      Assert.assertEquals(usingCore() ? 10 : 2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(usingCore() ? 7 : 1);
      Assert.assertEquals(CoreNotificationType.ACCEPTOR_STARTED, notif.getType());
      Assert.assertEquals(NettyAcceptorFactory.class.getName(), notif.getProperties().getSimpleStringProperty(new SimpleString("factory")).toString());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected AcceptorControl createManagementControl(final String name) throws Exception {
      return ManagementControlHelper.createAcceptorControl(name, mbeanServer);
   }
}
