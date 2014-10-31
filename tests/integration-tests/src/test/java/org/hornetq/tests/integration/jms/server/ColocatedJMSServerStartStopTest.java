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
package org.hornetq.tests.integration.jms.server;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQJMSContext;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.spi.core.security.HornetQSecurityManagerImpl;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.integration.jms.server.management.JMSUtil;
import org.hornetq.tests.util.ColocatedHornetQServer;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Queue;
import java.io.File;

/**
 *
 * A JMSServerStartStopTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ColocatedJMSServerStartStopTest extends UnitTestCase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private JMSServerManager liveJMSServer;

   private JMSServerManager liveJMSServer2;

   private TestableServer testableServerlive1;

   private HornetQJMSContext context;

   @Test
   public void testStopStart1() throws Exception
   {
      start();
      JMSUtil.waitForFailoverTopology(5000, ((ColocatedHornetQServer)liveJMSServer2.getHornetQServer()).backupServer, liveJMSServer.getHornetQServer());
      JMSUtil.waitForFailoverTopology(5000, ((ColocatedHornetQServer)liveJMSServer.getHornetQServer()).backupServer, liveJMSServer2.getHornetQServer());
      HornetQConnectionFactory connectionFactory = HornetQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.CF,
            new TransportConfiguration(NETTY_CONNECTOR_FACTORY));
      connectionFactory.setReconnectAttempts(-1);
      context = (HornetQJMSContext) connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      Queue queue = HornetQJMSClient.createQueue("myJMSQueue");
      JMSConsumer consumer = context.createConsumer(queue);

      for (int i = 0; i < 100; i++)
      {
         String body = "message:" + i;
         context.createProducer().send(queue, body);
         System.out.println(body);
      }

      for (int i = 0; i < 50; i++)
      {
         String msg = consumer.receiveBody(String.class);
         System.out.println("msg = " + msg);
      }

      testableServerlive1.crash(true, ((HornetQSession) context.getSession()).getCoreSession());

      for (int i = 0; i < 50; i++)
      {
         String msg = consumer.receiveBody(String.class, 5000);
         assertNotNull(msg);
         System.out.println("msg = " + msg);
      }
      stop();
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      File tmp = new File("/tmp/hornetq-unit-test");
      deleteDirectory(tmp);
      super.setUp();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (context != null)
         context.close();
      if (liveJMSServer != null)
         liveJMSServer.stop();
      liveJMSServer = null;
      if (liveJMSServer2 != null)
         liveJMSServer2.stop();
      liveJMSServer2 = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void stop() throws Exception
   {
      liveJMSServer.stop();
   }

   private void start() throws Exception
   {
      NodeManager nodeManagerLive1 = new InVMNodeManager(false);
      NodeManager nodeManagerLive2 = new InVMNodeManager(false);
      FileConfiguration fc = new FileConfiguration("colocated-server-start-stop-config1.xml");

      fc.start();

      HornetQSecurityManager sm = new HornetQSecurityManagerImpl();

      HornetQServer liveServer = addServer(new ColocatedHornetQServer(fc, sm, nodeManagerLive1, nodeManagerLive2));
      testableServerlive1 = new SameProcessHornetQServer(liveServer);

      liveJMSServer = new JMSServerManagerImpl(liveServer, "colocated-server-start-stop-jms-config1.xml");
      addHornetQComponent(liveJMSServer);
      liveJMSServer.setContext(null);

      liveJMSServer.start();

      FileConfiguration fc2 = new FileConfiguration("colocated-server-start-stop-config2.xml");

      fc2.start();

      HornetQSecurityManager sm2 = new HornetQSecurityManagerImpl();

      HornetQServer liveServer2 = addServer(new ColocatedHornetQServer(fc2, sm2, nodeManagerLive2, nodeManagerLive1));

      liveJMSServer2 = new JMSServerManagerImpl(liveServer2, "colocated-server-start-stop-jms-config2.xml");
      addHornetQComponent(liveJMSServer);
      liveJMSServer2.setContext(null);

      liveJMSServer2.start();
   }

   // Inner classes -------------------------------------------------

}
