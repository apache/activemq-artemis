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
package org.apache.activemq.tests.integration.jms.server;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.jms.HornetQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.config.impl.FileConfiguration;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.impl.HornetQServerImpl;
import org.apache.activemq.jms.client.HornetQConnectionFactory;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.spi.core.security.HornetQSecurityManager;
import org.apache.activemq.spi.core.security.HornetQSecurityManagerImpl;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * A JMSServerStartStopTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class JMSServerStartStopTest extends UnitTestCase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private JMSServerManager liveJMSServer;

   private Connection conn;

   private HornetQConnectionFactory jbcf;
   private final Set<HornetQConnectionFactory> connectionFactories = new HashSet<HornetQConnectionFactory>();

   @Test
   public void testStopStart1() throws Exception
   {
      final int numMessages = 5;

      for (int j = 0; j < numMessages; j++)
      {
         JMSServerStartStopTest.log.info("Iteration " + j);

         start();

         HornetQConnectionFactory jbcf = createConnectionFactory();

         jbcf.setBlockOnDurableSend(true);
         jbcf.setBlockOnNonDurableSend(true);

         Connection conn = jbcf.createConnection();
         try
         {
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue = sess.createQueue("myJMSQueue");

            MessageProducer producer = sess.createProducer(queue);

            TextMessage tm = sess.createTextMessage("message" + j);

            producer.send(tm);
         }
         finally
         {
            conn.close();

            jbcf.close();

            stop();
         }
      }

      start();

      jbcf = createConnectionFactory();

      jbcf.setBlockOnDurableSend(true);
      jbcf.setBlockOnNonDurableSend(true);

      conn = jbcf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue queue = sess.createQueue("myJMSQueue");

      MessageConsumer consumer = sess.createConsumer(queue);

      conn.start();

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = (TextMessage) consumer.receive(10000);

         Assert.assertNotNull("not null", tm);

         Assert.assertEquals("message" + i, tm.getText());
      }

      conn.close();

      jbcf.close();

      stop();
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-315
   @Test
   public void testCloseConnectionAfterServerIsShutdown() throws Exception
   {
      start();

      jbcf = createConnectionFactory();

      jbcf.setBlockOnDurableSend(true);
      jbcf.setBlockOnNonDurableSend(true);
      jbcf.setReconnectAttempts(-1);

      conn = jbcf.createConnection();

      stop();
      conn.close();
   }

   /**
    * @return
    */
   private HornetQConnectionFactory createConnectionFactory()
   {
      HornetQConnectionFactory cf =
         HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                           new TransportConfiguration(NETTY_CONNECTOR_FACTORY));

      connectionFactories.add(cf);
      return cf;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (conn != null)
         conn.close();
      if (jbcf != null)
         jbcf.close();
      for (HornetQConnectionFactory cf : connectionFactories)
      {
         try
         {
            cf.close();
         }
         catch (Exception ignored)
         {
            // no-op
         }
      }
      connectionFactories.clear();
      if (liveJMSServer != null)
         liveJMSServer.stop();
      liveJMSServer = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void stop() throws Exception
   {
      liveJMSServer.stop();
   }

   private void start() throws Exception
   {
      FileConfiguration fc = new FileConfiguration("server-start-stop-config1.xml");

      fc.start();

      fc.setJournalDirectory(getJournalDir());
      fc.setBindingsDirectory(getBindingsDir());
      fc.setLargeMessagesDirectory(getLargeMessagesDir());

      HornetQSecurityManager sm = new HornetQSecurityManagerImpl();

      HornetQServer liveServer = addServer(new HornetQServerImpl(fc, sm));

      liveJMSServer = new JMSServerManagerImpl(liveServer, "server-start-stop-jms-config1.xml");
      addHornetQComponent(liveJMSServer);
      liveJMSServer.setContext(null);

      liveJMSServer.start();
   }

   // Inner classes -------------------------------------------------

}
