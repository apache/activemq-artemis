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
package org.apache.activemq.tests.integration.client;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQIncompatibleClientServerException;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.protocol.core.Channel;
import org.apache.activemq.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.apache.activemq.core.remoting.server.impl.RemotingServiceImpl;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.core.version.impl.VersionImpl;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.tests.util.SpawnedVMSupport;
import org.apache.activemq.utils.VersionLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.activemq.tests.util.RandomUtil.randomString;

/**
 * A IncompatibleVersionTest
 */
public class IncompatibleVersionTest extends ServiceTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private CoreRemotingConnection connection;
   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = createServer(false, false);
      server.getConfiguration().setConnectionTTLOverride(500);
      server.start();

      locator = createInVMNonHALocator();
      ClientSessionFactory csf = createSessionFactory(locator);

      connection = (CoreRemotingConnection)csf.getConnection();
   }

   @Override
   @After
   public void tearDown()
   {
      connection.destroy();

      closeServerLocator(locator);
      stopComponent(server);
      // You CANNOT CALL super.tearDown();
   }

   @Test
   public void testCompatibleClientVersion() throws Exception
   {
      doTestClientVersionCompatibility(true);
   }

   @Test
   public void testIncompatibleClientVersion() throws Exception
   {
      doTestClientVersionCompatibility(false);
   }

   @Test
   public void testCompatibleClientVersionWithRealConnection1() throws Exception
   {
      assertTrue(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 1));
   }

   @Test
   public void testCompatibleClientVersionWithRealConnection2() throws Exception
   {
      assertTrue(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 5));
   }

   @Test
   public void testCompatibleClientVersionWithRealConnection3() throws Exception
   {
      assertTrue(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 10));
   }

   @Test
   public void testIncompatibleClientVersionWithRealConnection1() throws Exception
   {
      assertFalse(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 0));
   }

   @Test
   public void testIncompatibleClientVersionWithRealConnection2() throws Exception
   {
      assertFalse(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 4));
   }

   @Test
   public void testIncompatibleClientVersionWithRealConnection3() throws Exception
   {
      assertFalse(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 100));
   }

   private void doTestClientVersionCompatibility(boolean compatible) throws Exception
   {
      Channel channel1 = connection.getChannel(1, -1);
      long sessionChannelID = connection.generateChannelID();
      int version = VersionLoader.getVersion().getIncrementingVersion();
      if (!compatible)
      {
         version = -1;
      }
      Packet request = new CreateSessionMessage(randomString(),
                                                sessionChannelID,
                                                version,
                                                null,
                                                null,
                                                ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                false,
                                                true,
                                                true,
                                                false,
                                                ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                null);

      if (compatible)
      {
         CreateSessionResponseMessage packet = (CreateSessionResponseMessage) channel1.sendBlocking(request, PacketImpl.CREATESESSION_RESP);
         assertNotNull(packet);
         // 1 connection on the server
         assertEquals(1, server.getConnectionCount());
      }
      else
      {
         try
         {
            channel1.sendBlocking(request, PacketImpl.CREATESESSION_RESP);
            fail();
         }
         catch (ActiveMQIncompatibleClientServerException icsv)
         {
            //ok
         }
         catch (ActiveMQException e)
         {
            fail("Invalid Exception type:" + e.getType());
         }
         long start = System.currentTimeMillis();
         while (System.currentTimeMillis() < start + 3 * RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL)
         {
            if (server.getConnectionCount() == 0)
            {
               break;
            }
         }
         // no connection on the server
         assertEquals(0, server.getConnectionCount());
      }
   }

   private boolean doTestClientVersionCompatibilityWithRealConnection(String verList, int ver) throws Exception
   {
      String propFileName = "compatibility-test-activemq-version.properties";
      String serverStartedString = "IncompatibleVersionTest---server---started";

      Properties prop = new Properties();
      InputStream in = VersionImpl.class.getClassLoader().getResourceAsStream("activemq-version.properties");
      prop.load(in);
      prop.setProperty("activemq.version.compatibleVersionList", verList);
      prop.setProperty("activemq.version.incrementingVersion", Integer.toString(ver));
      prop.store(new FileOutputStream("target/test-classes/" + propFileName), null);

      Process serverProcess = null;
      boolean result = false;
      try
      {
         serverProcess = SpawnedVMSupport.spawnVM("org.apache.activemq.tests.integration.client.IncompatibleVersionTest",
                                                  new String[]{"-D" + VersionLoader.VERSION_PROP_FILE_KEY + "=" + propFileName},
                                                  "server",
                                                  serverStartedString);
         Thread.sleep(2000);

         Process client = SpawnedVMSupport.spawnVM("org.apache.activemq.tests.integration.client.IncompatibleVersionTest",
                                                   new String[]{"-D" + VersionLoader.VERSION_PROP_FILE_KEY + "=" + propFileName},
                                                   "client");

         if (client.waitFor() == 0)
         {
            result = true;
         }
      }
      finally
      {
         if (serverProcess != null)
         {
            try
            {
               serverProcess.destroy();
            }
            catch (Throwable t)
            {
               /* ignore */
            }
         }
      }

      return result;
   }

   private static class ServerStarter
   {
      public void perform(String startedString) throws Exception
      {
         Configuration conf = new ConfigurationImpl()
            .setSecurityEnabled(false)
            .addAcceptorConfiguration(new TransportConfiguration("org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory"));
         ActiveMQServer server = ActiveMQServers.newActiveMQServer(conf, false);
         server.start();

         log.info("### server: " + startedString);
      }
   }

   private static class ClientStarter
   {
      public void perform() throws Exception
      {
         ServerLocator locator = null;
         ClientSessionFactory sf = null;
         try
         {
            locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY));
            sf = locator.createSessionFactory();
            ClientSession session = sf.createSession(false, true, true);
            log.info("### client: connected. server incrementingVersion = " + session.getVersion());
            session.close();
         }
         finally
         {
            closeSessionFactory(sf);
            closeServerLocator(locator);
         }
      }
   }

   public static void main(String[] args) throws Exception
   {
      if (args[0].equals("server"))
      {
         ServerStarter ss = new ServerStarter();
         ss.perform(args[1]);
      }
      else if (args[0].equals("client"))
      {
         ClientStarter cs = new ClientStarter();
         cs.perform();
      }
      else
      {
         throw new Exception("args[0] must be \"server\" or \"client\"");
      }
   }
}
