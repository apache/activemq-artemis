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
package org.hornetq.tests.integration.client;
import org.junit.Before;

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 *
 * From https://jira.jboss.org/jira/browse/HORNETQ-144
 *
 */
public class HornetQCrashTest extends UnitTestCase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   public HornetQServer server;

   private volatile boolean ackReceived;
   private ServerLocator locator;

   @Test
   public void testHang() throws Exception
   {
      Configuration configuration = createDefaultConfig()
         .setPersistenceEnabled(false)
         .setSecurityEnabled(false)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      server = addServer(HornetQServers.newHornetQServer(configuration));

      server.start();

      server.getRemotingService().addIncomingInterceptor(new AckInterceptor(server));




      // Force an ack at once - this means the send call will block
      locator.setConfirmationWindowSize(1);

      ClientSessionFactory clientSessionFactory = createSessionFactory(locator);

      ClientSession session = clientSessionFactory.createSession();

      session.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         public void sendAcknowledged(final Message message)
         {
            ackReceived = true;
         }
      });

      ClientProducer producer = session.createProducer("fooQueue");

      ClientMessage msg = session.createMessage(false);

      msg.putStringProperty("someKey", "someValue");

      producer.send(msg);

      Thread.sleep(250);

      Assert.assertFalse(ackReceived);

      session.close();
   }

   public static class AckInterceptor implements Interceptor
   {
      private final HornetQServer server;

      AckInterceptor(final HornetQServer server)
      {
         this.server = server;
      }

      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         HornetQCrashTest.log.info("AckInterceptor.intercept " + packet);

         if (packet.getType() == PacketImpl.SESS_SEND)
         {
            try
            {
               HornetQCrashTest.log.info("Stopping server");

               new Thread()
               {
                  @Override
                  public void run()
                  {
                     try
                     {
                        server.stop();
                     }
                     catch (Exception e)
                     {
                        e.printStackTrace();
                     }
                  }
               }.start();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }

            return false;
         }
         return true;
      }

   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));
      addServerLocator(locator);
   }
}
