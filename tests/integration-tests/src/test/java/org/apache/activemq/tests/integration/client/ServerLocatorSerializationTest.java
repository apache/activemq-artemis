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
package org.apache.activemq.tests.integration.client;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.jms.HornetQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.jms.client.HornetQConnectionFactory;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.ServiceTestBase;

import javax.jms.Connection;
import javax.jms.Session;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ServerLocatorSerializationTest extends ServiceTestBase
{
   private HornetQServer server;
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      Configuration configuration = createDefaultConfig(isNetty());
      server = createServer(false, configuration);
      server.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      server.stop();
      super.tearDown();
   }

   @Test
   public void testLocatorSerialization() throws Exception
   {
      log.info("Starting Netty locator");
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())));

      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession(false, false);
      session.close();
      csf.close();

      log.info("Serializing locator");
      ServerLocatorImpl locatorImpl = (ServerLocatorImpl) locator;
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bos);
      out.writeObject(locatorImpl);

      log.info("De-serializing locator");
      ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      ObjectInputStream in = new ObjectInputStream(bis);
      locatorImpl = (ServerLocatorImpl) in.readObject();

      csf = createSessionFactory(locator);
      session = csf.createSession(false, false);
      session.close();
      csf.close();

      locator.close();
      locatorImpl.close();
   }

   @Test
   public void testConnectionFactorySerialization() throws Exception
   {
      log.info("Starting connection factory");
      HornetQConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory"));

      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      session.close();
      connection.close();

      log.info("Serializing connection factory");
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bos);
      out.writeObject(cf);

      log.info("De-serializing connection factory");
      ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      ObjectInputStream in = new ObjectInputStream(bis);
      cf = (HornetQConnectionFactory) in.readObject();

      connection = cf.createConnection();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      session.close();
      connection.close();

      cf.close();
   }

   public boolean isNetty()
   {
      return true;
   }
}
