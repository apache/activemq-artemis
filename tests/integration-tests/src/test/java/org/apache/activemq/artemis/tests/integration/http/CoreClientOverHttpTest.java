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
package org.apache.activemq.tests.integration.http;
import org.junit.Before;

import org.junit.Test;

import java.util.HashMap;
import java.util.Random;

import org.junit.Assert;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.jms.client.ActiveMQTextMessage;
import org.apache.activemq.tests.util.UnitTestCase;

public class CoreClientOverHttpTest extends UnitTestCase
{
   private static final SimpleString QUEUE = new SimpleString("CoreClientOverHttpTestQueue");
   private Configuration conf;
   private ActiveMQServer server;
   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);

      conf = createDefaultConfig()
         .setSecurityEnabled(false)
         .addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));

      server = addServer(ActiveMQServers.newActiveMQServer(conf, false));

      server.start();
      locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params));
      addServerLocator(locator);
   }

   @Test
   public void testCoreHttpClient() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
         message.getBodyBuffer().writeString("CoreClientOverHttpTest");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals("CoreClientOverHttpTest", message2.getBodyBuffer().readString());

         message2.acknowledge();
      }

      session.close();
   }

   @Test
   public void testCoreHttpClientIdle() throws Exception
   {
      locator.setConnectionTTL(500);
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      Thread.sleep(500 * 5);

      session.close();
   }

   // https://issues.jboss.org/browse/JBPAPP-5542
   @Test
   public void testCoreHttpClient8kPlus() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      String[] content = new String[numMessages];

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
         content[i] = this.getFixedSizeString(((i % 5) + 1) * 1024 * 8);
         message.getBodyBuffer().writeString(content[i]);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals(content[i], message2.getBodyBuffer().readString());

         message2.acknowledge();
      }

      session.close();
   }

   private String getFixedSizeString(int size)
   {
      StringBuffer sb = new StringBuffer();
      Random r = new Random();
      for (int i = 0; i < size; i++)
      {
         char chr = (char)r.nextInt(256);
         sb.append(chr);
      }
      String result = sb.toString();
      return result;
   }

}
