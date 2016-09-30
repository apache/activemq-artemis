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
package org.apache.activemq.artemis.tests.integration.http;

import java.util.HashMap;
import java.util.Random;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CoreClientOverHttpTest extends ActiveMQTestBase {

   private static final SimpleString QUEUE = new SimpleString("CoreClientOverHttpTestQueue");
   private Configuration conf;
   private ActiveMQServer server;
   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);

      conf = createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      server = addServer(ActiveMQServers.newActiveMQServer(conf, false));
      server.start();
      locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params)));
   }

   @Test
   public void testCoreHttpClient() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
         message.getBodyBuffer().writeString("CoreClientOverHttpTest");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals("CoreClientOverHttpTest", message2.getBodyBuffer().readString());

         message2.acknowledge();
      }

      session.close();
   }

   @Test
   public void testCoreHttpClientIdle() throws Exception {
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
   public void testCoreHttpClient8kPlus() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      String[] content = new String[numMessages];

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
         content[i] = this.getFixedSizeString(((i % 5) + 1) * 1024 * 8);
         message.getBodyBuffer().writeString(content[i]);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals(content[i], message2.getBodyBuffer().readString());

         message2.acknowledge();
      }

      session.close();
   }

   private String getFixedSizeString(int size) {
      StringBuffer sb = new StringBuffer();
      Random r = new Random();
      for (int i = 0; i < size; i++) {
         char chr = (char) r.nextInt(256);
         sb.append(chr);
      }
      String result = sb.toString();
      return result;
   }

}
