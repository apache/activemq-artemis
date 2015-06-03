/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.protocols.hornetq;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.hornetq.api.core.client.HornetQClient;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class HornetQProtocolTest extends ActiveMQTestBase
{
   protected ActiveMQServer server;

   private static final Logger LOG = LoggerFactory.getLogger(HornetQProtocolTest.class);

   @Before
   public void setUp() throws Exception
   {
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, "" + 5445);
      params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PROTOCOLS_PROP_NAME, "HORNETQ");
      TransportConfiguration transportConfig = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      super.setUp();
      server = createServer(true, true);
      server.getConfiguration().getAcceptorConfigurations().add(transportConfig);
      LOG.info("Added connector {} to broker", "HornetQ");
      server.start();
      waitForServerToStart(server);
   }

   @Test
   public void testMessagePropertiesAreTransformedBetweenCoreAndHQProtocols() throws Exception
   {
      org.hornetq.api.core.client.ClientSession hqSession = createHQClientSession();
      ClientSession coreSession = createCoreClientSession();

      // Create Queue
      String queueName = "test.hq.queue";
      hqSession.createQueue(queueName, queueName, true);

      // HornetQ Client Objects
      hqSession.start();
      org.hornetq.api.core.client.ClientProducer hqProducer = hqSession.createProducer(queueName);
      org.hornetq.api.core.client.ClientConsumer hqConsumer = hqSession.createConsumer(queueName);

      // Core Client Objects
      coreSession.start();
      ClientConsumer coreConsumer = coreSession.createConsumer(queueName);

      // Check that HornetQ Properties are correctly converted to core properties.
      for (int i = 0; i < 2; i++)
      {
         hqProducer.send(createHQTestMessage(hqSession));
      }

      ClientMessage coreMessage1 = coreConsumer.receive(1000);
      assertTrue(coreMessage1.containsProperty(Message.HDR_DUPLICATE_DETECTION_ID));
      coreSession.close();

      // Check that HornetQ Properties are correctly transformed from then to HornetQ properties
      org.hornetq.api.core.client.ClientMessage hqMessage1 = hqConsumer.receive(1000);
      assertTrue(hqMessage1.containsProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID));

      hqSession.close();
   }

   @Test
   public void testDuplicateIDPropertyWithHornetQProtocol() throws Exception
   {
      org.hornetq.api.core.client.ClientSession session = createHQClientSession();

      String queueName = "test.hq.queue";
      session.createQueue(queueName, queueName, true);

      org.hornetq.api.core.client.ClientProducer producer = session.createProducer(queueName);
      org.hornetq.api.core.client.ClientConsumer consumer = session.createConsumer(queueName);
      org.hornetq.api.core.client.ClientMessage message = session.createMessage(false);

      String messageId = UUID.randomUUID().toString();
      message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), messageId);

      session.start();
      producer.send(message);
      org.hornetq.api.core.client.ClientMessage m = consumer.receive(1000);
      assertTrue(m.containsProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID));
      assertNotNull(m);

      producer.send(message);
      m = consumer.receive(1000);
      assertNull(m);

      producer.send(message);
      m = consumer.receive(1000);
      assertNull(m);

      session.close();
   }

   @Test
   public void testDuplicateIDPropertyWithHornetQAndCoreProtocol() throws Exception
   {
      org.hornetq.api.core.client.ClientSession hqSession = createHQClientSession();

      String queueName = "test.hq.queue";
      hqSession.createQueue(queueName, queueName, true);

      org.hornetq.api.core.client.ClientProducer hqProducer = hqSession.createProducer(queueName);
      org.hornetq.api.core.client.ClientMessage message = hqSession.createMessage(false);

      String messageId = UUID.randomUUID().toString();
      message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), messageId);

      ClientSession coreSession = createCoreClientSession();
      ClientConsumer coreConsumer = coreSession.createConsumer(queueName);

      hqSession.start();
      coreSession.start();

      hqProducer.send(message);
      Message m = coreConsumer.receive(1000);
      assertTrue(m.containsProperty(Message.HDR_DUPLICATE_DETECTION_ID));
      assertNotNull(m);


      hqProducer.send(message);
      m = coreConsumer.receive(1000);
      assertNull(m);

      hqProducer.send(message);
      m = coreConsumer.receive(1000);
      assertNull(m);
   }

   private org.hornetq.api.core.client.ClientMessage createHQTestMessage(org.hornetq.api.core.client.ClientSession session)
   {
      org.hornetq.api.core.client.ClientMessage message = session.createMessage(false);
      String v = UUID.randomUUID().toString();
      message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), v);
      return message;
   }

   private ClientMessage createCoreTestMessage(ClientSession session)
   {
      ClientMessage message = session.createMessage(false);
      String v = UUID.randomUUID().toString();
      message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), v);
      return message;
   }

   private org.hornetq.api.core.client.ClientSession createHQClientSession() throws Exception
   {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("host", "localhost");
      map.put("port", 5445);

      org.hornetq.api.core.client.ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new org.hornetq.api.core.TransportConfiguration(org.hornetq.core.remoting.impl.netty.NettyConnectorFactory.class.getName(), map));
      org.hornetq.api.core.client.ClientSessionFactory sf = serverLocator.createSessionFactory();

      return sf.createSession();
   }

   private ClientSession createCoreClientSession() throws Exception
   {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("host", "localhost");
      map.put("port", 61616);

      ServerLocator serverLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), map));
      ClientSessionFactory sf = serverLocator.createSessionFactory();

      return sf.createSession();
   }
}
