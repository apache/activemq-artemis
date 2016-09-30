/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.protocols.hornetq;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.protocol.hornetq.client.HornetQClientProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.artemis.ra.recovery.RecoveryManager;
import org.apache.activemq.artemis.service.extensions.xa.recovery.XARecoveryConfig;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HornetQProtocolManagerTest extends ActiveMQTestBase {

   ActiveMQServer server;
   EmbeddedJMS embeddedJMS;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      Configuration configuration = createDefaultConfig(false);
      configuration.setPersistenceEnabled(false);
      configuration.getAcceptorConfigurations().clear();
      configuration.addAcceptorConfiguration("legacy", "tcp://localhost:61616?protocols=HORNETQ").
         addAcceptorConfiguration("corepr", "tcp://localhost:61617?protocols=CORE");

      configuration.addConnectorConfiguration("legacy", "tcp://localhost:61616");
      JMSConfiguration jmsConfiguration = new JMSConfigurationImpl();

      jmsConfiguration.getQueueConfigurations().add(new JMSQueueConfigurationImpl().setName("testQueue").setBindings("testQueue"));
      embeddedJMS = new EmbeddedJMS();
      embeddedJMS.setConfiguration(configuration);
      embeddedJMS.setJmsConfiguration(jmsConfiguration);
      embeddedJMS.start();
   }

   @Override
   public void tearDown() throws Exception {
      embeddedJMS.stop();
      super.tearDown();
   }

   @Test
   public void testLegacy() throws Exception {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616?protocolManagerFactoryStr=" + HornetQClientProtocolManagerFactory.class.getName());
      connectionFactory.createConnection().close();
      ActiveMQConnectionFactory connectionFactory2 = new ActiveMQConnectionFactory("tcp://localhost:61617");
      connectionFactory2.createConnection().close();

      RecoveryManager manager = new RecoveryManager();
      manager.register(connectionFactory, null, null, new ConcurrentHashMap<String, String>());
      manager.register(connectionFactory2, null, null, new ConcurrentHashMap<String, String>());

      for (XARecoveryConfig resource : manager.getResources()) {
         try (ServerLocator locator = resource.createServerLocator();
              ClientSessionFactory factory = locator.createSessionFactory();
              ClientSession session = factory.createSession()) {
            // Nothing
         }
      }

   }

   /** This test will use an ArtemisConnectionFactory with clientProtocolManager=*/
   @Test
   public void testLegacy2() throws Exception {

      ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl();
      configuration.setConnectorNames("legacy");
      configuration.setName("legacy");
      configuration.setProtocolManagerFactoryStr(HornetQClientProtocolManagerFactory.class.getName());
      embeddedJMS.getJMSServerManager().createConnectionFactory(false, configuration, "legacy");

      Queue queue = (Queue) embeddedJMS.lookup("testQueue");

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) embeddedJMS.lookup("legacy");
      Connection connection = connectionFactory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(queue);

      TextMessage message = session.createTextMessage("Test");
      for (int i = 0; i < 5; i++) {
         message.setStringProperty(Message.HDR_DUPLICATE_DETECTION_ID.toString(), "duplicate");
         producer.send(message);
      }

      connection.start();
      MessageConsumer consumer = session.createConsumer(queue);
      TextMessage messageRec = (TextMessage) consumer.receive(5000);
      Assert.assertNotNull(messageRec);

      Assert.assertEquals("Test", messageRec.getText());
      Assert.assertNull(consumer.receiveNoWait());
      connection.close();
      connectionFactory.close();

   }

}

