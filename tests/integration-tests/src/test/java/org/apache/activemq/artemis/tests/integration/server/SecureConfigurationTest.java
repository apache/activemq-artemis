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
package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SecureConfigurationTest extends ActiveMQTestBase {

   @Parameterized.Parameters(name = "{index}: protocol={0}")
   public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][] {
         {"CORE"}, {"AMQP"}, {"OPENWIRE"}
      });
   }

   /* NOT private @see https://github.com/junit-team/junit4/wiki/parameterized-tests */
   @Parameterized.Parameter(0)
   public String protocol;

   @Test
   public void testSecureSharedDurableSubscriber() throws Exception {
      //This is because OpenWire does not support JMS 2.0
      Assume.assumeFalse(protocol.equals("OPENWIRE"));

      ActiveMQServer server = getActiveMQServer("multicast_topic.xml");
      try {
         server.start();
         internal_testSecureSharedDurableSubscriber(getConnectionFactory("b", "b"));
      } finally {
         try {
            server.stop();
         } catch (Exception e) {
         }
      }
   }

   private void internal_testSecureSharedDurableSubscriber(ConnectionFactory connectionFactory) throws JMSException {
      String message = "blah";

      //Expect to be able to create subscriber on pre-defined/existing queue.
      String messageRecieved = sendAndReceiveText(connectionFactory, null, message, "secured_topic_shared_durable", (t, s) -> s.createSharedDurableConsumer(t, "secured_topic_shared_durable/queue"));
      Assert.assertEquals(message, messageRecieved);

      try {
         sendAndReceiveText(connectionFactory, null, message, "secured_topic_shared_durable", (t, s) -> s.createSharedDurableConsumer(t, "secured_topic_shared_durable/non-existant-queue"));
         Assert.fail("Security exception expected, but did not occur, excepetion expected as not permissioned to dynamically create queue");
      } catch (JMSSecurityException j) {
         //Expected exception
      }

      try {
         sendAndReceiveText(connectionFactory, null, message, "secured_topic_shared_durable", (t, s) -> s.createSharedDurableConsumer(t, "secured_topic_shared_durable/queue", "age < 10"));
         Assert.fail("Security exception expected, but did not occur, excepetion expected as not permissioned to dynamically create queue");
      } catch (JMSSecurityException j) {
         //Expected exception
      }
   }

   @Test
   public void testSecureSharedSubscriber() throws Exception {
      //This is because OpenWire does not support JMS 2.0
      Assume.assumeFalse(protocol.equals("OPENWIRE"));

      ActiveMQServer server = getActiveMQServer("multicast_topic.xml");
      try {
         server.start();
         internal_testSecureSharedSubscriber(getConnectionFactory("b", "b"));
      } finally {
         try {
            server.stop();
         } catch (Exception e) {
         }
      }
   }

   private void internal_testSecureSharedSubscriber(ConnectionFactory connectionFactory) throws JMSException {
      String message = "blah";

      //Expect to be able to create subscriber on pre-defined/existing queue.
      String messageRecieved = sendAndReceiveText(connectionFactory, null, message, "secured_topic_shared", (t, s) -> s.createSharedConsumer(t, "secured_topic_shared/queue"));
      Assert.assertEquals(message, messageRecieved);

      try {
         sendAndReceiveText(connectionFactory, null, message, "secured_topic_shared", (t, s) -> s.createSharedConsumer(t, "secured_topic_shared/non-existant-queue"));
         Assert.fail("Security exception expected, but did not occur, excepetion expected as not permissioned to dynamically create queue");
      } catch (JMSSecurityException j) {
         //Expected exception
      }

      try {
         sendAndReceiveText(connectionFactory, null, message, "secured_topic_shared", (t, s) -> s.createSharedConsumer(t, "secured_topic_shared/queue", "age < 10"));
         Assert.fail("Security exception expected, but did not occur, excepetion expected as not permissioned to dynamically create queue");
      } catch (JMSSecurityException j) {
         //Expected exception
      }
   }

   @Test
   public void testSecureDurableSubscriber() throws Exception {
      ActiveMQServer server = getActiveMQServer("multicast_topic.xml");
      try {
         server.start();
         internal_testSecureDurableSubscriber(getConnectionFactory("b", "b"));
      } finally {
         try {
            server.stop();
         } catch (Exception e) {
         }
      }
   }

   private void internal_testSecureDurableSubscriber(ConnectionFactory connectionFactory) throws JMSException {
      String message = "blah";

      //Expect to be able to create subscriber on pre-defined/existing queue.
      String messageRecieved = sendAndReceiveText(connectionFactory, "clientId", message, "secured_topic_durable", (t, s) -> s.createDurableSubscriber(t, "secured_topic_durable/queue"));
      Assert.assertEquals(message, messageRecieved);

      try {
         sendAndReceiveText(connectionFactory, "clientId", message, "secured_topic_durable", (t, s) -> s.createDurableSubscriber(t, "secured_topic_durable/non-existant-queue"));
         Assert.fail("Security exception expected, but did not occur, excepetion expected as not permissioned to dynamically create queue");
      } catch (JMSSecurityException j) {
         //Expected exception
      }

      try {
         sendAndReceiveText(connectionFactory, "clientId", message, "secured_topic_durable", (t, s) -> s.createDurableSubscriber(t, "secured_topic_durable/queue", "age < 10", false));
         Assert.fail("Security exception expected, but did not occur, excepetion expected as not permissioned to dynamically create queue");
      } catch (JMSSecurityException j) {
         //Expected exception
      }

      try {
         sendAndReceiveText(connectionFactory, "clientId", message, "secured_topic_durable", (t, s) -> s.createDurableSubscriber(t, "secured_topic_durable/queue", "age < 10", true));
         Assert.fail("Security exception expected, but did not occur, excepetion expected as not permissioned to dynamically create queue");
      } catch (JMSSecurityException j) {
         //Expected exception
      }
   }

   private ConnectionFactory getConnectionFactory(String user, String password) {
      switch (protocol) {
         case "CORE": return getActiveMQConnectionFactory(user, password);
         case "AMQP" : return getAMQPConnectionFactory(user, password);
         case "OPENWIRE": return getOpenWireConnectionFactory(user, password);
         default: throw new IllegalStateException("Unsupported Protocol");
      }
   }

   private ActiveMQConnectionFactory getActiveMQConnectionFactory(String user, String password) {
      ActiveMQConnectionFactory activeMQConnection = new ActiveMQConnectionFactory("tcp://localhost:61616");
      activeMQConnection.setUser(user);
      activeMQConnection.setPassword(password);
      return activeMQConnection;
   }

   private JmsConnectionFactory getAMQPConnectionFactory(String user, String password) {
      JmsConnectionFactory jmsConnectionFactory = new JmsConnectionFactory("amqp://localhost:61616");
      jmsConnectionFactory.setUsername(user);
      jmsConnectionFactory.setPassword(password);
      return jmsConnectionFactory;
   }

   private org.apache.activemq.ActiveMQConnectionFactory getOpenWireConnectionFactory(String user, String password) {
      org.apache.activemq.ActiveMQConnectionFactory activeMQConnectionFactory = new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:61616");
      activeMQConnectionFactory.setUserName(user);
      activeMQConnectionFactory.setPassword(password);
      return activeMQConnectionFactory;
   }

   private String sendAndReceiveText(ConnectionFactory connectionFactory, String clientId, String message, String topicName, ConsumerSupplier consumerSupplier) throws JMSException {
      String messageRecieved;
      try (Connection connection = connectionFactory.createConnection()) {
         if (clientId != null && !clientId.isEmpty()) {
            connection.setClientID(clientId);
         }
         connection.start();
         try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Topic topic = session.createTopic(topicName);
            MessageConsumer messageConsumer = consumerSupplier.create(topic, session);
            messageConsumer.receive(1000);

            TextMessage messageToSend = session.createTextMessage(message);
            session.createProducer(topic).send(messageToSend);

            TextMessage received = (TextMessage) messageConsumer.receive(1000);
            messageRecieved = received != null ? received.getText() : null;
         }
      }
      return messageRecieved;
   }

   protected ActiveMQServer getActiveMQServer(String brokerConfig) throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileJMSConfiguration fileConfiguration = new FileJMSConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(brokerConfig);
      deploymentManager.addDeployable(fc);
      deploymentManager.addDeployable(fileConfiguration);
      deploymentManager.readConfiguration();


      SecurityConfiguration securityConfiguration = new SecurityConfiguration();
      securityConfiguration.addUser("a", "a");
      securityConfiguration.addRole("a", "a");

      securityConfiguration.addUser("b", "b");
      securityConfiguration.addRole("b", "b");


      ActiveMQJAASSecurityManager sm = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), securityConfiguration);

      return addServer(new ActiveMQServerImpl(fc, sm));
   }

   private interface ConsumerSupplier {
      MessageConsumer create(Topic topic, Session session) throws JMSException;
   }

}
