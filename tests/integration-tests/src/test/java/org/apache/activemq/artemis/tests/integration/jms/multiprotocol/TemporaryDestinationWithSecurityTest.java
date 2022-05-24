/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.jms.multiprotocol;

import javax.jms.Connection;
import javax.jms.JMSSecurityException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class TemporaryDestinationWithSecurityTest extends MultiProtocolJMSClientTestSupport {

   private SecureConnectionSupplier connectionSupplier;
   private final String TEMP_QUEUE_NAMESPACE = RandomUtil.randomString();
   private String createOnlyUser = "createonly";
   private String createOnlyPass = "createonly";

   @Parameterized.Parameters(name = "connectionSupplier={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{
         {"AMQP"},
         {"CORE"},
         {"OPENWIRE"}
      });
   }

   public TemporaryDestinationWithSecurityTest(String connectionSupplier) {
      switch (connectionSupplier) {
         case "AMQP":
            this.connectionSupplier = (username, password) -> createConnection(username, password);
            break;
         case "CORE":
            this.connectionSupplier = (username, password) -> createCoreConnection(username, password);
            break;
         case "OPENWIRE":
            this.connectionSupplier = (username, password) -> createOpenWireConnection(username, password);
            break;
      }
   }

   @Override
   protected boolean isSecurityEnabled() {
      return true;
   }

   @Override
   protected void enableSecurity(ActiveMQServer server, String... securityMatches) {
      super.enableSecurity(server, TEMP_QUEUE_NAMESPACE + ".#");

      // add a new user/role who can create but not delete
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser(createOnlyUser, createOnlyPass);
      securityManager.getConfiguration().addRole(createOnlyUser, "createonly");
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      Set<Role> value = securityRepository.getMatch(TEMP_QUEUE_NAMESPACE + ".#");
      value.add(new Role("createonly", false, false, true, false, true, false, false, false, true, false));
   }
   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      params.put("supportAdvisory", "false");
   }

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      super.addConfiguration(server);
      server.getConfiguration().setTemporaryQueueNamespace(TEMP_QUEUE_NAMESPACE);
   }

   @Test(timeout = 60000)
   public void testCreateTemporaryQueue() throws Throwable {
      Connection connection = connectionSupplier.createConnection(fullUser, fullPass);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TemporaryQueue queue = session.createTemporaryQueue();
      MessageProducer producer = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      producer.send(message);

      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      message = (TextMessage) consumer.receive(5000);

      assertNotNull(message);
   }

   @Test(timeout = 60000)
   public void testCreateTemporaryQueueNegative() throws Throwable {
      Connection connection = connectionSupplier.createConnection(noprivUser, noprivPass);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try {
         session.createTemporaryQueue();
         fail();
      } catch (JMSSecurityException e) {
         // expected
      }
   }

   @Test(timeout = 30000)
   public void testDeleteTemporaryQueue() throws Exception {
      Connection connection = connectionSupplier.createConnection(fullUser, fullPass);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final javax.jms.TemporaryQueue queue = session.createTemporaryQueue();
      assertNotNull(queue);

      Queue queueView = getProxyToQueue(queue.getQueueName());
      assertNotNull(queueView);

      queue.delete();

      Wait.assertTrue("Temp Queue should be deleted.", () -> getProxyToQueue(queue.getQueueName()) == null, 3000, 10);
   }

   @Test(timeout = 30000)
   public void testDeleteTemporaryQueueNegative() throws Exception {
      Connection connection = connectionSupplier.createConnection(createOnlyUser, createOnlyPass);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final javax.jms.TemporaryQueue queue = session.createTemporaryQueue();
      assertNotNull(queue);

      Queue queueView = getProxyToQueue(queue.getQueueName());
      assertNotNull(queueView);

      try {
         queue.delete();
         fail();
      } catch (JMSSecurityException e) {
         // expected
      }

      connection.close();

      Wait.assertTrue(() -> getProxyToAddress(queue.getQueueName()) == null, 3000, 10);
   }

   @Test(timeout = 60000)
   public void testCreateTemporaryTopic() throws Throwable {
      Connection connection = connectionSupplier.createConnection(fullUser, fullPass);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TemporaryTopic topic = session.createTemporaryTopic();

      MessageConsumer consumer = session.createConsumer(topic);
      MessageProducer producer = session.createProducer(topic);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      producer.send(message);

      connection.start();
      message = (TextMessage) consumer.receive(5000);
      assertNotNull(message);
   }

   @Test(timeout = 60000)
   public void testCreateTemporaryTopicNegative() throws Throwable {
      Connection connection = connectionSupplier.createConnection(noprivUser, noprivPass);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try {
         session.createTemporaryTopic();
         fail();
      } catch (JMSSecurityException e) {
         // expected
      }
   }

   @Test(timeout = 30000)
   public void testDeleteTemporaryTopic() throws Exception {
      Connection connection = connectionSupplier.createConnection(fullUser, fullPass);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final javax.jms.TemporaryTopic topic = session.createTemporaryTopic();
      assertNotNull(topic);

      Wait.assertTrue(() -> getProxyToAddress(topic.getTopicName()) != null, 3000, 10);

      topic.delete();

      Wait.assertTrue("Temp Queue should be deleted.", () -> getProxyToQueue(topic.getTopicName()) == null, 3000, 10);
   }

   @Test(timeout = 30000)
   public void testDeleteTemporaryTopicNegative() throws Exception {
      Connection connection = connectionSupplier.createConnection(createOnlyUser, createOnlyPass);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final javax.jms.TemporaryTopic topic = session.createTemporaryTopic();
      assertNotNull(topic);

      Wait.assertTrue(() -> getProxyToAddress(topic.getTopicName()) != null, 3000, 10);

      try {
         topic.delete();
         fail();
      } catch (JMSSecurityException e) {
         // expected
      }

      connection.close();

      Wait.assertTrue(() -> getProxyToAddress(topic.getTopicName()) == null, 3000, 10);
   }
}
