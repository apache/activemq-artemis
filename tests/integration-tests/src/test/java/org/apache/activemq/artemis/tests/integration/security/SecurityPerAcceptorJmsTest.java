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
package org.apache.activemq.artemis.tests.integration.security;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameter;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class SecurityPerAcceptorJmsTest extends ActiveMQTestBase {

   private enum Protocol {
      CORE, AMQP, OPENWIRE
   }

   @Parameters(name = "protocol={0}")
   public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][] {
         {Protocol.CORE},
         {Protocol.AMQP},
         {Protocol.OPENWIRE}
      });
   }

   @Parameter(index = 0)
   public Protocol protocol;

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = SecurityPerAcceptorJmsTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   private ConnectionFactory cf;
   private final String URL = "tcp://127.0.0.1:61616";

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
      switch (protocol) {
         case CORE:
            cf = new ActiveMQConnectionFactory(URL);
            break;
         case OPENWIRE:
            cf = new org.apache.activemq.ActiveMQConnectionFactory(URL);
            break;
         case AMQP:
            cf = new JmsConnectionFactory("amqp://localhost:61616");

      }
   }

   @TestTemplate
   public void testJAASSecurityManagerAuthentication() throws Exception {
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true).setResolveProtocols(true).addAcceptorConfiguration("netty", URL + "?securityDomain=PropertiesLogin"), ManagementFactory.getPlatformMBeanServer(), new ActiveMQJAASSecurityManager(), false));
      server.start();
      try (Connection c = cf.createConnection("first", "secret")) {
         Thread.sleep(200);
      } catch (JMSException e) {
         fail("should not throw exception");
      }
   }

   @TestTemplate
   public void testJAASSecurityManagerAuthorizationNegative() throws Exception {
      final SimpleString ADDRESS = SimpleString.of("address");

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager();
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setResolveProtocols(true).addAcceptorConfiguration("netty", "tcp://127.0.0.1:61616?securityDomain=PropertiesLogin").setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", false, false, false, false, false, false, false, false, false, false, false, false));
      server.getConfiguration().putSecurityRoles("#", roles);

      // ensure advisory permission is still set for openwire to allow connection to succeed, alternative is url param jms.watchTopicAdvisories=false on the client connection factory
      roles = new HashSet<>();
      roles.add(new Role("programmers", false, true, false, false, true, true, false, false, true, false, false, false));
      server.getConfiguration().putSecurityRoles("ActiveMQ.Advisory.#", roles);

      server.start();
      server.addAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      server.createQueue(QueueConfiguration.of(ADDRESS).setAddress(ADDRESS).setRoutingType(RoutingType.ANYCAST));

      Connection c = cf.createConnection("first", "secret");
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // PRODUCE
      try {
         MessageProducer producer = s.createProducer(s.createQueue(ADDRESS.toString()));
         producer.send(s.createMessage());
         fail("should throw exception here");
      } catch (JMSException e) {
         e.printStackTrace();
         assertTrue(e.getMessage().contains("User: first does not have permission='SEND' on address address"));
      }

      // CONSUME
      try {
         MessageConsumer consumer = s.createConsumer(s.createQueue(ADDRESS.toString()));
         fail("should throw exception here");
      } catch (JMSException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='CONSUME' for queue address on address address"));
      }

      // BROWSE
      try {
         QueueBrowser browser = s.createBrowser(s.createQueue(ADDRESS.toString()));
         browser.getEnumeration();
         fail("should throw exception here");
      } catch (JMSException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='BROWSE' for queue address on address address"));
      }
      c.close();
   }

   @TestTemplate
   public void testJAASSecurityManagerAuthorizationPositive() throws Exception {
      final String ADDRESS = "address";

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true).setResolveProtocols(true).addAcceptorConfiguration("netty", "tcp://127.0.0.1:61616?securityDomain=PropertiesLogin"), ManagementFactory.getPlatformMBeanServer(), new ActiveMQJAASSecurityManager(), false));
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", true, true, true, true, true, true, true, true, true, true, false, false));
      server.getConfiguration().putSecurityRoles("#", roles);
      server.start();

      Connection c = cf.createConnection("first", "secret");
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // PRODUCE
      try {
         MessageProducer producer = s.createProducer(s.createQueue(ADDRESS));
         producer.send(s.createMessage());
      } catch (JMSException e) {
         fail("should not throw exception here");
      }

      // CONSUME
      try {
         MessageConsumer consumer = s.createConsumer(s.createQueue(ADDRESS));
      } catch (JMSException e) {
         fail("should not throw exception here");
      }

      // BROWSE
      try {
         QueueBrowser browser = s.createBrowser(s.createQueue(ADDRESS));
         browser.getEnumeration();
      } catch (JMSException e) {
         fail("should not throw exception here");
      }
      c.close();
   }
}
