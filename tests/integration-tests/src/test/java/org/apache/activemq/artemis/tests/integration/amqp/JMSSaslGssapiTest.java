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
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JMSSaslGssapiTest extends JMSClientTestSupport {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = JMSSaslGssapiTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }
   MiniKdc kdc = null;
   private final boolean debug = false;

   @Before
   public void setUpKerberos() throws Exception {
      kdc = new MiniKdc(MiniKdc.createConf(), temporaryFolder.newFolder("kdc"));
      kdc.start();

      // hard coded match, default_keytab_name in minikdc-krb5.conf template
      File userKeyTab = new File("target/test.krb5.keytab");
      kdc.createPrincipal(userKeyTab, "client", "amqp/localhost");

      if (debug) {
         java.util.logging.Logger logger = java.util.logging.Logger.getLogger("javax.security.sasl");
         logger.setLevel(java.util.logging.Level.FINEST);
         logger.addHandler(new java.util.logging.ConsoleHandler());
         for (java.util.logging.Handler handler : logger.getHandlers()) {
            handler.setLevel(java.util.logging.Level.FINEST);
         }
      }
   }

   @After
   public void stopKerberos() throws Exception {
      if (kdc != null) {
         kdc.stop();
      }
   }

   @Override
   protected boolean isSecurityEnabled() {
      return true;
   }

   @Override
   protected void configureBrokerSecurity(ActiveMQServer server) {
      server.getConfiguration().setSecurityEnabled(isSecurityEnabled());
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.setConfigurationName("Krb5Plus");
      securityManager.setConfiguration(null);

      final String roleName = "ALLOW_ALL";
      Role role = new Role(roleName, true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getSecurityRepository().addMatch(getQueueName().toString(), roles);

   }

   @Override
   protected String getJmsConnectionURIOptions() {
      return "amqp.saslMechanisms=GSSAPI";
   }

   @Override
   protected URI getBrokerQpidJMSConnectionURI() {

      try {
         int port = AMQP_PORT;

         // match the sasl.service <the host name>
         String uri = "amqp://localhost:" + port;

         if (!getJmsConnectionURIOptions().isEmpty()) {
            uri = uri + "?" + getJmsConnectionURIOptions();
         }

         return new URI(uri);
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      params.put("saslMechanisms", "GSSAPI");
      params.put("saslLoginConfigScope", "amqp-sasl-gssapi");
   }

   @Test(timeout = 600000)
   public void testConnection() throws Exception {
      Connection connection = createConnection("client", null);
      connection.start();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageConsumer consumer = session.createConsumer(queue);
         MessageProducer producer = session.createProducer(queue);

         final String text = RandomUtil.randomString();
         producer.send(session.createTextMessage(text));

         TextMessage m = (TextMessage) consumer.receive(1000);
         assertNotNull(m);
         assertEquals(text, m.getText());

      } finally {
         connection.close();
      }
   }
}
