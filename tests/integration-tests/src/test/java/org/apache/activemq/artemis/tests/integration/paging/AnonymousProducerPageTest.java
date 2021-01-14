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
package org.apache.activemq.artemis.tests.integration.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AnonymousProducerPageTest extends ActiveMQTestBase {

   protected final String protocol;

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection getParams() {
      return Arrays.asList(new Object[][]{
         {"AMQP"}, {"CORE"}, {"OPENWIRE"}});
   }

   public AnonymousProducerPageTest(String protocol) {
      this.protocol = protocol;
   }

   protected static final String NETTY_ACCEPTOR = "netty-acceptor";

   ActiveMQServer server;

   @Before
   public void createServer() throws Exception {

      int port = 5672;

      this.server = addServer(this.createServer(true, true));

      server.getConfiguration().getAddressesSettings().clear();
      server.getConfiguration().addAddressesSetting("#", new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE));

      server.getConfiguration().getAcceptorConfigurations().clear();
      server.getConfiguration().getAcceptorConfigurations().add(addAcceptorConfiguration(server, port));
      server.getConfiguration().setName(getName());
      server.getConfiguration().setJournalDirectory(server.getConfiguration().getJournalDirectory() + port);
      server.getConfiguration().setBindingsDirectory(server.getConfiguration().getBindingsDirectory() + port);
      server.getConfiguration().setPagingDirectory(server.getConfiguration().getPagingDirectory() + port);
      server.getConfiguration().setLargeMessagesDirectory(server.getConfiguration().getLargeMessagesDirectory());
      server.getConfiguration().setJMXManagementEnabled(true);
      server.getConfiguration().setMessageExpiryScanPeriod(100);
      server.start();
   }

   protected TransportConfiguration addAcceptorConfiguration(ActiveMQServer server, int port) {
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, String.valueOf(port));
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, getConfiguredProtocols());
      HashMap<String, Object> amqpParams = new HashMap<>();
      configureAMQPAcceptorParameters(amqpParams);
      TransportConfiguration tc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, NETTY_ACCEPTOR, amqpParams);
      configureAMQPAcceptorParameters(tc);
      return tc;
   }
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      // None by default
   }

   protected void configureAMQPAcceptorParameters(TransportConfiguration tc) {
      // None by default
   }

   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Test(timeout = 60_000)
   public void testNotBlockOnGlobalMaxSizeWithAnonymousProduce() throws Exception {
      final int MSG_SIZE = 1000;
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < MSG_SIZE; i++) {
         builder.append('0');
      }
      final String data = builder.toString();
      final int MSG_COUNT = 3_000;

      // sending size to explode max size
      server.getPagingManager().addSize((int) ((PagingManagerImpl) server.getPagingManager()).getMaxSize());
      server.getPagingManager().addSize(100_000);

      server.getAddressSettingsRepository().addMatch("blockedQueue", new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK));

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:5672");
      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = session.createProducer(null);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);
      javax.jms.Queue jmsQueue = session.createQueue(getName());

      for (int i = 0; i < MSG_COUNT; i++) {
         TextMessage message = session.createTextMessage(data);
         producer.send(jmsQueue, message);
      }
      session.commit();
      if (protocol.equals("AMQP")) {
         // this is only valid for AMQP
         validatePolicyMismatch(session, producer);
      }
      connection.close();
   }

   private void validatePolicyMismatch(Session session, MessageProducer producer) throws JMSException {
      AssertionLoggerHandler.startCapture();
      try {
         producer.send(session.createQueue("blockedQueue"), session.createMessage());
         session.commit();
         Assert.assertTrue(AssertionLoggerHandler.findText("AMQ111004"));
         AssertionLoggerHandler.clear();
         producer.send(session.createQueue(getName()), session.createMessage());
         session.commit();
         Assert.assertFalse("The warning should be printed only once", AssertionLoggerHandler.findText("AMQ111004"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

}
