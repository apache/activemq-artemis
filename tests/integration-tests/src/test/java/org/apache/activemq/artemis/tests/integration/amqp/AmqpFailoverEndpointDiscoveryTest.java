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
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AmqpFailoverEndpointDiscoveryTest extends FailoverTestBase {

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameterized.Parameters(name = "{0}")
   public static Collection<?> getParameters() {

      // these 3 are for comparison
      return Arrays.asList(new Object[][]{{"NON_SSL", 0}
         /*, {"SSL", 1} */ });
   }

   private final int protocol;

   public AmqpFailoverEndpointDiscoveryTest(String name, int protocol) {
      this.protocol = protocol;
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live) {
      return getNettyAcceptorTransportConfig(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      return getNettyConnectorTransportConfig(live);
   }

   @Test(timeout = 120000)
   public void testFailoverListWithAMQP() throws Exception {
      JmsConnectionFactory factory = getJmsConnectionFactory();
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(ADDRESS.toString());
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("hello before failover"));
         liveServer.crash(true, true);
         producer.send(session.createTextMessage("hello after failover"));
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
         TextMessage receive = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull(receive);
         Assert.assertEquals("hello before failover", receive.getText());
         receive = (TextMessage) consumer.receive(5000);
         Assert.assertEquals("hello after failover", receive.getText());
         Assert.assertNotNull(receive);
      }
   }

   private JmsConnectionFactory getJmsConnectionFactory() {
      if (protocol == 0) {
         return new JmsConnectionFactory("failover:(amqp://localhost:61616)");
      } else {
         String keystore = this.getClass().getClassLoader().getResource("client-side-keystore.jks").getFile();
         String truststore = this.getClass().getClassLoader().getResource("client-side-truststore.jks").getFile();
         return new JmsConnectionFactory("failover:(amqps://localhost:61616?transport.keyStoreLocation=" + keystore + "&transport.keyStorePassword=secureexample&transport.trustStoreLocation=" + truststore + "&transport.trustStorePassword=secureexample&transport.verifyHost=false)");
      }
   }

   private TransportConfiguration getNettyAcceptorTransportConfig(final boolean live) {
      Map<String, Object> server1Params = new HashMap<>();
      if (protocol == 1) {
         server1Params.put(TransportConstants.SSL_ENABLED_PROP_NAME, "true");

         server1Params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "server-side-keystore.jks");
         server1Params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
         server1Params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "server-side-truststore.jks");
         server1Params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      }

      if (live) {
         return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, server1Params);
      }

      server1Params.put(org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);

      return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, server1Params);
   }

   private TransportConfiguration getNettyConnectorTransportConfig(final boolean live) {
      Map<String, Object> server1Params = new HashMap<>();
      if (protocol == 1) {
         server1Params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
         server1Params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
         server1Params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
         server1Params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "client-side-keystore.jks");
         server1Params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      }
      if (live) {
         return new TransportConfiguration(NETTY_CONNECTOR_FACTORY, server1Params);
      }
      server1Params.put(org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      return new TransportConfiguration(NETTY_CONNECTOR_FACTORY, server1Params);
   }
}
