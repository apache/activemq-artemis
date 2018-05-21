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
package org.apache.activemq.artemis.tests.integration.ssl;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Map;

@RunWith(Parameterized.class)
public class SSLProviderTwoWayTest extends SSLTestBase {

   public SSLProviderTwoWayTest(String sslProvider, String clientSslProvider) {
      super(sslProvider, clientSslProvider);
   }

   @Override
   protected void configureSSLParameters(Map<String, Object> params) {
      super.configureSSLParameters(params);

      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, SERVER_SIDE_TRUSTSTORE);
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      params.put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, "JKS");
      params.put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);
   }

   @Test
   public void testProviderConfig() {
      NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor(getNettyAcceptorName());
      assertNotNull(acceptor);
      String sslProviderInUse = (String) acceptor.getConfiguration().get(TransportConstants.SSL_PROVIDER);
      assertEquals(sslProvider, sslProviderInUse);
      assertTrue((Boolean) acceptor.getConfiguration().get(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME));
   }

   @Test
   public void testProviderLoading2Way() throws Exception {
      if (!isOpenSSLSupported()) {
         System.out.println("*** Skip test on un-supported platform.");
         return;
      }

      final String text = "Hello SSL!";
      StringBuilder uri = new StringBuilder("tcp://" + tc.getParams().get(TransportConstants.HOST_PROP_NAME).toString()
              + ":" + tc.getParams().get(TransportConstants.PORT_PROP_NAME).toString());

      uri.append("?").append(TransportConstants.SSL_ENABLED_PROP_NAME).append("=true");
      uri.append("&").append(TransportConstants.SSL_PROVIDER).append("=").append(clientSslProvider);
      uri.append("&").append(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME).append("=").append("JKS");
      uri.append("&").append(TransportConstants.KEYSTORE_PATH_PROP_NAME).append("=").append(CLIENT_SIDE_KEYSTORE);
      uri.append("&").append(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME).append("=").append(PASSWORD);
      uri.append("&").append(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME).append("=JKS");
      uri.append("&").append(TransportConstants.TRUSTSTORE_PATH_PROP_NAME).append("=").append(CLIENT_SIDE_TRUSTSTORE);
      uri.append("&").append(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME).append("=").append(PASSWORD);

      System.out.println("uri: " + uri.toString());
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocator(uri.toString()));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(QUEUE, RoutingType.ANYCAST, QUEUE);
      ClientProducer producer = addClientProducer(session.createProducer(QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());

   }

}
