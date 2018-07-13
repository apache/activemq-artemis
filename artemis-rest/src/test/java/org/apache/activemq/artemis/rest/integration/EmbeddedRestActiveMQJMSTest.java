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
package org.apache.activemq.artemis.rest.integration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.rest.HttpHeaderProperty;
import org.apache.activemq.artemis.rest.test.TransformTest;
import org.apache.activemq.artemis.spi.core.naming.BindingRegistry;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.jboss.resteasy.test.TestPortProvider;
import org.apache.activemq.artemis.rest.test.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EmbeddedRestActiveMQJMSTest {

   private static EmbeddedRestActiveMQJMS server;
   private static Link consumeNext;

   @BeforeClass
   public static void startEmbedded() throws Exception {
      server = new EmbeddedRestActiveMQJMS(null);
      assertNotNull(server.embeddedActiveMQ);
      server.getManager().setConfigResourcePath("activemq-rest.xml");

      SecurityConfiguration securityConfiguration = createDefaultSecurityConfiguration();
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), securityConfiguration);
      server.getEmbeddedJMS().setSecurityManager(securityManager);

      server.start();
      List<String> connectors = createInVmConnector();
      server.getEmbeddedJMS().getJMSServerManager().createConnectionFactory("ConnectionFactory", false, JMSFactoryType.CF, connectors, "ConnectionFactory");

      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/queues/exampleQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      assertEquals(200, response.getStatus());
      Link sender = response.getLinkHeader().getLinkByTitle("create");
      System.out.println("create: " + sender);
      Link consumers = response.getLinkHeader().getLinkByTitle("pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      consumeNext = response.getLinkHeader().getLinkByTitle("consume-next");
      System.out.println("consume-next: " + consumeNext);
   }

   private static List<String> createInVmConnector() {
      List<String> connectors = new ArrayList<>();
      connectors.add("in-vm");
      return connectors;
   }

   @AfterClass
   public static void stopEmbedded() throws Exception {
      server.stop();
      server = null;
   }

   @Test
   public void shouldReturnStatusOK() throws Exception {
      TransformTest.Order order = createTestOrder("1", "$5.00");
      publish("exampleQueue", order, null);

      ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/xml").post(String.class);

      assertEquals(200, res.getStatus());
      res.releaseConnection();
   }

   @Test
   public void shouldReturnPublishedEntity() throws Exception {
      TransformTest.Order order = createTestOrder("1", "$5.00");

      publish("exampleQueue", order, null);
      ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/xml").post(String.class);

      TransformTest.Order order2 = res.getEntity(TransformTest.Order.class);
      assertEquals(order, order2);
      res.releaseConnection();
   }

   @Test
   public void shouldReturnLink() throws Exception {
      TransformTest.Order order = createTestOrder("1", "$5.00");
      publish("exampleQueue", order, null);

      ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/xml").post(String.class);

      consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
      res.releaseConnection();
      assertNotNull(consumeNext);
   }

   @Test
   public void shouldUseXmlAcceptHeaderToSetContentType() throws Exception {
      TransformTest.Order order = createTestOrder("1", "$5.00");
      publish("exampleQueue", order, null);

      ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/xml").post(String.class);

      assertEquals("application/xml", res.getHeaders().getFirst("Content-Type").toString().toLowerCase());

      consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
      res.releaseConnection();
      assertNotNull(consumeNext);
   }

   @Test
   public void shouldUseMessagePropertyToSetContentType() throws Exception {
      TransformTest.Order order = createTestOrder("2", "$15.00");
      publish("exampleQueue", order, "application/xml");

      ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").post(String.class);

      assertEquals("application/xml", res.getHeaders().getFirst("Content-Type").toString().toLowerCase());

      consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
      res.releaseConnection();
      assertNotNull(consumeNext);
   }

   @Test
   public void shouldUseJsonAcceptHeaderToSetContentType() throws Exception {
      TransformTest.Order order = createTestOrder("1", "$5.00");
      publish("exampleQueue", order, null);

      ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/json").post(String.class);
      assertEquals("application/json", res.getHeaders().getFirst("Content-Type").toString().toLowerCase());

      consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
      res.releaseConnection();
      assertNotNull(consumeNext);
   }

   private static Connection createConnection() throws JMSException {
      BindingRegistry reg = server.getRegistry();
      ConnectionFactory factory = (ConnectionFactory) reg.lookup("ConnectionFactory");
      return factory.createConnection();
   }

   private static SecurityConfiguration createDefaultSecurityConfiguration() {
      SecurityConfiguration securityConfiguration = new SecurityConfiguration();
      securityConfiguration.addUser("guest", "guest");
      securityConfiguration.addRole("guest", "guest");
      securityConfiguration.setDefaultUser("guest");
      return securityConfiguration;
   }

   private TransformTest.Order createTestOrder(String name, String amount) {
      TransformTest.Order order = new TransformTest.Order();
      order.setName(name);
      order.setAmount(amount);
      return order;
   }

   private static void publish(String destination, Serializable object, String contentType) throws Exception {
      Connection conn = createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination dest = session.createQueue(destination);

      try {
         assertNotNull("Destination was null", dest);
         MessageProducer producer = session.createProducer(dest);
         ObjectMessage message = session.createObjectMessage();

         if (contentType != null) {
            message.setStringProperty(HttpHeaderProperty.CONTENT_TYPE, contentType);
         }
         message.setObject(object);

         producer.send(message);
      } finally {
         conn.close();
      }
   }
}
