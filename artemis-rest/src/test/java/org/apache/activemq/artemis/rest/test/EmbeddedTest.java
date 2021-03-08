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
package org.apache.activemq.artemis.rest.test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.io.Serializable;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.rest.HttpHeaderProperty;
import org.apache.activemq.artemis.rest.integration.EmbeddedRestActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EmbeddedTest {
   private static final Logger log = Logger.getLogger(EmbeddedTest.class);

   public static EmbeddedRestActiveMQ server;

   @BeforeClass
   public static void startEmbedded() throws Exception {
      server = new EmbeddedRestActiveMQ(null);
      server.getManager().setConfigResourcePath("activemq-rest.xml");
      SecurityConfiguration securityConfiguration = new SecurityConfiguration();
      securityConfiguration.addUser("guest", "guest");
      securityConfiguration.addRole("guest", "guest");
      securityConfiguration.setDefaultUser("guest");
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), securityConfiguration);
      server.getEmbeddedActiveMQ().setSecurityManager(securityManager);
      server.start();
   }

   @AfterClass
   public static void stopEmbedded() throws Exception {
      server.stop();
      server = null;
   }

   public static void publish(String destination, Serializable object, String contentType) throws Exception {
      ConnectionFactory factory = ActiveMQJMSClient.createConnectionFactory("vm://0","cf");
      Connection conn = factory.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination dest = session.createQueue(destination);

      try {
         Assert.assertNotNull("Destination was null", dest);
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

   @Test
   public void testTransform() throws Exception {

      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/queues/exampleQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = response.getLinkHeader().getLinkByTitle("create");
      log.debug("create: " + sender);
      Link consumers = response.getLinkHeader().getLinkByTitle("pull-consumers");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = response.getLinkHeader().getLinkByTitle("consume-next");
      log.debug("consume-next: " + consumeNext);

      // test that Accept header is used to set content-type
      {
         TransformTest.Order order = new TransformTest.Order();
         order.setName("1");
         order.setAmount("$5.00");
         publish("exampleQueue", order, null);

         ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/xml").post(String.class);
         Assert.assertEquals(200, res.getStatus());
         Assert.assertTrue(res.getHeaders().getFirst("Content-Type").toString().toLowerCase().contains("application/xml"));
         TransformTest.Order order2 = res.getEntity(TransformTest.Order.class);
         Assert.assertEquals(order, order2);
         consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
         res.releaseConnection();
         Assert.assertNotNull(consumeNext);
      }

      // test that Accept header is used to set content-type
      {
         TransformTest.Order order = new TransformTest.Order();
         order.setName("1");
         order.setAmount("$5.00");
         publish("exampleQueue", order, null);

         ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/json").post(String.class);
         Assert.assertEquals(200, res.getStatus());
         Assert.assertEquals("application/json", res.getHeaders().getFirst("Content-Type").toString().toLowerCase());
         TransformTest.Order order2 = res.getEntity(TransformTest.Order.class);
         Assert.assertEquals(order, order2);
         consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
         res.releaseConnection();
         Assert.assertNotNull(consumeNext);
      }

      // test that message property is used to set content type
      {
         TransformTest.Order order = new TransformTest.Order();
         order.setName("2");
         order.setAmount("$15.00");
         publish("exampleQueue", order, "application/xml");

         ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").post(String.class);
         Assert.assertEquals(200, res.getStatus());
         Assert.assertTrue(res.getHeaders().getFirst("Content-Type").toString().toLowerCase().contains("application/xml"));
         TransformTest.Order order2 = res.getEntity(TransformTest.Order.class);
         Assert.assertEquals(order, order2);
         consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
         res.releaseConnection();
         Assert.assertNotNull(consumeNext);
      }
   }
}
