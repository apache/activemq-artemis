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

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.rest.ActiveMQ;
import org.apache.activemq.artemis.rest.queue.QueueDeployment;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class TransformTest extends MessageTestBase {

   @BeforeClass
   public static void setup() throws Exception {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testQueue");
      manager.getQueueManager().deploy(deployment);
   }

   @XmlRootElement
   public static class Order implements Serializable {

      private static final long serialVersionUID = 2510412973800601968L;
      private String name;
      private String amount;

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      public String getAmount() {
         return amount;
      }

      public void setAmount(String amount) {
         this.amount = amount;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o)
            return true;
         if (o == null || getClass() != o.getClass())
            return false;

         Order order = (Order) o;

         if (!amount.equals(order.amount))
            return false;
         if (!name.equals(order.name))
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = name.hashCode();
         result = 31 * result + amount.hashCode();
         return result;
      }
   }

   public static void publish(String destination, Serializable object, String contentType) throws Exception {
      ClientSession session = manager.getQueueManager().getSessionFactory().createSession();
      try {
         ClientProducer producer = session.createProducer(destination);
         ClientMessage message = session.createMessage(Message.OBJECT_TYPE, false);
         if (contentType == null) {
            ActiveMQ.setEntity(message, object);
         } else
            ActiveMQ.setEntity(message, object, contentType);
         producer.send(message);
         session.start();
      } finally {
         session.close();
      }

   }

   @Test
   public void testTransform() throws Exception {

      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("consume-next: " + consumeNext);

      // test that Accept header is used to set content-type
      {
         Order order = new Order();
         order.setName("1");
         order.setAmount("$5.00");
         publish("testQueue", order, null);

         response = consumeNext.request().accept("application/xml").post(String.class);
         Assert.assertEquals(200, response.getStatus());
         Assert.assertEquals("application/xml", response.getHeaders().getFirst("Content-Type").toString().toLowerCase());
         Order order2 = response.getEntity(Order.class);
         response.releaseConnection();
         Assert.assertEquals(order, order2);
         consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
         Assert.assertNotNull(consumeNext);
      }

      // test that Accept header is used to set content-type
      {
         Order order = new Order();
         order.setName("1");
         order.setAmount("$5.00");
         publish("testQueue", order, null);

         response = consumeNext.request().accept("application/json").post(String.class);
         Assert.assertEquals(200, response.getStatus());
         Assert.assertEquals("application/json", response.getHeaders().getFirst("Content-Type").toString().toLowerCase());
         Order order2 = response.getEntity(Order.class);
         response.releaseConnection();
         Assert.assertEquals(order, order2);
         consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
         Assert.assertNotNull(consumeNext);
      }

      // test that message property is used to set content type
      {
         Order order = new Order();
         order.setName("2");
         order.setAmount("$15.00");
         publish("testQueue", order, "application/xml");

         response = consumeNext.request().post(String.class);
         Assert.assertEquals(200, response.getStatus());
         Assert.assertEquals("application/xml", response.getHeaders().getFirst("Content-Type").toString().toLowerCase());
         Order order2 = response.getEntity(Order.class);
         response.releaseConnection();
         Assert.assertEquals(order, order2);
         consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
         Assert.assertNotNull(consumeNext);
      }
   }

   public static class Listener implements MessageHandler {

      public static Order order;
      public static CountDownLatch latch = new CountDownLatch(1);

      @Override
      public void onMessage(ClientMessage clientMessage) {
         System.out.println("onMessage!");
         try {
            order = ActiveMQ.getEntity(clientMessage, Order.class);
         } catch (Exception e) {
            e.printStackTrace();
         }
         latch.countDown();
      }
   }

   @Test
   public void testJmsConsumer() throws Exception {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      final String queueName = "testJmsConsumer";
      deployment.setName(queueName);
      manager.getQueueManager().deploy(deployment);
      ClientSession session = manager.getQueueManager().getSessionFactory().createSession();
      try {
         session.createConsumer(queueName).setMessageHandler(new Listener());
         session.start();

         ClientRequest request = new ClientRequest(generateURL(Util.getUrlPath(queueName)));

         ClientResponse<?> response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
         System.out.println("create: " + sender);
         Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
         System.out.println("pull: " + consumers);
         response = Util.setAutoAck(consumers, true);
         Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
         System.out.println("consume-next: " + consumeNext);

         // test that Accept header is used to set content-type
         {
            Order order = new Order();
            order.setName("1");
            order.setAmount("$5.00");
            response = sender.request().body("application/xml", order).post();
            response.releaseConnection();
            Assert.assertEquals(201, response.getStatus());

            Listener.latch.await(2, TimeUnit.SECONDS);
            Assert.assertNotNull(Listener.order);
            Assert.assertEquals(order, Listener.order);
         }
      } finally {
         session.close();
      }
   }

}
