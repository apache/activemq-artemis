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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.rest.HttpHeaderProperty;
import org.apache.activemq.artemis.rest.queue.push.xml.XmlLink;
import org.apache.activemq.artemis.rest.topic.PushTopicRegistration;
import org.apache.activemq.artemis.rest.topic.TopicDeployment;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class SelectorTest extends MessageTestBase {

   public static ConnectionFactory connectionFactory;
   public static String topicName = "testTopic";
   public static String prefixedTopicName = ActiveMQDestination.createTopicAddressFromName(topicName).toString();

   @BeforeClass
   public static void setup() throws Exception {
      connectionFactory = new ActiveMQJMSConnectionFactory(manager.getQueueManager().getServerLocator());
      System.out.println("Queue name: " + prefixedTopicName);
      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(topicName);
      manager.getTopicManager().deploy(deployment);
   }

   @XmlRootElement
   public static class Order implements Serializable {

      private static final long serialVersionUID = 482698090549294508L;
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
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }

         Order order = (Order) o;

         if (!amount.equals(order.amount)) {
            return false;
         }
         if (!name.equals(order.name)) {
            return false;
         }

         return true;
      }

      @Override
      public String toString() {
         return "Order{" +
            "name='" + name + '\'' +
            '}';
      }

      @Override
      public int hashCode() {
         int result = name.hashCode();
         result = 31 * result + amount.hashCode();
         return result;
      }
   }

   public static Destination createDestination(String dest) {
      ActiveMQDestination destination = (ActiveMQDestination) ActiveMQDestination.fromPrefixedName(dest);
      System.out.println("SimpleAddress: " + destination.getSimpleAddress());
      return destination;
   }

   public static void publish(String dest, Serializable object, String contentType, String tag) throws Exception {
      Connection conn = connectionFactory.createConnection();
      try {
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = createDestination(dest);
         MessageProducer producer = session.createProducer(destination);
         ObjectMessage message = session.createObjectMessage();

         if (contentType != null) {
            message.setStringProperty(HttpHeaderProperty.CONTENT_TYPE, contentType);
         }
         if (tag != null) {
            message.setStringProperty("MyTag", tag);
         }
         message.setObject(object);

         producer.send(message);
      } finally {
         conn.close();
      }
   }

   @Path("/push")
   public static class PushReceiver {

      public static Order oneOrder;
      public static Order twoOrder;

      @POST
      @Path("one")
      public void one(Order order) {
         oneOrder = order;
      }

      @POST
      @Path("two")
      public void two(Order order) {
         twoOrder = order;
      }

   }

   @Test
   public void testPush() throws Exception {
      server.getJaxrsServer().getDeployment().getRegistry().addPerRequestResource(PushReceiver.class);
      ClientRequest request = new ClientRequest(generateURL("/topics/" + topicName));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-subscriptions");
      System.out.println("push: " + consumers);

      PushTopicRegistration oneReg = new PushTopicRegistration();
      oneReg.setDurable(false);
      XmlLink target = new XmlLink();
      target.setMethod("post");
      target.setHref(generateURL("/push/one"));
      target.setType("application/xml");
      oneReg.setTarget(target);
      oneReg.setSelector("MyTag = '1'");
      response = consumers.request().body("application/xml", oneReg).post();
      response.releaseConnection();
      Link oneSubscription = response.getLocationLink();

      PushTopicRegistration twoReg = new PushTopicRegistration();
      twoReg.setDurable(false);
      target = new XmlLink();
      target.setMethod("post");
      target.setHref(generateURL("/push/two"));
      target.setType("application/xml");
      twoReg.setTarget(target);
      twoReg.setSelector("MyTag = '2'");
      response = consumers.request().body("application/xml", twoReg).post();
      response.releaseConnection();
      Link twoSubscription = response.getLocationLink();

      Order order = new Order();
      order.setName("1");
      order.setAmount("$5.00");
      publish(prefixedTopicName, order, null, "1");
      Thread.sleep(200);
      Assert.assertEquals(order, PushReceiver.oneOrder);

      order.setName("2");
      publish(prefixedTopicName, order, null, "2");
      Thread.sleep(200);
      Assert.assertEquals(order, PushReceiver.twoOrder);

      order.setName("3");
      publish(prefixedTopicName, order, null, "2");
      Thread.sleep(200);
      Assert.assertEquals(order, PushReceiver.twoOrder);

      order.setName("4");
      publish(prefixedTopicName, order, null, "1");
      Thread.sleep(200);
      Assert.assertEquals(order, PushReceiver.oneOrder);

      order.setName("5");
      publish(prefixedTopicName, order, null, "1");
      Thread.sleep(200);
      Assert.assertEquals(order, PushReceiver.oneOrder);

      order.setName("6");
      publish(prefixedTopicName, order, null, "1");
      Thread.sleep(200);
      Assert.assertEquals(order, PushReceiver.oneOrder);

      response = oneSubscription.request().delete();
      response.releaseConnection();
      response = twoSubscription.request().delete();
      response.releaseConnection();
   }

   @Test
   public void testPull() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/" + topicName));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      System.out.println("pull: " + consumers);
      response = consumers.request().formParameter("autoAck", "true").formParameter("selector", "MyTag = '1'").post();
      response.releaseConnection();

      Link consumeOne = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("consumeOne: " + consumeOne);
      response = consumers.request().formParameter("autoAck", "true").formParameter("selector", "MyTag = '2'").post();
      response.releaseConnection();
      Link consumeTwo = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("consumeTwo: " + consumeTwo);

      // test that Accept header is used to set content-type
      {
         Order order = new Order();
         order.setName("1");
         order.setAmount("$5.00");
         publish(prefixedTopicName, order, null, "1");
         order.setName("2");
         publish(prefixedTopicName, order, null, "2");
         order.setName("3");
         publish(prefixedTopicName, order, null, "2");
         order.setName("4");
         publish(prefixedTopicName, order, null, "1");
         order.setName("5");
         publish(prefixedTopicName, order, null, "1");
         order.setName("6");
         publish(prefixedTopicName, order, null, "1");

         {
            order.setName("1");
            consumeOne = consumeOrder(order, consumeOne);
            order.setName("2");
            consumeTwo = consumeOrder(order, consumeTwo);
            order.setName("3");
            consumeTwo = consumeOrder(order, consumeTwo);
            order.setName("4");
            consumeOne = consumeOrder(order, consumeOne);
            order.setName("5");
            consumeOne = consumeOrder(order, consumeOne);
            order.setName("6");
            consumeOne = consumeOrder(order, consumeOne);
         }
      }
   }

   private Link consumeOrder(Order order, Link consumeNext) throws Exception {
      ClientResponse<?> response = consumeNext.request().header("Accept-Wait", "4").accept("application/xml").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("application/xml", response.getHeaders().getFirst("Content-Type").toString().toLowerCase());
      Order order2 = response.getEntity(Order.class);
      Assert.assertEquals(order, order2);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      Assert.assertNotNull(consumeNext);
      response.releaseConnection();
      return consumeNext;
   }
}
