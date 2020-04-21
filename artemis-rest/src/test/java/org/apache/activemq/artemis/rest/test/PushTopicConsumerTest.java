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

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.rest.queue.QueueDeployment;
import org.apache.activemq.artemis.rest.queue.push.ActiveMQPushStrategy;
import org.apache.activemq.artemis.rest.queue.push.xml.XmlLink;
import org.apache.activemq.artemis.rest.topic.PushTopicRegistration;
import org.apache.activemq.artemis.rest.topic.TopicDeployment;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class PushTopicConsumerTest extends MessageTestBase {

   private static final Logger log = Logger.getLogger(PushTopicConsumerTest.class);

   @BeforeClass
   public static void setup() throws Exception {
      //      TopicDeployment deployment = new TopicDeployment();
      //      deployment.setDuplicatesAllowed(true);
      //      deployment.setDurableSend(false);
      //      deployment.setName("testTopic");
      //      manager.getTopicManager().deploy(deployment);
      //      QueueDeployment deployment2 = new QueueDeployment();
      //      deployment2.setDuplicatesAllowed(true);
      //      deployment2.setDurableSend(false);
      //      deployment2.setName("forwardQueue");
      //      manager.getQueueManager().deploy(deployment2);
   }

   @Test
   public void testBridge() throws Exception {
      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testBridge");
      manager.getTopicManager().deploy(deployment);
      QueueDeployment deployment2 = new QueueDeployment();
      deployment2.setDuplicatesAllowed(true);
      deployment2.setDurableSend(false);
      deployment2.setName("testBridgeForwardQueue");
      manager.getQueueManager().deploy(deployment2);

      ClientRequest request = new ClientRequest(generateURL("/topics/testBridge"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-subscriptions");
      log.debug("push subscriptions: " + pushSubscriptions);

      request = new ClientRequest(generateURL("/queues/testBridgeForwardQueue"));
      response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      PushTopicRegistration reg = new PushTopicRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      target.setHref(generateURL("/queues/testBridgeForwardQueue"));
      target.setRelationship("destination");
      reg.setTarget(target);
      response = pushSubscriptions.request().body("application/xml", reg).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      Link pushSubscription = response.getLocationLink();

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header("Accept-Wait", "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = pushSubscription.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testClass() throws Exception {
      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testClass");
      manager.getTopicManager().deploy(deployment);
      QueueDeployment deployment2 = new QueueDeployment();
      deployment2.setDuplicatesAllowed(true);
      deployment2.setDurableSend(false);
      deployment2.setName("testClassForwardQueue");
      manager.getQueueManager().deploy(deployment2);

      ClientRequest request = new ClientRequest(generateURL("/topics/testClass"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-subscriptions");
      log.debug("push subscriptions: " + pushSubscriptions);

      request = new ClientRequest(generateURL("/queues/testClassForwardQueue"));
      response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      PushTopicRegistration reg = new PushTopicRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      target.setHref(generateURL("/queues/testClassForwardQueue"));
      target.setClassName(ActiveMQPushStrategy.class.getName());
      reg.setTarget(target);
      response = pushSubscriptions.request().body("application/xml", reg).post();
      Assert.assertEquals(201, response.getStatus());
      Link pushSubscription = response.getLocationLink();
      response.releaseConnection();

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header("Accept-Wait", "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = pushSubscription.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testTemplate() throws Exception {
      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testTemplate");
      manager.getTopicManager().deploy(deployment);
      QueueDeployment deployment2 = new QueueDeployment();
      deployment2.setDuplicatesAllowed(true);
      deployment2.setDurableSend(false);
      deployment2.setName("testTemplateForwardQueue");
      manager.getQueueManager().deploy(deployment2);

      ClientRequest request = new ClientRequest(generateURL("/topics/testTemplate"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-subscriptions");
      log.debug("push subscriptions: " + pushSubscriptions);

      request = new ClientRequest(generateURL("/queues/testTemplateForwardQueue"));
      response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      Link createWithId = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create-with-id");
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      PushTopicRegistration reg = new PushTopicRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      target.setRelationship("template");
      target.setHref(createWithId.getHref());
      reg.setTarget(target);
      response = pushSubscriptions.request().body("application/xml", reg).post();
      Assert.assertEquals(201, response.getStatus());
      Link pushSubscription = response.getLocationLink();
      response.releaseConnection();

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header("Accept-Wait", "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = pushSubscription.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Path("/my")
   public static class MyResource {

      public static String gotit;

      @PUT
      public void put(String str) {
         gotit = str;
      }

   }

   @Path("/myConcurrent")
   public static class MyConcurrentResource {

      public static AtomicInteger concurrentInvocations = new AtomicInteger();
      public static AtomicInteger maxConcurrentInvocations = new AtomicInteger();

      @PUT
      public void put(String str) {
         concurrentInvocations.getAndIncrement();

         if (concurrentInvocations.get() > maxConcurrentInvocations.get()) {
            maxConcurrentInvocations.set(concurrentInvocations.get());
         }
         try {
            // sleep here so the concurrent invocations can stack up
            Thread.sleep(1000);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }

         concurrentInvocations.getAndDecrement();
      }
   }

   @Test
   public void testUri() throws Exception {
      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testUri");
      manager.getTopicManager().deploy(deployment);

      ClientRequest request = new ClientRequest(generateURL("/topics/testUri"));
      server.getJaxrsServer().getDeployment().getRegistry().addPerRequestResource(MyResource.class);

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-subscriptions");
      log.debug("push subscriptions: " + pushSubscriptions);

      PushTopicRegistration reg = new PushTopicRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      target.setMethod("put");
      target.setHref(generateURL("/my"));
      reg.setTarget(target);
      response = pushSubscriptions.request().body("application/xml", reg).post();
      Assert.assertEquals(201, response.getStatus());
      Link pushSubscription = response.getLocationLink();
      response.releaseConnection();

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      Thread.sleep(100);

      Assert.assertEquals("1", MyResource.gotit);
      response = pushSubscription.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testUriWithMultipleSessions() throws Exception {
      final int CONCURRENT = 10;

      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testUriWithMultipleSessions");
      manager.getTopicManager().deploy(deployment);

      ClientRequest request = new ClientRequest(generateURL("/topics/testUriWithMultipleSessions"));
      server.getJaxrsServer().getDeployment().getRegistry().addPerRequestResource(MyConcurrentResource.class);

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-subscriptions");
      log.debug("push subscriptions: " + pushSubscriptions);

      PushTopicRegistration reg = new PushTopicRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      target.setMethod("put");
      target.setHref(generateURL("/myConcurrent"));
      reg.setTarget(target);
      reg.setSessionCount(CONCURRENT);
      response = pushSubscriptions.request().body("application/xml", reg).post();
      Assert.assertEquals(201, response.getStatus());
      Link pushSubscription = response.getLocationLink();
      response.releaseConnection();

      for (int i = 0; i < CONCURRENT; i++) {
         response = sender.request().body("text/plain", Integer.toString(1)).post();
         response.releaseConnection();
         Assert.assertEquals(201, response.getStatus());
      }

      // wait until all the invocations have completed
      while (MyConcurrentResource.concurrentInvocations.get() > 0) {
         Thread.sleep(100);
      }

      Assert.assertEquals(CONCURRENT, MyConcurrentResource.maxConcurrentInvocations.get());

      response = pushSubscription.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }
}
