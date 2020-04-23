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
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.queue.push.xml.XmlLink;
import org.apache.activemq.artemis.rest.util.Constants;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class PushQueueConsumerTest extends MessageTestBase {
   private static final Logger log = Logger.getLogger(PushQueueConsumerTest.class);

   enum PushRegistrationType {
      CLASS, BRIDGE, URI, TEMPLATE
   }

   @Test
   public void testBridge() throws Exception {
      Link destinationForConsumption = null;
      ClientResponse consumerResponse = null;
      Link pushSubscription = null;
      String messageContent = "1";

      try {
         // The name of the queue used for the test should match the name of the test
         String queue = "testBridge";
         String queueToPushTo = "pushedFrom-" + queue;
         log.debug("\n" + queue);
         deployQueue(queue);
         deployQueue(queueToPushTo);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destination = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         ClientResponse queueToPushToResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queueToPushTo))));
         ClientResponse autoAckResponse = setAutoAck(queueToPushToResponse, true);
         destinationForConsumption = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), autoAckResponse, "consume-next");

         pushSubscription = createPushRegistration(queueToPushTo, pushSubscriptions, PushRegistrationType.BRIDGE);

         sendMessage(destination, messageContent);

         consumerResponse = consume(destinationForConsumption, messageContent);
      } finally {
         cleanupConsumer(consumerResponse);
         cleanupSubscription(pushSubscription);
      }
   }

   private void cleanupSubscription(Link pushSubscription) throws Exception {
      if (pushSubscription != null) {
         ClientResponse<?> response = pushSubscription.request().delete();
         response.releaseConnection();
         Assert.assertEquals(204, response.getStatus());
      }
   }

   @Test
   public void testClass() throws Exception {
      Link destinationForConsumption = null;
      ClientResponse consumerResponse = null;
      Link pushSubscription = null;
      String messageContent = "1";

      try {
         // The name of the queue used for the test should match the name of the test
         String queue = "testClass";
         String queueToPushTo = "pushedFrom-" + queue;
         log.debug("\n" + queue);

         deployQueue(queue);
         deployQueue(queueToPushTo);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destinationForSend = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         ClientResponse queueToPushToResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queueToPushTo))));
         ClientResponse autoAckResponse = setAutoAck(queueToPushToResponse, true);
         destinationForConsumption = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), autoAckResponse, "consume-next");

         pushSubscription = createPushRegistration(queueToPushTo, pushSubscriptions, PushRegistrationType.CLASS);

         sendMessage(destinationForSend, messageContent);

         consumerResponse = consume(destinationForConsumption, messageContent);
      } finally {
         cleanupConsumer(consumerResponse);
         cleanupSubscription(pushSubscription);
      }
   }

   @Test
   public void testTemplate() throws Exception {
      Link destinationForConsumption = null;
      ClientResponse consumerResponse = null;
      Link pushSubscription = null;
      String messageContent = "1";

      try {
         // The name of the queue used for the test should match the name of the test
         String queue = "testTemplate";
         String queueToPushTo = "pushedFrom-" + queue;
         log.debug("\n" + queue);

         deployQueue(queue);
         deployQueue(queueToPushTo);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destinationForSend = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         ClientResponse queueToPushToResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queueToPushTo))));
         Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueToPushToResponse, "pull-consumers");
         Link createWithId = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueToPushToResponse, "create-with-id");
         ClientResponse autoAckResponse = Util.setAutoAck(consumers, true);
         destinationForConsumption = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), autoAckResponse, "consume-next");

         pushSubscription = createPushRegistration(createWithId.getHref(), pushSubscriptions, PushRegistrationType.TEMPLATE);

         sendMessage(destinationForSend, messageContent);

         consumerResponse = consume(destinationForConsumption, messageContent);
      } finally {
         cleanupConsumer(consumerResponse);
         cleanupSubscription(pushSubscription);
      }
   }

   @Path("/my")
   public static class MyResource {

      public static String got_it;

      @PUT
      public void put(String str) {
         got_it = str;
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
      Link pushSubscription = null;
      String messageContent = "1";

      try {
         // The name of the queue used for the test should match the name of the test
         String queue = "testUri";
         String queueToPushTo = "pushedFrom-" + queue;
         log.debug("\n" + queue);

         deployQueue(queue);
         deployQueue(queueToPushTo);
         server.getJaxrsServer().getDeployment().getRegistry().addPerRequestResource(MyResource.class);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destinationForSend = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         pushSubscription = createPushRegistration(generateURL("/my"), pushSubscriptions, PushRegistrationType.URI);

         sendMessage(destinationForSend, messageContent);

         Thread.sleep(100);

         Assert.assertEquals(messageContent, MyResource.got_it);
      } finally {
         cleanupSubscription(pushSubscription);
      }
   }

   @Test
   public void testUriWithMultipleSessions() throws Exception {
      Link pushSubscription = null;
      String messageContent = "1";
      final int CONCURRENT = 10;

      try {
         // The name of the queue used for the test should match the name of the test
         String queue = "testUriWithMultipleSessions";
         String queueToPushTo = "pushedFrom-" + queue;
         log.debug("\n" + queue);

         deployQueue(queue);
         deployQueue(queueToPushTo);
         server.getJaxrsServer().getDeployment().getRegistry().addPerRequestResource(MyConcurrentResource.class);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destinationForSend = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         pushSubscription = createPushRegistration(generateURL("/myConcurrent"), pushSubscriptions, PushRegistrationType.URI, CONCURRENT);

         for (int i = 0; i < CONCURRENT; i++) {
            sendMessage(destinationForSend, messageContent);
         }

         // wait until all the invocations have completed
         while (MyConcurrentResource.concurrentInvocations.get() > 0) {
            Thread.sleep(100);
         }

         Assert.assertEquals(CONCURRENT, MyConcurrentResource.maxConcurrentInvocations.get());
      } finally {
         cleanupSubscription(pushSubscription);
      }
   }

   private void deployQueue(String queueName) throws Exception {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(queueName);
      manager.getQueueManager().deploy(deployment);
   }

   private ClientResponse consume(Link destination, String expectedContent) throws Exception {
      ClientResponse response;
      response = destination.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals(expectedContent, response.getEntity(String.class));
      response.releaseConnection();
      return response;
   }

   private void sendMessage(Link sender, String content) throws Exception {
      ClientResponse sendMessageResponse = sender.request().body("text/plain", content).post();
      sendMessageResponse.releaseConnection();
      Assert.assertEquals(201, sendMessageResponse.getStatus());
   }

   private ClientResponse setAutoAck(ClientResponse response, boolean ack) throws Exception {
      Link pullConsumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      ClientResponse autoAckResponse = pullConsumers.request().formParameter("autoAck", Boolean.toString(ack)).post();
      autoAckResponse.releaseConnection();
      Assert.assertEquals(201, autoAckResponse.getStatus());
      return autoAckResponse;
   }

   private void cleanupConsumer(ClientResponse consumerResponse) throws Exception {
      if (consumerResponse != null) {
         Link consumer = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), consumerResponse, "consumer");
         ClientResponse<?> response = consumer.request().delete();
         response.releaseConnection();
         Assert.assertEquals(204, response.getStatus());
      }
   }

   private Link createPushRegistration(String queueToPushTo,
                                       Link pushSubscriptions,
                                       PushRegistrationType pushRegistrationType) throws Exception {
      return createPushRegistration(queueToPushTo, pushSubscriptions, pushRegistrationType, 1);
   }

   private Link createPushRegistration(String queueToPushTo,
                                       Link pushSubscriptions,
                                       PushRegistrationType pushRegistrationType,
                                       int sessionCount) throws Exception {
      PushRegistration reg = new PushRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      if (pushRegistrationType == PushRegistrationType.CLASS) {
         target.setHref(generateURL(Util.getUrlPath(queueToPushTo)));
         target.setClassName(ActiveMQPushStrategy.class.getName());
      } else if (pushRegistrationType == PushRegistrationType.BRIDGE) {
         target.setHref(generateURL(Util.getUrlPath(queueToPushTo)));
         target.setRelationship("destination");
      } else if (pushRegistrationType == PushRegistrationType.TEMPLATE) {
         target.setHref(queueToPushTo);
         target.setRelationship("template");
      } else if (pushRegistrationType == PushRegistrationType.URI) {
         target.setMethod("put");
         target.setHref(queueToPushTo);
      }
      reg.setTarget(target);
      reg.setSessionCount(sessionCount);
      ClientResponse pushRegistrationResponse = pushSubscriptions.request().body("application/xml", reg).post();
      pushRegistrationResponse.releaseConnection();
      Assert.assertEquals(201, pushRegistrationResponse.getStatus());
      Link pushSubscription = pushRegistrationResponse.getLocationLink();
      return pushSubscription;
   }
}
