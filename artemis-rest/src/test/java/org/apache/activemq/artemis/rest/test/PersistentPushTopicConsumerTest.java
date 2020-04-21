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

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.rest.MessageServiceManager;
import org.apache.activemq.artemis.rest.queue.push.xml.XmlLink;
import org.apache.activemq.artemis.rest.topic.PushTopicRegistration;
import org.apache.activemq.artemis.rest.topic.TopicDeployment;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.jboss.resteasy.test.EmbeddedContainer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

/**
 * Test durable queue push consumers
 */
public class PersistentPushTopicConsumerTest {
   private static final Logger log = Logger.getLogger(PersistentPushTopicConsumerTest.class);

   public static ActiveMQServer server;
   public static MessageServiceManager manager;
   protected static ResteasyDeployment deployment;

   @BeforeClass
   public static void setup() throws Exception {
      Configuration configuration = new ConfigurationImpl().setPersistenceEnabled(false).setSecurityEnabled(false).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      server = ActiveMQServers.newActiveMQServer(configuration);
      server.start();
   }

   @AfterClass
   public static void cleanup() throws Exception {
      server.stop();
      server = null;
   }

   public static void startup() throws Exception {
      deployment = EmbeddedContainer.start();

      manager = new MessageServiceManager(null);
      manager.start();
      deployment.getRegistry().addSingletonResource(manager.getQueueManager().getDestination());
      deployment.getRegistry().addSingletonResource(manager.getTopicManager().getDestination());

      deployment.getRegistry().addPerRequestResource(Receiver.class);
   }

   public static void shutdown() throws Exception {
      manager.stop();
      manager = null;
      EmbeddedContainer.stop();
      deployment = null;
   }

   @Test
   public void testFailure() throws Exception {
      try {
         String testName = "testFailure";
         startup();
         deployTopic(testName);

         ClientRequest request = new ClientRequest(generateURL("/topics/" + testName));

         ClientResponse<?> response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
         log.debug("create: " + sender);
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-subscriptions");
         log.debug("push subscriptions: " + pushSubscriptions);

         PushTopicRegistration reg = new PushTopicRegistration();
         reg.setDurable(true);
         XmlLink target = new XmlLink();
         target.setHref("http://localhost:3333/error");
         target.setRelationship("uri");
         reg.setTarget(target);
         reg.setDisableOnFailure(true);
         reg.setMaxRetries(3);
         reg.setRetryWaitMillis(10);
         response = pushSubscriptions.request().body("application/xml", reg).post();
         Assert.assertEquals(201, response.getStatus());
         Link pushSubscription = response.getLocationLink();
         response.releaseConnection();

         ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
         res.releaseConnection();
         Assert.assertEquals(201, res.getStatus());

         Thread.sleep(5000);

         response = pushSubscription.request().get();
         PushTopicRegistration reg2 = response.getEntity(PushTopicRegistration.class);
         response.releaseConnection();
         Assert.assertEquals(reg.isDurable(), reg2.isDurable());
         Assert.assertEquals(reg.getTarget().getHref(), reg2.getTarget().getHref());
         Assert.assertFalse(reg2.isEnabled());
         response.releaseConnection();

         String destination = reg2.getDestination();
         ClientSession session = manager.getQueueManager().getSessionFactory().createSession(false, false, false);
         ClientSession.QueueQuery query = session.queueQuery(new SimpleString(destination));
         Assert.assertFalse(query.isExists());

         manager.getQueueManager().getPushStore().removeAll();
      } finally {
         shutdown();
      }
   }

   @Test
   public void testSuccessFirst() throws Exception {
      try {
         String testName = "testSuccessFirst";
         startup();
         deployTopic(testName);

         ClientRequest request = new ClientRequest(generateURL("/topics/" + testName));

         ClientResponse<?> response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link sender = MessageTestBase.getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
         log.debug("create: " + sender);
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "push-subscriptions");
         log.debug("push subscriptions: " + pushSubscriptions);

         String sub1 = generateURL("/subscribers/1");
         String sub2 = generateURL("/subscribers/2");

         PushTopicRegistration reg = new PushTopicRegistration();
         reg.setDurable(true);
         XmlLink target = new XmlLink();
         target.setHref(sub1);
         reg.setTarget(target);
         response = pushSubscriptions.request().body("application/xml", reg).post();
         response.releaseConnection();
         Assert.assertEquals(201, response.getStatus());

         reg = new PushTopicRegistration();
         reg.setDurable(true);
         target = new XmlLink();
         target.setHref(sub2);
         reg.setTarget(target);
         response = pushSubscriptions.request().body("application/xml", reg).post();
         response.releaseConnection();
         Assert.assertEquals(201, response.getStatus());

         shutdown();
         startup();
         deployTopic(testName);

         ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
         res.releaseConnection();
         Assert.assertEquals(201, res.getStatus());

         Receiver.latch.await(1, TimeUnit.SECONDS);

         Assert.assertEquals("1", Receiver.subscriber1);
         Assert.assertEquals("1", Receiver.subscriber2);

         manager.getTopicManager().getPushStore().removeAll();
      } finally {
         shutdown();
      }
   }

   @Path("/subscribers")
   public static class Receiver {

      public static String subscriber1;
      public static String subscriber2;
      public static CountDownLatch latch = new CountDownLatch(2);

      @Path("1")
      @POST
      @Consumes("text/plain")
      public void postOne(String msg) {
         log.debug("in subscribers 1!!!!!!!!!! " + msg);
         subscriber1 = msg;
         latch.countDown();
      }

      @Path("2")
      @POST
      @Consumes("text/plain")
      public void postTwo(String msg) {
         log.debug("in subscribers 2!!!!!!!!!! " + msg);
         subscriber2 = msg;
         latch.countDown();
      }
   }

   private void deployTopic(String topicName) throws Exception {
      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(topicName);
      manager.getTopicManager().deploy(deployment);

   }
}
