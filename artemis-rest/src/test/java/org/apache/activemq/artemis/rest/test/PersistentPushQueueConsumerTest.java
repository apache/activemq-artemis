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

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.rest.MessageServiceManager;
import org.apache.activemq.artemis.rest.queue.QueueDeployment;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.queue.push.xml.XmlLink;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.jboss.resteasy.test.EmbeddedContainer;
import org.junit.Assert;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

/**
 * Test durable queue push consumers
 */
public class PersistentPushQueueConsumerTest {

   public static MessageServiceManager manager;
   protected static ResteasyDeployment deployment;
   public static ActiveMQServer activeMQServer;

   public static void startup() throws Exception {
      Configuration configuration = new ConfigurationImpl().setPersistenceEnabled(false).setSecurityEnabled(false).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      activeMQServer = ActiveMQServers.newActiveMQServer(configuration);
      activeMQServer.start();

      deployment = EmbeddedContainer.start();
      manager = new MessageServiceManager(null);
      manager.start();
      deployment.getRegistry().addSingletonResource(manager.getQueueManager().getDestination());
      deployment.getRegistry().addSingletonResource(manager.getTopicManager().getDestination());
   }

   public static void shutdown() throws Exception {
      manager.stop();
      manager = null;
      EmbeddedContainer.stop();
      deployment = null;
      activeMQServer.stop();
      activeMQServer = null;
   }

   @Test
   public void testBridge() throws Exception {
      try {
         startup();

         String testName = "testBridge";
         deployBridgeQueues(testName);

         ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

         ClientResponse<?> response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
         System.out.println("create: " + sender);
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-consumers");
         System.out.println("push subscriptions: " + pushSubscriptions);

         request = new ClientRequest(generateURL("/queues/" + testName + "forwardQueue"));
         response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
         System.out.println("pull: " + consumers);
         response = Util.setAutoAck(consumers, true);
         Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
         System.out.println("poller: " + consumeNext);

         PushRegistration reg = new PushRegistration();
         reg.setDurable(true);
         reg.setDisableOnFailure(true);
         XmlLink target = new XmlLink();
         target.setHref(generateURL("/queues/" + testName + "forwardQueue"));
         target.setRelationship("destination");
         reg.setTarget(target);
         response = pushSubscriptions.request().body("application/xml", reg).post();
         response.releaseConnection();
         Assert.assertEquals(201, response.getStatus());

         shutdown();
         startup();
         deployBridgeQueues(testName);

         ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
         res.releaseConnection();
         Assert.assertEquals(201, res.getStatus());

         res = consumeNext.request().header("Accept-Wait", "2").post(String.class);

         Assert.assertEquals(200, res.getStatus());
         Assert.assertEquals("1", res.getEntity(String.class));
         res.releaseConnection();
         Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
         res = session.request().delete();
         res.releaseConnection();
         Assert.assertEquals(204, res.getStatus());

         manager.getQueueManager().getPushStore().removeAll();
      } finally {
         shutdown();
      }
   }

   private void deployBridgeQueues(String testName) throws Exception {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(testName);
      manager.getQueueManager().deploy(deployment);
      QueueDeployment deployment2 = new QueueDeployment();
      deployment2.setDuplicatesAllowed(true);
      deployment2.setDurableSend(false);
      deployment2.setName(testName + "forwardQueue");
      manager.getQueueManager().deploy(deployment2);
   }

   @Test
   public void testFailure() throws Exception {
      try {
         startup();

         String testName = "testFailure";
         QueueDeployment deployment = new QueueDeployment();
         deployment.setDuplicatesAllowed(true);
         deployment.setDurableSend(false);
         deployment.setName(testName);
         manager.getQueueManager().deploy(deployment);

         ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

         ClientResponse<?> response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
         System.out.println("create: " + sender);
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-consumers");
         System.out.println("push subscriptions: " + pushSubscriptions);

         PushRegistration reg = new PushRegistration();
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
         PushRegistration reg2 = response.getEntity(PushRegistration.class);
         Assert.assertEquals(reg.isDurable(), reg2.isDurable());
         Assert.assertEquals(reg.getTarget().getHref(), reg2.getTarget().getHref());
         Assert.assertFalse(reg2.isEnabled()); // make sure the failure disables the PushRegistration
         response.releaseConnection();

         manager.getQueueManager().getPushStore().removeAll();
      } finally {
         shutdown();
      }
   }
}
