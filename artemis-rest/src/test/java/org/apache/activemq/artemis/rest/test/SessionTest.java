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

import org.apache.activemq.artemis.rest.queue.QueueDeployment;
import org.apache.activemq.artemis.rest.topic.TopicDeployment;
import org.apache.activemq.artemis.rest.util.Constants;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class SessionTest extends MessageTestBase {
   private static final Logger log = Logger.getLogger(SessionTest.class);

   @BeforeClass
   public static void setup() throws Exception {
      QueueDeployment deployment1 = new QueueDeployment("testQueue", true);
      manager.getQueueManager().deploy(deployment1);
      TopicDeployment deployment = new TopicDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testTopic");
      manager.getTopicManager().deploy(deployment);
   }

   @Test
   public void testRestartFromAutoAckSession() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link session = response.getLocationLink();
      response = session.request().head();
      response.releaseConnection();
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().head();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      response = sender.request().body("text/plain", Integer.toString(3)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("3", response.getEntity(String.class));
      response.releaseConnection();

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testTopicRestartFromAutoAckSession() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link session = response.getLocationLink();
      response = session.request().head();
      response.releaseConnection();
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      response = sender.request().body("text/plain", Integer.toString(3)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("3", response.getEntity(String.class));
      response.releaseConnection();

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRestartFromAckSession() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link session = response.getLocationLink();
      response = session.request().head();
      response.releaseConnection();
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("consume-next: " + consumeNext);
      Link ack = null;

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // consume
      response = consumeNext.request().header(Constants.WAIT_HEADER, "3").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNull(consumeNext);
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNotNull(ack);

      // acknowledge
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNotNull(consumeNext);
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNull(ack);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // consume
      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNull(consumeNext);
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNotNull(ack);

      // acknowledge
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNotNull(consumeNext);
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNull(ack);

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testTopicRestartFromAckSession() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link session = response.getLocationLink();
      response = session.request().head();
      response.releaseConnection();
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("consume-next: " + consumeNext);
      Link ack = null;

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // consume
      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNull(consumeNext);
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNotNull(ack);

      // acknowledge
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNotNull(consumeNext);
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNull(ack);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // consume
      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNull(consumeNext);
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNotNull(ack);

      // acknowledge
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNotNull(consumeNext);
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNull(ack);

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }
}
