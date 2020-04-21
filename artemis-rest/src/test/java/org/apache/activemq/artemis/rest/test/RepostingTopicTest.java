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

/**
 * repost on same consume-next
 * repost on old consume-next
 * repost on same consume-next with timeouts
 * repost on same ack-next
 * repost successful ack
 * repost successful unack
 * repost ack after unack
 * repost unack after ack
 * post on old ack-next
 * post on old ack-next after an ack
 * ack with an old ack link
 */
public class RepostingTopicTest extends MessageTestBase {
   private static final Logger log = Logger.getLogger(RepostingTopicTest.class);

   @BeforeClass
   public static void setup() throws Exception {
      TopicDeployment deployment1 = new TopicDeployment("testTopic", true);
      manager.getTopicManager().deploy(deployment1);
   }

   @Test
   public void testReconnectOnNamedSubscriber() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = consumers.request().formParameter("name", "bill").post();
      response.releaseConnection();

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // recreate subscription a second time as named.  Should pick up old one.

      response = consumers.request().formParameter("name", "bill").post();
      response.releaseConnection();
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("resource consume-next: " + consumeNext);
      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      response = consumeNext.request().header("Accept-Wait", "2").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();

      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRestartOnDurableNamedSubscriber() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = consumers.request().formParameter("name", "bill").formParameter("durable", "true").post();
      response.releaseConnection();

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      manager.getTopicManager().getDestination().findTopic("testTopic").getSubscriptions().stop();

      // recreate subscription a second time as named.  Should pick up old one.

      response = consumers.request().formParameter("name", "bill").formParameter("durable", "true").post();
      response.releaseConnection();
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("resource consume-next: " + consumeNext);
      response = consumeNext.request().header("Accept-Wait", "2").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      response = consumeNext.request().header("Accept-Wait", "2").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();

      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRestartOnNonDurableNamedSubscriber() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = consumers.request().formParameter("name", "bill").post();
      response.releaseConnection();

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      manager.getTopicManager().getDestination().findTopic("testTopic").getSubscriptions().stop();

      // recreate subscription a second time as named.  Should pick up old one.

      response = consumers.request().formParameter("name", "bill").post();
      response.releaseConnection();
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("resource consume-next: " + consumeNext);
      response = consumeNext.request().header("Accept-Wait", "2").post(String.class);
      response.releaseConnection();
      Assert.assertEquals(503, response.getStatus());

      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testPostOnSameConsumeNext() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("resource consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("session 1st consumeNext: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();

      session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("session 2nd consumeNext: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(3)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("3", response.getEntity(String.class));
      response.releaseConnection();

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testPostOnOldConsumeNext() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("resource consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      Link firstConsumeNext = consumeNext;
      log.debug("session 1st consumeNext: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("session 2nd consumeNext: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(3)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("3", response.getEntity(String.class));
      response.releaseConnection();

      response = firstConsumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(412, response.getStatus());
      log.debug(response.getEntity(String.class));
      response.releaseConnection();

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testPostOnSameConsumeNextWithTimeout() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("resource consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("session 1st consumeNext: " + consumeNext);

      // test timeout here
      response = consumeNext.request().post(String.class);
      response.releaseConnection();
      Assert.assertEquals(503, response.getStatus());
      session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("session 2nd consumeNext: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(3)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("3", response.getEntity(String.class));
      response.releaseConnection();

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testPostOnSameAcknowledgeNextAndAck() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("session 1st acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRepostSuccessfulUnacknowledge() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "false").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "false").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "false").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("session 1st acknowledge-next: " + consumeNext);

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRepostAckAfterUnacknowledge() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "false").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "true").post();
      Assert.assertEquals(412, response.getStatus());
      log.debug(response.getEntity(String.class));
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("session 1st acknowledge-next: " + consumeNext);

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRepostUnAckAfterAcknowledge() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "false").post();
      Assert.assertEquals(412, response.getStatus());
      log.debug(response.getEntity(String.class));
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("session 1st acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testPostOnOldAcknowledgeNextAndAck() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      log.debug("session: " + session);
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Link oldAck = ack;
      log.debug("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      log.debug("session 1st acknowledge-next: " + consumeNext);
      Link firstConsumeNext = consumeNext;

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      log.debug("ack: " + ack);

      response = oldAck.request().formParameter("acknowledge", "true").post();
      Assert.assertEquals(412, response.getStatus());
      log.debug(response.getEntity(String.class));
      response.releaseConnection();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");

      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");

      response = consumeNext.request().post(String.class);
      response.releaseConnection();
      Assert.assertEquals(503, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }
}
