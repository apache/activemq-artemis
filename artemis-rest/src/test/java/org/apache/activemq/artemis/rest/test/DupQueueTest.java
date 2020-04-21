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
import org.apache.activemq.artemis.rest.util.Constants;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class DupQueueTest extends MessageTestBase {
   private static final Logger log = Logger.getLogger(DupQueueTest.class);

   @Test
   public void testDup() throws Exception {
      String testName = "testDup";
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(false);
      deployment.setDurableSend(false);
      deployment.setName(testName);
      manager.getQueueManager().deploy(deployment);

      ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

      ClientResponse<?> response = request.head();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("poller: " + consumeNext);

      ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
      Assert.assertEquals(307, res.getStatus());
      sender = res.getLocationLink();
      res.releaseConnection();
      log.debug("create-next: " + sender);
      Assert.assertNotNull(sender);
      res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create-next");
      res = sender.request().body("text/plain", Integer.toString(2)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      res.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      log.debug("consumeNext: " + consumeNext);

      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, res.getStatus());
      res.releaseConnection();
      session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      log.debug("consumeNext: " + consumeNext);
      res = consumeNext.request().post(String.class);
      res.releaseConnection();

      res = session.request().delete();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());
   }

   @Test
   public void testDupWithId() throws Exception {
      String testName = "testDupWithId";
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(false);
      deployment.setDurableSend(false);
      deployment.setName(testName);
      manager.getQueueManager().deploy(deployment);

      ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create-with-id");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("poller: " + consumeNext);

      ClientResponse<?> res = sender.request().pathParameter("id", "1").body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(1)).pathParameter("id", "1").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create-next");
      res = sender.request().body("text/plain", Integer.toString(2)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      res.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      log.debug("consumeNext: " + consumeNext);

      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("2", res.getEntity(String.class));
      res.releaseConnection();
      session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      log.debug("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      log.debug("consumeNext: " + consumeNext);
      res = consumeNext.request().post(String.class);
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());

      res = session.request().delete();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());
   }
}