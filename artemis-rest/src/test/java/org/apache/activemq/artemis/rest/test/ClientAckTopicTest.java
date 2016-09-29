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
import org.apache.activemq.artemis.rest.util.CustomHeaderLinkStrategy;
import org.apache.activemq.artemis.rest.util.LinkHeaderLinkStrategy;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClientAckTopicTest extends MessageTestBase {

   @BeforeClass
   public static void setup() throws Exception {
      TopicDeployment deployment1 = new TopicDeployment("testQueue", true);
      manager.getTopicManager().deploy(deployment1);
   }

   @Test
   public void testAckTimeoutX2() throws Exception {
      TopicDeployment deployment = new TopicDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testAck");
      manager.getTopicManager().deploy(deployment);

      manager.getTopicManager().setLinkStrategy(new LinkHeaderLinkStrategy());
      testAckTimeout();
      manager.getTopicManager().setLinkStrategy(new CustomHeaderLinkStrategy());
      testAckTimeout();
   }

   public void testAckTimeout() throws Exception {

      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/topics/testAck"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link subscriptions = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "pull-subscriptions");
      response = subscriptions.request().formParameter("autoAck", "false").formParameter("durable", "true").post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      Link sub1 = response.getLocationLink();
      Assert.assertNotNull(sub1);

      Link consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("poller: " + consumeNext);

      {
         ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
         res.releaseConnection();
         Assert.assertEquals(201, res.getStatus());

         res = consumeNext.request().post(String.class);
         res.releaseConnection();
         Assert.assertEquals(200, res.getStatus());
         Link ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
         System.out.println("ack: " + ack);
         Assert.assertNotNull(ack);
         Link session = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consumer");
         System.out.println("session: " + session);
         consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledge-next");
         System.out.println("consumeNext: " + consumeNext);

         // test timeout
         Thread.sleep(2000);

         ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
         if (ackRes.getStatus() == 500) {
            System.out.println("Failure: " + ackRes.getEntity(String.class));
         }
         ackRes.releaseConnection();
         Assert.assertEquals(412, ackRes.getStatus());
         System.out.println("**** Successfully failed ack");
         consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), ackRes, "acknowledge-next");
         System.out.println("consumeNext: " + consumeNext);
      }
      {
         ClientResponse<?> res = consumeNext.request().header(Constants.WAIT_HEADER, "2").post(String.class);
         res.releaseConnection();
         Assert.assertEquals(200, res.getStatus());
         Link ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
         System.out.println("ack: " + ack);
         Assert.assertNotNull(ack);
         consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledge-next");
         System.out.println("consumeNext: " + consumeNext);

         ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
         if (ackRes.getStatus() != 204) {
            System.out.println(ackRes.getEntity(String.class));
         }
         ackRes.releaseConnection();
         Assert.assertEquals(204, ackRes.getStatus());
      }
      Assert.assertEquals(204, sub1.request().delete().getStatus());
   }

   @Test
   public void testSuccessFirst() throws Exception {
      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/topics/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);

      Link subscriptions = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "pull-subscriptions");
      response = subscriptions.request().formParameter("autoAck", "false").formParameter("durable", "true").post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      Link sub1 = response.getLocationLink();
      Assert.assertNotNull(sub1);
      Link consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("poller: " + consumeNext);

      ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      System.out.println("call ack next");
      res = consumeNext.request().post(String.class);
      res.releaseConnection();
      Assert.assertEquals(200, res.getStatus());
      Link ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      Link session = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledge-next");
      System.out.println("consumeNext: " + consumeNext);
      ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), ackRes, "acknowledge-next");

      System.out.println("sending next...");
      res = sender.request().body("text/plain", Integer.toString(2)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      System.out.println(consumeNext);
      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      res.releaseConnection();
      Assert.assertEquals(200, res.getStatus());
      ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledge-next");
      System.out.println("consumeNext: " + consumeNext);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());

      Assert.assertEquals(204, sub1.request().delete().getStatus());
   }

   @Test
   public void testSuccessFirstNonDurable() throws Exception {
      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/topics/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);

      Link subscriptions = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "pull-subscriptions");
      response = subscriptions.request().formParameter("autoAck", "false").formParameter("durable", "false").post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      Link sub1 = response.getLocationLink();
      Assert.assertNotNull(sub1);
      Link consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("poller: " + consumeNext);

      ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      System.out.println("call ack next");
      res = consumeNext.request().post(String.class);
      res.releaseConnection();
      Assert.assertEquals(200, res.getStatus());
      Link ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      Link session = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledge-next");
      System.out.println("consumeNext: " + consumeNext);
      ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), ackRes, "acknowledge-next");

      System.out.println("sending next...");
      res = sender.request().body("text/plain", Integer.toString(2)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      System.out.println(consumeNext);
      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      res.releaseConnection();
      Assert.assertEquals(200, res.getStatus());
      ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledge-next");
      System.out.println("consumeNext: " + consumeNext);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());

      Assert.assertEquals(204, sub1.request().delete().getStatus());
   }

   @Test
   public void testPull() throws Exception {
      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/topics/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link subscriptions = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "pull-subscriptions");
      response = subscriptions.request().formParameter("autoAck", "false").formParameter("durable", "true").post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      Link sub1 = response.getLocationLink();
      Assert.assertNotNull(sub1);
      Link consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("poller: " + consumeNext);

      ClientResponse<String> res = consumeNext.request().post(String.class);
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());
      consumeNext = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledge-next");
      System.out.println(consumeNext);
      res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals(Integer.toString(1), res.getEntity());
      res.releaseConnection();
      Link ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      res = consumeNext.request().post();
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(2)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(3)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals(Integer.toString(2), res.getEntity());
      res.releaseConnection();
      ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals(Integer.toString(3), res.getEntity());
      res.releaseConnection();
      ack = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());

      res = consumeNext.request().post();
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());
      System.out.println(sub1);
      res = sub1.request().delete();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());
   }
}
