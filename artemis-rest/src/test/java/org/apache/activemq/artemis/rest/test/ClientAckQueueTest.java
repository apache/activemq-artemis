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
import org.apache.activemq.artemis.rest.util.CustomHeaderLinkStrategy;
import org.apache.activemq.artemis.rest.util.LinkHeaderLinkStrategy;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class ClientAckQueueTest extends MessageTestBase {

   @BeforeClass
   public static void setup() throws Exception {
      QueueDeployment deployment1 = new QueueDeployment("testQueue", true);
      manager.getQueueManager().deploy(deployment1);
   }

   @Test
   public void testAckTimeoutX2() throws Exception {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testAck");
      manager.getQueueManager().deploy(deployment);

      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());
      testAckTimeout();
      manager.getQueueManager().setLinkStrategy(new CustomHeaderLinkStrategy());
      testAckTimeout();
   }

   public void testAckTimeout() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testAck"));

      ClientResponse<?> response = Util.head(request);

      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("poller: " + consumeNext);

      {
         ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
         res.releaseConnection();
         Assert.assertEquals(201, res.getStatus());

         res = consumeNext.request().post(String.class);
         res.releaseConnection();
         Assert.assertEquals(200, res.getStatus());
         Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
         System.out.println("ack: " + ack);
         Assert.assertNotNull(ack);
         Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
         System.out.println("session: " + session);
         consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");
         System.out.println("consumeNext: " + consumeNext);

         // test timeout
         Thread.sleep(2000);

         ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
         ackRes.releaseConnection();
         Assert.assertEquals(412, ackRes.getStatus());
         System.out.println("**** Successfully failed ack");
         consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "acknowledge-next");
         System.out.println("consumeNext: " + consumeNext);
      }
      {
         ClientResponse<?> res = consumeNext.request().header(Constants.WAIT_HEADER, "2").post(String.class);
         res.releaseConnection();
         Assert.assertEquals(200, res.getStatus());
         Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
         System.out.println("ack: " + ack);
         Assert.assertNotNull(ack);
         Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
         System.out.println("session: " + session);
         consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");
         System.out.println("consumeNext: " + consumeNext);

         ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
         if (ackRes.getStatus() != 204) {
            System.out.println(ackRes.getEntity(String.class));
         }
         ackRes.releaseConnection();
         Assert.assertEquals(204, ackRes.getStatus());

         Assert.assertEquals(204, session.request().delete().getStatus());
      }
   }

   @Test
   public void testSuccessFirstX2() throws Exception {
      String testName = "testSuccessFirstX2";

      QueueDeployment queueDeployment = new QueueDeployment(testName, true);
      manager.getQueueManager().deploy(queueDeployment);

      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());
      testSuccessFirst(1, testName);
      manager.getQueueManager().setLinkStrategy(new CustomHeaderLinkStrategy());
      testSuccessFirst(3, testName);
   }

   public void testSuccessFirst(int start, String queueName) throws Exception {
      ClientRequest request = new ClientRequest(generateURL(Util.getUrlPath(queueName)));

      ClientResponse<?> response = Util.head(request);
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull-consumers: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("acknowledge-next: " + consumeNext);

      String data = Integer.toString(start);
      System.out.println("Sending: " + data);
      ClientResponse<?> res = sender.request().body("text/plain", data).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      System.out.println("call acknowledge-next");
      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals(Integer.toString(start++), res.getEntity());
      res.releaseConnection();
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");
      System.out.println("consumeNext: " + consumeNext);
      ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "acknowledge-next");

      System.out.println("sending next...");
      String data2 = Integer.toString(start);
      System.out.println("Sending: " + data2);
      res = sender.request().body("text/plain", data2).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      System.out.println(consumeNext);
      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals(Integer.toString(start++), res.getEntity());
      res.releaseConnection();
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "acknowledge-next");
      System.out.println("consumeNext: " + consumeNext);
      res = consumeNext.request().post(String.class);
      res.releaseConnection();

      System.out.println(res.getStatus());

      Assert.assertEquals(204, session.request().delete().getStatus());
   }

   @Test
   public void testPullX2() throws Exception {
      String testName = "testPullX2";

      QueueDeployment queueDeployment = new QueueDeployment(testName, true);
      manager.getQueueManager().deploy(queueDeployment);

      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());
      testPull(1, testName);
      manager.getQueueManager().setLinkStrategy(new CustomHeaderLinkStrategy());
      testPull(4, testName);
   }

   public void testPull(int start, String queueName) throws Exception {
      ClientRequest request = new ClientRequest(generateURL(Util.getUrlPath(queueName)));

      ClientResponse<?> response = Util.head(request);
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("poller: " + consumeNext);

      ClientResponse<String> res = consumeNext.request().post(String.class);
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");
      System.out.println(consumeNext);
      res = sender.request().body("text/plain", Integer.toString(start)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals(Integer.toString(start++), res.getEntity());
      res.releaseConnection();
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      res = consumeNext.request().post();
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(start)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(start + 1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals(Integer.toString(start++), res.getEntity());
      res.releaseConnection();
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals(Integer.toString(start++), res.getEntity());
      res.releaseConnection();
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "consumer");

      res = consumeNext.request().post();
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());
      System.out.println(session);
      res = session.request().delete();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());
   }

   @Test
   public void testReconnectX2() throws Exception {
      String testName = "testReconnectX2";

      QueueDeployment queueDeployment = new QueueDeployment(testName, true);
      manager.getQueueManager().deploy(queueDeployment);

      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());
      testReconnect(testName);
      manager.getQueueManager().setLinkStrategy(new CustomHeaderLinkStrategy());
      testReconnect(testName);
   }

   public void testReconnect(String queueName) throws Exception {
      ClientRequest request = new ClientRequest(generateURL(Util.getUrlPath(queueName)));

      ClientResponse<?> response = Util.head(request);
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("poller: " + consumeNext);

      ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().post(String.class);
      res.releaseConnection();
      Assert.assertEquals(200, res.getStatus());
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");
      System.out.println("consumeNext: " + consumeNext);
      ClientResponse ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "acknowledge-next");
      System.out.println("before close session consumeNext: " + consumeNext);

      // test reconnect with a disconnected acknowledge-next
      Assert.assertEquals(204, session.request().delete().getStatus());

      res = sender.request().body("text/plain", Integer.toString(2)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      res.releaseConnection();
      Assert.assertEquals(200, res.getStatus());
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "consumer");
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "acknowledge-next");
      System.out.println("session: " + session);

      // test reconnect with disconnected acknowledge

      res = sender.request().body("text/plain", Integer.toString(3)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      res.releaseConnection();
      Assert.assertEquals(200, res.getStatus());
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);

      Assert.assertEquals(204, session.request().delete().getStatus());

      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(412, ackRes.getStatus());
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "acknowledge-next");
      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      res.releaseConnection();
      Assert.assertEquals(200, res.getStatus());
      ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      System.out.println("ack: " + ack);
      Assert.assertNotNull(ack);
      ackRes = ack.request().formParameter("acknowledge", "true").post();
      ackRes.releaseConnection();
      Assert.assertEquals(204, ackRes.getStatus());
      session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), ackRes, "consumer");

      Assert.assertEquals(204, session.request().delete().getStatus());
   }
}
