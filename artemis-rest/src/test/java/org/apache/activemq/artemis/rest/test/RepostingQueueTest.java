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
public class RepostingQueueTest extends MessageTestBase {

   @BeforeClass
   public static void setup() throws Exception {
      QueueDeployment deployment1 = new QueueDeployment("testQueue", true);
      manager.getQueueManager().deploy(deployment1);
   }

   @Test
   public void testPostOnSameConsumeNext() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("resource consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("session 1st consumeNext: " + consumeNext);

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

      session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("session 2nd consumeNext: " + consumeNext);

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
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("resource consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      Link firstConsumeNext = consumeNext;
      System.out.println("session 1st consumeNext: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("session 2nd consumeNext: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(3)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("3", response.getEntity(String.class));
      response.releaseConnection();

      response = firstConsumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(412, response.getStatus());
      System.out.println(response.getEntity(String.class));
      response.releaseConnection();

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testPostOnSameConsumeNextWithTimeout() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("resource consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("session 1st consumeNext: " + consumeNext);

      // test timeout here
      response = consumeNext.request().post(String.class);
      response.releaseConnection();
      Assert.assertEquals(503, response.getStatus());
      session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("session 2nd consumeNext: " + consumeNext);

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
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "3").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      Link ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("session 1st acknowledge-next: " + consumeNext);

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
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRepostSuccessfulUnacknowledge() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      Link ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "false").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "false").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "false").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("session 1st acknowledge-next: " + consumeNext);

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
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRepostAckAfterUnacknowledge() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      Link ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "false").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "true").post();
      Assert.assertEquals(412, response.getStatus());
      System.out.println(response.getEntity(String.class));
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("session 1st acknowledge-next: " + consumeNext);

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
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testRepostUnAckAfterAcknowledge() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "3").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      Link ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      response = ack.request().formParameter("acknowledge", "false").post();
      Assert.assertEquals(412, response.getStatus());
      System.out.println(response.getEntity(String.class));
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("session 1st acknowledge-next: " + consumeNext);

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
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testPostOnOldAcknowledgeNextAndAck() throws Exception {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("resource acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "3").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consumer");
      System.out.println("session: " + session);
      Link ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Link oldAck = ack;
      System.out.println("ack: " + ack);
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("session 1st acknowledge-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      System.out.println("ack: " + ack);

      response = oldAck.request().formParameter("acknowledge", "true").post();
      Assert.assertEquals(412, response.getStatus());
      System.out.println(response.getEntity(String.class));
      response.releaseConnection();

      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");

      response = consumeNext.request().post(String.class);
      response.releaseConnection();
      Assert.assertEquals(503, response.getStatus());

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

}
