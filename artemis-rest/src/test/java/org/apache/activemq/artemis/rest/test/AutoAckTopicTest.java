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
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.Assert;
import org.junit.Test;

public class AutoAckTopicTest extends MessageTestBase {

   @Test
   public void testSuccessFirst() throws Exception {
      String testName = "AutoAckTopicTest.testSuccessFirst";
      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(testName);
      manager.getTopicManager().deploy(deployment);

      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/topics/" + testName));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
      Link subscriptions = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "pull-subscriptions");

      ClientResponse<?> res = subscriptions.request().post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      Link sub1 = res.getLocationLink();
      Assert.assertNotNull(sub1);
      Link consumeNext1 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");
      Assert.assertNotNull(consumeNext1);
      System.out.println("consumeNext1: " + consumeNext1);

      res = subscriptions.request().post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      Link sub2 = res.getLocationLink();
      Assert.assertNotNull(sub2);
      Link consumeNext2 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");
      Assert.assertNotNull(consumeNext2);
      System.out.println("consumeNext2: " + consumeNext2);

      res = sender.request().body("text/plain", "1").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = sender.request().body("text/plain", "2").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext1.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      res.releaseConnection();
      consumeNext1 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");

      res = consumeNext1.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("2", res.getEntity(String.class));
      res.releaseConnection();
      consumeNext1 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");

      res = consumeNext2.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      res.releaseConnection();
      consumeNext2 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");

      res = consumeNext2.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("2", res.getEntity(String.class));
      res.releaseConnection();
      consumeNext2 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");
      Assert.assertEquals(204, sub1.request().delete().getStatus());
      Assert.assertEquals(204, sub2.request().delete().getStatus());
   }

   @Test
   public void testNewSubNotBlockedByTimeoutTask() throws Exception {
      // Default config is 1s interval, 300s timeout.

      // Create a topic
      String testName = "AutoAckTopicTest.testNewSubNotBlockedByTimeoutTask";
      TopicDeployment deployment = new TopicDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(testName);
      manager.getTopicManager().deploy(deployment);

      // Create a consumer
      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/topics/" + testName));
      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
      Link subscriptions = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "pull-subscriptions");

      // Create the pull-subscription itself.
      ClientResponse<?> res = subscriptions.request().post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      Link sub1 = res.getLocationLink();
      Assert.assertNotNull(sub1);
      Link consumeNext1 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");
      Assert.assertNotNull(consumeNext1);

      // Pull on the topic for 8s (long enoguh to guarantee the rest of the test
      // will pass/fail due to the timeouttask + test operations)
      AcceptWaitListener awlistener = new AcceptWaitListener(consumeNext1.getHref());
      Thread t = new Thread(awlistener);
      t.start();
      // Wait 2 seconds to ensure a new TimeoutTask is running concurrently.
      Thread.sleep(2000);
      // Attempt to create a new pull-subscription. Validate that it takes no longer than 2 seconds
      // (it should take like 20ms, but give it a relatively huge amount of leeway)
      NewPullSubscriber nps = new NewPullSubscriber(subscriptions.getHref());
      Thread npsThread = new Thread(nps);
      npsThread.start();
      Thread.sleep(2000);
      Assert.assertTrue("NewPullSubscriber did not finish in 2 seconds!", nps.isFinished());
      Assert.assertFalse("AcceptWaitListener failed to open connection!", awlistener.isFailed());
      Assert.assertFalse("NewPullSubscriber failed to open new subscription!", nps.isFailed());
      npsThread.interrupt();
      t.interrupt();
   }

   private class NewPullSubscriber implements Runnable {

      private final String url;
      private boolean isFinished = false;
      private boolean failed = false;

      private NewPullSubscriber(String url) {
         this.url = url;
      }

      public boolean isFinished() {
         return isFinished;
      }

      public boolean isFailed() {
         return failed;
      }

      @Override
      public void run() {
         try {
            isFinished = false;
            ClientRequest request = new ClientRequest(url);
            ClientResponse<?> response = request.post();
            response.releaseConnection();
            System.out.println("NPS response: " + response.getStatus());
            Assert.assertEquals(201, response.getStatus());
            isFinished = true;
         } catch (Exception e) {
            System.out.println("Exception " + e);
            failed = true;
         }
      }
   }

   private class AcceptWaitListener implements Runnable {

      private final int acceptWaitTime = 8;
      private String url;
      private boolean isFinished = false;
      private boolean failed = false;

      private AcceptWaitListener(String url) {
         this.url = url;
      }

      public boolean isFinished() {
         return this.isFinished;
      }

      public boolean isFailed() {
         return this.failed;
      }

      @Override
      public void run() {
         try {
            ClientRequest req = new ClientRequest(url);
            req.header("Accept-Wait", acceptWaitTime);
            ClientResponse<?> response = req.post();
            response.releaseConnection();
            isFinished = true;
         } catch (Exception e) {
            failed = true;
         }
      }
   }
}
