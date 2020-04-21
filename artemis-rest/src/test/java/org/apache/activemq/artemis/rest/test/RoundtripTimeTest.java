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
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class RoundtripTimeTest extends MessageTestBase {
   private static final Logger log = Logger.getLogger(RoundtripTimeTest.class);

   @Test
   public void testSuccessFirst() throws Exception {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testQueue");
      manager.getQueueManager().deploy(deployment);

      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      log.debug("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      log.debug("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      log.debug("consume-next: " + consumeNext);

      long start = System.currentTimeMillis();
      int num = 100;
      for (int i = 0; i < num; i++) {
         response = sender.request().body("text/plain", Integer.toString(i + 1)).post();
         response.releaseConnection();
      }
      long end = System.currentTimeMillis() - start;
      log.debug(num + " iterations took " + end + "ms");

      for (int i = 0; i < num; i++) {
         response = consumeNext.request().post(String.class);
         consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
         Assert.assertEquals(200, response.getStatus());
         Assert.assertEquals(Integer.toString(i + 1), response.getEntity(String.class));
         response.releaseConnection();
      }
   }

}