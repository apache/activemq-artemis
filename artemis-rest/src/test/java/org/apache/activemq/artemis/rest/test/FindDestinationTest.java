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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.Assert;
import org.junit.Test;

public class FindDestinationTest extends MessageTestBase {

   @Test
   public void testFindQueue() throws Exception {
      String testName = "testFindQueue";
      server.getActiveMQServer().addAddressInfo(new AddressInfo(SimpleString.toSimpleString(testName), RoutingType.MULTICAST));
      server.getActiveMQServer().createQueue(new SimpleString(testName), RoutingType.MULTICAST, new SimpleString(testName), null, false, false);

      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/queues/" + testName));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("poller: " + consumeNext);

      ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      res.releaseConnection();
      Link session = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      Assert.assertEquals(204, session.request().delete().getStatus());
   }

   @Test
   public void testFindTopic() throws Exception {
      server.getActiveMQServer().addAddressInfo(new AddressInfo(SimpleString.toSimpleString("testTopic"), RoutingType.MULTICAST));
      server.getActiveMQServer().createQueue(new SimpleString("testTopic"), RoutingType.MULTICAST, new SimpleString("testTopic"), null, false, false);
      ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "create");
      Link subscriptions = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), response, "pull-subscriptions");

      ClientResponse<?> res = subscriptions.request().post();
      Assert.assertEquals(201, res.getStatus());
      Link sub1 = res.getLocationLink();
      res.releaseConnection();
      Assert.assertNotNull(sub1);
      Link consumeNext1 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");
      Assert.assertNotNull(consumeNext1);
      System.out.println("consumeNext1: " + consumeNext1);

      res = subscriptions.request().post();
      Assert.assertEquals(201, res.getStatus());
      Link sub2 = res.getLocationLink();
      res.releaseConnection();
      Assert.assertNotNull(sub2);
      Link consumeNext2 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");
      Assert.assertNotNull(consumeNext1);

      res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(2)).post();
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

      res = consumeNext2.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      res.releaseConnection();
      consumeNext2 = getLinkByTitle(manager.getTopicManager().getLinkStrategy(), res, "consume-next");

      res = consumeNext2.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("2", res.getEntity(String.class));
      res.releaseConnection();

      res = sub1.request().delete();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());

      res = sub2.request().delete();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());
   }
}