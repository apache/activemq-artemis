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
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.rest.HttpHeaderProperty;
import org.apache.activemq.artemis.rest.queue.QueueDeployment;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.activemq.artemis.rest.util.HttpMessageHelper.POSTED_AS_HTTP_MESSAGE;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.jboss.resteasy.test.TestPortProvider.generateURL;

public class MessagePropertiesTest extends MessageTestBase {
   private static final Logger log = Logger.getLogger(MessagePropertiesTest.class);

   public static class Listener implements MessageHandler {

      public static CountDownLatch latch = new CountDownLatch(1);
      public static Set< SimpleString > propertyNames;


      @Override
      public void onMessage(ClientMessage clientMessage) {
         try {
            propertyNames = clientMessage.getPropertyNames();
         } catch (Exception e) {
            e.printStackTrace();
         }
         latch.countDown();
      }
   }

   @Test
   public void testJmsProperties() throws Exception {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      final String queueName = "testQueue";
      deployment.setName(queueName);
      manager.getQueueManager().deploy(deployment);
      ClientSession session = manager.getQueueManager().getSessionFactory().createSession();
      try {
         session.createConsumer(queueName).setMessageHandler(new Listener());
         session.start();

         ClientRequest request = new ClientRequest(generateURL(Util.getUrlPath(queueName)));

         ClientResponse<?> response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link sender = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
         log.debug("create: " + sender);

         {
            response = sender.request().body("text/plain", "val")
                .header( "dummyHeader", "DummyValue" )
                .header( HttpHeaderProperty.MESSAGE_PROPERTY_DISCRIMINATOR + "property1", "val" )
                .post();
            response.releaseConnection();
            Assert.assertEquals(201, response.getStatus());

            Listener.latch.await( 2, TimeUnit.SECONDS);
            Assert.assertEquals(4, Listener.propertyNames.size());
            Assert.assertThat( Listener.propertyNames, hasItem( new SimpleString( "http_content$type" ) ) );
            Assert.assertThat( Listener.propertyNames, hasItem( new SimpleString( "http_content$length" ) ) );
            Assert.assertThat( Listener.propertyNames, hasItem( new SimpleString( POSTED_AS_HTTP_MESSAGE ) ) );
            Assert.assertThat( Listener.propertyNames, hasItem( new SimpleString( "property1" ) ) );
            Assert.assertThat( Listener.propertyNames, not(hasItem( new SimpleString( "dummyHeader" )) ) );
         }
      } finally {
         session.close();
      }
   }

}
