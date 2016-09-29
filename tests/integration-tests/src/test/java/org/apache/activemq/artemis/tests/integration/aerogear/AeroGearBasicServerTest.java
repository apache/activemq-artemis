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
package org.apache.activemq.artemis.tests.integration.aerogear;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.integration.aerogear.AeroGearConnectorServiceFactory;
import org.apache.activemq.artemis.integration.aerogear.AeroGearConstants;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;

public class AeroGearBasicServerTest extends ActiveMQTestBase {

   private ActiveMQServer server;
   private ServerLocator locator;
   private Server jetty;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      /*
      * there will be a thread kept alive by the http connection, we could disable the thread check but this means that the tests
      * interfere with one another, we just have to wait for it to be killed
      * */
      jetty = new Server();
      SelectChannelConnector connector0 = new SelectChannelConnector();
      connector0.setPort(8080);
      connector0.setMaxIdleTime(30000);
      connector0.setHost("localhost");
      jetty.addConnector(connector0);
      jetty.start();
      HashMap<String, Object> params = new HashMap<>();
      params.put(AeroGearConstants.QUEUE_NAME, "testQueue");
      params.put(AeroGearConstants.ENDPOINT_NAME, "http://localhost:8080");
      params.put(AeroGearConstants.APPLICATION_ID_NAME, "9d646a12-e601-4452-9e05-efb0fccdfd08");
      params.put(AeroGearConstants.APPLICATION_MASTER_SECRET_NAME, "ed75f17e-cf3c-4c9b-a503-865d91d60d40");
      params.put(AeroGearConstants.RETRY_ATTEMPTS_NAME, 2);
      params.put(AeroGearConstants.RETRY_INTERVAL_NAME, 1);
      params.put(AeroGearConstants.BADGE_NAME, "99");
      params.put(AeroGearConstants.ALIASES_NAME, "me,him,them");
      params.put(AeroGearConstants.DEVICE_TYPE_NAME, "android,ipad");
      params.put(AeroGearConstants.SOUND_NAME, "sound1");
      params.put(AeroGearConstants.VARIANTS_NAME, "variant1,variant2");

      Configuration configuration = createDefaultInVMConfig().addConnectorServiceConfiguration(new ConnectorServiceConfiguration().setFactoryClassName(AeroGearConnectorServiceFactory.class.getName()).setParams(params).setName("TestAeroGearService")).addQueueConfiguration(new CoreQueueConfiguration().setAddress("testQueue").setName("testQueue"));

      server = addServer(createServer(configuration));
      server.start();

   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (jetty != null) {
         jetty.stop();
      }
      super.tearDown();
   }

   @Test
   public void aerogearSimpleReceiveTest() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);
      AeroGearHandler aeroGearHandler = new AeroGearHandler(latch);
      jetty.addHandler(aeroGearHandler);
      locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);
      ClientProducer producer = session.createProducer("testQueue");
      ClientMessage m = session.createMessage(true);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALERT.toString(), "hello from ActiveMQ!");
      m.putStringProperty("AEROGEAR_PROP1", "prop1");
      m.putBooleanProperty("AEROGEAR_PROP2", true);

      producer.send(m);

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertNotNull(aeroGearHandler.jsonObject);
      JsonObject body = aeroGearHandler.jsonObject.getJsonObject("message");
      assertNotNull(body);
      String prop1 = body.getString("AEROGEAR_PROP1");
      assertNotNull(prop1);
      assertEquals(prop1, "prop1");
      prop1 = body.getString("AEROGEAR_PROP2");
      assertNotNull(prop1);
      assertEquals(prop1, "true");
      String alert = body.getString("alert");
      assertNotNull(alert);
      assertEquals(alert, "hello from ActiveMQ!");
      String sound = body.getString("sound");
      assertNotNull(sound);
      assertEquals(sound, "sound1");
      int badge = body.getInt("badge");
      assertNotNull(badge);
      assertEquals(badge, 99);
      JsonArray jsonArray = aeroGearHandler.jsonObject.getJsonArray("variants");
      assertNotNull(jsonArray);
      assertEquals(jsonArray.getString(0), "variant1");
      assertEquals(jsonArray.getString(1), "variant2");
      jsonArray = aeroGearHandler.jsonObject.getJsonArray("alias");
      assertNotNull(jsonArray);
      assertEquals(jsonArray.getString(0), "me");
      assertEquals(jsonArray.getString(1), "him");
      assertEquals(jsonArray.getString(2), "them");
      jsonArray = aeroGearHandler.jsonObject.getJsonArray("deviceType");
      assertNotNull(jsonArray);
      assertEquals(jsonArray.getString(0), "android");
      assertEquals(jsonArray.getString(1), "ipad");
      int ttl = aeroGearHandler.jsonObject.getInt("ttl");
      assertEquals(ttl, 3600);
      latch = new CountDownLatch(1);
      aeroGearHandler.resetLatch(latch);

      //now override the properties
      m = session.createMessage(true);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALERT.toString(), "another hello from ActiveMQ!");
      m.putStringProperty(AeroGearConstants.AEROGEAR_BADGE.toString(), "111");
      m.putStringProperty(AeroGearConstants.AEROGEAR_SOUND.toString(), "s1");
      m.putIntProperty(AeroGearConstants.AEROGEAR_TTL.toString(), 10000);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALIASES.toString(), "alias1,alias2");
      m.putStringProperty(AeroGearConstants.AEROGEAR_DEVICE_TYPES.toString(), "dev1,dev2");
      m.putStringProperty(AeroGearConstants.AEROGEAR_VARIANTS.toString(), "v1,v2");

      producer.send(m);
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertNotNull(aeroGearHandler.jsonObject);
      body = aeroGearHandler.jsonObject.getJsonObject("message");
      assertNotNull(body);
      alert = body.getString("alert");
      assertNotNull(alert);
      assertEquals(alert, "another hello from ActiveMQ!");
      sound = body.getString("sound");
      assertNotNull(sound);
      assertEquals(sound, "s1");
      badge = body.getInt("badge");
      assertEquals(badge, 111);
      jsonArray = aeroGearHandler.jsonObject.getJsonArray("variants");
      assertNotNull(jsonArray);
      assertEquals(jsonArray.getString(0), "v1");
      assertEquals(jsonArray.getString(1), "v2");
      jsonArray = aeroGearHandler.jsonObject.getJsonArray("alias");
      assertNotNull(jsonArray);
      assertEquals(jsonArray.getString(0), "alias1");
      assertEquals(jsonArray.getString(1), "alias2");
      jsonArray = aeroGearHandler.jsonObject.getJsonArray("deviceType");
      assertNotNull(jsonArray);
      assertEquals(jsonArray.getString(0), "dev1");
      assertEquals(jsonArray.getString(1), "dev2");
      ttl = aeroGearHandler.jsonObject.getInt("ttl");
      assertEquals(ttl, 10000);
      session.start();
      ClientMessage message = session.createConsumer("testQueue").receiveImmediate();
      assertNull(message);
   }

   class AeroGearHandler extends AbstractHandler {

      JsonObject jsonObject;
      private CountDownLatch latch;

      AeroGearHandler(CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public void handle(String target,
                         HttpServletRequest httpServletRequest,
                         HttpServletResponse httpServletResponse,
                         int i) throws IOException, ServletException {
         Request request = (Request) httpServletRequest;
         httpServletResponse.setContentType("text/html");
         httpServletResponse.setStatus(HttpServletResponse.SC_OK);
         request.setHandled(true);
         byte[] bytes = new byte[httpServletRequest.getContentLength()];
         httpServletRequest.getInputStream().read(bytes);
         String json = new String(bytes);
         jsonObject = JsonUtil.readJsonObject(json);
         latch.countDown();
      }

      public void resetLatch(CountDownLatch latch) {
         this.latch = latch;
      }
   }

   @Test
   public void aerogearReconnectTest() throws Exception {
      jetty.stop();
      final CountDownLatch reconnectLatch = new CountDownLatch(1);
      jetty.addHandler(new AbstractHandler() {
         @Override
         public void handle(String target,
                            HttpServletRequest httpServletRequest,
                            HttpServletResponse httpServletResponse,
                            int i) throws IOException, ServletException {
            Request request = (Request) httpServletRequest;
            httpServletResponse.setContentType("text/html");
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            request.setHandled(true);
            reconnectLatch.countDown();
         }

      });
      locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);
      ClientProducer producer = session.createProducer("testQueue");
      final CountDownLatch latch = new CountDownLatch(2);
      ClientMessage m = session.createMessage(true);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALERT.toString(), "hello from ActiveMQ!");

      producer.send(m, new SendAcknowledgementHandler() {
         @Override
         public void sendAcknowledged(Message message) {
            latch.countDown();
         }
      });
      m = session.createMessage(true);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALERT.toString(), "another hello from ActiveMQ!");

      producer.send(m, new SendAcknowledgementHandler() {
         @Override
         public void sendAcknowledged(Message message) {
            latch.countDown();
         }
      });
      latch.await(5, TimeUnit.SECONDS);
      Thread.sleep(1000);
      jetty.start();
      reconnectLatch.await(5, TimeUnit.SECONDS);
      session.start();
      ClientMessage message = session.createConsumer("testQueue").receiveImmediate();
      assertNull(message);
   }

   @Test
   public void aerogear401() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);
      jetty.addHandler(new AbstractHandler() {
         @Override
         public void handle(String target,
                            HttpServletRequest httpServletRequest,
                            HttpServletResponse httpServletResponse,
                            int i) throws IOException, ServletException {
            Request request = (Request) httpServletRequest;
            httpServletResponse.setContentType("text/html");
            httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            request.setHandled(true);
            latch.countDown();
         }

      });
      locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);
      ClientProducer producer = session.createProducer("testQueue");
      ClientMessage m = session.createMessage(true);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALERT.toString(), "hello from ActiveMQ!");

      producer.send(m);
      m = session.createMessage(true);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALERT.toString(), "another hello from ActiveMQ!");

      producer.send(m);
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      session.start();
      ClientConsumer consumer = session.createConsumer("testQueue");
      ClientMessage message = consumer.receive(5000);
      assertNotNull(message);
      message = consumer.receive(5000);
      assertNotNull(message);
   }

   @Test
   public void aerogear404() throws Exception {
      jetty.addHandler(new AbstractHandler() {
         @Override
         public void handle(String target,
                            HttpServletRequest httpServletRequest,
                            HttpServletResponse httpServletResponse,
                            int i) throws IOException, ServletException {
            Request request = (Request) httpServletRequest;
            httpServletResponse.setContentType("text/html");
            httpServletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
            request.setHandled(true);
         }

      });
      locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);
      ClientProducer producer = session.createProducer("testQueue");
      ClientMessage m = session.createMessage(true);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALERT.toString(), "hello from ActiveMQ!");

      producer.send(m);
      m = session.createMessage(true);
      m.putStringProperty(AeroGearConstants.AEROGEAR_ALERT.toString(), "another hello from ActiveMQ!");

      producer.send(m);
      session.start();
      ClientConsumer consumer = session.createConsumer("testQueue");
      ClientMessage message = consumer.receive(5000);
      assertNotNull(message);
      message = consumer.receive(5000);
      assertNotNull(message);
   }
}
