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
package org.apache.activemq.artemis.tests.integration.vertx;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.integration.vertx.VertxConstants;
import org.apache.activemq.artemis.integration.vertx.VertxIncomingConnectorServiceFactory;
import org.apache.activemq.artemis.integration.vertx.VertxOutgoingConnectorServiceFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.impl.BaseMessage;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;
import org.vertx.java.spi.cluster.impl.hazelcast.HazelcastClusterManagerFactory;

/**
 * This class tests the basics of ActiveMQ
 * vertx integration
 */
public class ActiveMQVertxUnitTest extends ActiveMQTestBase {

   private PlatformManager vertxManager;
   private ActiveMQServer server;

   private String host = "localhost";
   private String port = "0";

   private String incomingQueue1 = "vertxTestIncomingQueue1";
   private String incomingVertxAddress1 = "org.apache.activemq.test.incoming1";

   //outgoing using send
   private String inOutQueue1 = "vertxTestInOutQueue1";
   private String incomingVertxAddress2 = "org.apache.activemq.test.incoming2";
   private String outgoingVertxAddress1 = "org.apache.activemq.test.outgoing1";

   //outgoing using publish
   private String inOutQueue2 = "vertxTestInOutQueue2";
   private String incomingVertxAddress3 = "org.apache.activemq.test.incoming3";
   private String outgoingVertxAddress2 = "org.apache.activemq.test.outgoing2";

   // Vertx is changing the classLoader to null.. this will preserve the original classloader
   private ClassLoader contextClassLoader;

   //subclasses may override this method
   //in order to get a server with different connector services
   @Before
   @Override
   public void setUp() throws Exception {
      contextClassLoader = Thread.currentThread().getContextClassLoader();
      createVertxService();

      super.setUp();
      //all queues
      CoreQueueConfiguration qc1 = createCoreQueueConfiguration(incomingQueue1);
      CoreQueueConfiguration qc2 = createCoreQueueConfiguration(inOutQueue1);
      CoreQueueConfiguration qc3 = createCoreQueueConfiguration(inOutQueue2);

      //incoming
      HashMap<String, Object> config1 = createIncomingConnectionConfig(incomingVertxAddress1, incomingQueue1);
      ConnectorServiceConfiguration inconf1 = createIncomingConnectorServiceConfiguration(config1, "test-vertx-incoming-connector1");

      //outgoing send style
      HashMap<String, Object> config2 = createOutgoingConnectionConfig(inOutQueue1, incomingVertxAddress2);
      ConnectorServiceConfiguration inconf2 = createIncomingConnectorServiceConfiguration(config2, "test-vertx-incoming-connector2");

      HashMap<String, Object> config3 = createOutgoingConnectionConfig(inOutQueue1, outgoingVertxAddress1);
      ConnectorServiceConfiguration outconf1 = createOutgoingConnectorServiceConfiguration(config3, "test-vertx-outgoing-connector1");

      //outgoing publish style
      HashMap<String, Object> config4 = createOutgoingConnectionConfig(inOutQueue2, incomingVertxAddress3);
      ConnectorServiceConfiguration inconf3 = createIncomingConnectorServiceConfiguration(config4, "test-vertx-incoming-connector3");

      HashMap<String, Object> config5 = createOutgoingConnectionConfig(inOutQueue2, outgoingVertxAddress2);
      config5.put(VertxConstants.VERTX_PUBLISH, "true");
      ConnectorServiceConfiguration outconf2 = createOutgoingConnectorServiceConfiguration(config5, "test-vertx-outgoing-connector2");

      Configuration configuration = createDefaultInVMConfig().addQueueConfiguration(qc1).addQueueConfiguration(qc2).addQueueConfiguration(qc3).addConnectorServiceConfiguration(inconf1).addConnectorServiceConfiguration(inconf2).addConnectorServiceConfiguration(outconf1).addConnectorServiceConfiguration(inconf3).addConnectorServiceConfiguration(outconf2);

      server = createServer(false, configuration);
      server.start();
   }

   /**
    * (vertx events) ===> (incomingQueue1) ===> (activemq consumer)
    */
   @Test
   public void testIncomingEvents() throws Exception {
      Vertx vertx = vertxManager.vertx();

      //send a string message
      String greeting = "Hello World!";
      vertx.eventBus().send(incomingVertxAddress1, greeting);

      ClientMessage msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      System.out.println("==== received msg: " + msg);

      int vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_STRING, vertxType);

      String body = msg.getBodyBuffer().readString();
      System.out.println("==== body: " + body);

      assertEquals(greeting, body);

      //send a Buffer message
      final byte[] content = greeting.getBytes(StandardCharsets.UTF_8);
      Buffer buffer = new Buffer(content);
      vertx.eventBus().send(incomingVertxAddress1, buffer);

      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_BUFFER, vertxType);

      ActiveMQBuffer activeMQBuffer = msg.getBodyBuffer();
      int len = activeMQBuffer.readInt();
      System.out.println("==== len is: " + len);
      assertEquals(content.length, len);
      byte[] bytes = new byte[len];
      activeMQBuffer.readBytes(bytes);

      //bytes must match
      for (int i = 0; i < len; i++) {
         assertEquals(content[i], bytes[i]);
      }

      //send a boolean
      vertx.eventBus().send(incomingVertxAddress1, Boolean.TRUE);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_BOOLEAN, vertxType);

      Boolean booleanValue = msg.getBodyBuffer().readBoolean();
      assertEquals(Boolean.TRUE, booleanValue);

      //send a byte array
      vertx.eventBus().send(incomingVertxAddress1, content);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_BYTEARRAY, vertxType);

      len = msg.getBodyBuffer().readInt();
      byte[] recvBytes = new byte[len];
      msg.getBodyBuffer().readBytes(recvBytes);
      //bytes must match
      for (int i = 0; i < len; i++) {
         assertEquals(content[i], recvBytes[i]);
      }

      //send a byte
      Byte aByte = (byte) 15;
      vertx.eventBus().send(incomingVertxAddress1, aByte);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_BYTE, vertxType);

      Byte recvByte = msg.getBodyBuffer().readByte();
      assertEquals(aByte, recvByte);

      //send a Character
      Character aChar = 'a';
      vertx.eventBus().send(incomingVertxAddress1, aChar);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_CHARACTER, vertxType);
      Character recvChar = msg.getBodyBuffer().readChar();
      assertEquals(aChar, recvChar);

      //send a Double
      Double aDouble = 1234.56d;
      vertx.eventBus().send(incomingVertxAddress1, aDouble);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_DOUBLE, vertxType);
      Double recvDouble = msg.getBodyBuffer().readDouble();
      assertEquals(aDouble, recvDouble);

      //send a Float
      Float aFloat = 1234.56f;
      vertx.eventBus().send(incomingVertxAddress1, aFloat);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_FLOAT, vertxType);
      Float recvFloat = msg.getBodyBuffer().readFloat();
      assertEquals(aFloat, recvFloat);

      //send an Integer
      Integer aInt = 1234;
      vertx.eventBus().send(incomingVertxAddress1, aInt);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_INT, vertxType);
      Integer recvInt = msg.getBodyBuffer().readInt();
      assertEquals(aInt, recvInt);

      //send a Long
      Long aLong = 12345678L;
      vertx.eventBus().send(incomingVertxAddress1, aLong);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_LONG, vertxType);
      Long recvLong = msg.getBodyBuffer().readLong();
      assertEquals(aLong, recvLong);

      //send a Short
      Short aShort = (short) 321;
      vertx.eventBus().send(incomingVertxAddress1, aShort);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_SHORT, vertxType);
      Short recvShort = msg.getBodyBuffer().readShort();
      assertEquals(aShort, recvShort);

      //send a JsonObject
      String jsonObjectString = "{\n" +
         "\"Image\": {\n" +
         "\"Width\":  800,\n" +
         "\"Height\": 600,\n" +
         "\"Title\":  \"View from 15th Floor\",\n" +
         "\"Thumbnail\": {\n" +
         "\"Url\":    \"http://www.example.com/image/481989943\",\n" +
         "\"Height\": 125,\n" +
         "\"Width\":  100\n" +
         "},\n" +
         "\"IDs\": [116, 943, 234, 38793]\n" +
         "}\n" +
         "}";
      JsonObject aJsonObj = new JsonObject(jsonObjectString);
      vertx.eventBus().send(incomingVertxAddress1, aJsonObj);
      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_JSON_OBJECT, vertxType);
      String recvJsonString = msg.getBodyBuffer().readString();
      System.out.println("==== received json: " + recvJsonString);
      assertEquals(aJsonObj, new JsonObject(recvJsonString));

      //send a JsonArray
      String jsonArrayString = "[\n" +
         "{\n" +
         "\"precision\": \"zip\",\n" +
         "\"Latitude\":  37.7668,\n" +
         "\"Longitude\": -122.3959,\n" +
         "\"Address\":   \"\",\n" +
         "\"City\":      \"SAN FRANCISCO\",\n" +
         "\"State\":     \"CA\",\n" +
         "\"Zip\":       \"94107\",\n" +
         "\"Country\":   \"US\"\n" +
         "},\n" +
         "{\n" +
         "\"precision\": \"zip\",\n" +
         "\"Latitude\":  37.371991,\n" +
         "\"Longitude\": -122.026020,\n" +
         "\"Address\":   \"\",\n" +
         "\"City\":      \"SUNNYVALE\",\n" +
         "\"State\":     \"CA\",\n" +
         "\"Zip\":       \"94085\",\n" +
         "\"Country\":   \"US\"\n" +
         "}\n" +
         "]";
      JsonArray aJsonArray = new JsonArray(jsonArrayString);
      System.out.println("a json array string: " + aJsonArray);
      vertx.eventBus().send(incomingVertxAddress1, aJsonArray);

      msg = receiveFromQueue(incomingQueue1);
      assertNotNull(msg);
      vertxType = msg.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);
      assertEquals(VertxConstants.TYPE_JSON_ARRAY, vertxType);
      recvJsonString = msg.getBodyBuffer().readString();
      System.out.println("==== received json: " + recvJsonString);
      assertEquals(aJsonArray, new JsonArray(recvJsonString));
   }

   /**
    * vertx events (incomingVertxAddress2)
    * ===> (inOutQueue1)
    * ===> (outgoing handler)
    * ===> send to vertx (outgoingVertxAddress1)
    */
   @Test
   public void testOutgoingEvents() throws Exception {
      Vertx vertx = vertxManager.vertx();

      //regiser a handler to receive outgoing messages
      VertxTestHandler handler = new VertxTestHandler();
      vertx.eventBus().registerHandler(outgoingVertxAddress1, handler);

      //send a string message
      String greeting = "Hello World!";
      vertx.eventBus().send(incomingVertxAddress2, greeting);

      //check message in handler
      handler.checkStringMessageReceived(greeting);

      //send a Buffer message
      final byte[] content = greeting.getBytes(StandardCharsets.UTF_8);
      Buffer buffer = new Buffer(content);
      vertx.eventBus().send(incomingVertxAddress2, buffer);

      handler.checkBufferMessageReceived(buffer);

      //send a boolean
      Boolean boolValue = Boolean.TRUE;
      vertx.eventBus().send(incomingVertxAddress2, boolValue);

      handler.checkBooleanMessageReceived(boolValue);

      byte[] byteArray = greeting.getBytes(StandardCharsets.UTF_8);
      vertx.eventBus().send(incomingVertxAddress2, byteArray);

      handler.checkByteArrayMessageReceived(byteArray);

      //send a byte
      Byte aByte = (byte) 15;
      vertx.eventBus().send(incomingVertxAddress2, aByte);

      handler.checkByteMessageReceived(aByte);

      //send a Character
      Character aChar = 'a';
      vertx.eventBus().send(incomingVertxAddress2, aChar);

      handler.checkCharacterMessageReceived(aChar);

      //send a Double
      Double aDouble = 1234.56d;
      vertx.eventBus().send(incomingVertxAddress2, aDouble);

      handler.checkDoubleMessageReceived(aDouble);

      //send a Float
      Float aFloat = 1234.56f;
      vertx.eventBus().send(incomingVertxAddress2, aFloat);

      handler.checkFloatMessageReceived(aFloat);

      //send an Integer
      Integer aInt = 1234;
      vertx.eventBus().send(incomingVertxAddress2, aInt);

      handler.checkIntegerMessageReceived(aInt);

      //send a Long
      Long aLong = 12345678L;
      vertx.eventBus().send(incomingVertxAddress2, aLong);

      handler.checkLongMessageReceived(aLong);

      //send a Short
      Short aShort = (short) 321;
      vertx.eventBus().send(incomingVertxAddress2, aShort);

      handler.checkShortMessageReceived(aShort);

      //send a JsonObject
      String jsonObjectString = "{\n" +
         "\"Image\": {\n" +
         "\"Width\":  800,\n" +
         "\"Height\": 600,\n" +
         "\"Title\":  \"View from 15th Floor\",\n" +
         "\"Thumbnail\": {\n" +
         "\"Url\":    \"http://www.example.com/image/481989943\",\n" +
         "\"Height\": 125,\n" +
         "\"Width\":  100\n" +
         "},\n" +
         "\"IDs\": [116, 943, 234, 38793]\n" +
         "}\n" +
         "}";
      JsonObject aJsonObj = new JsonObject(jsonObjectString);
      vertx.eventBus().send(incomingVertxAddress2, aJsonObj);

      handler.checkJsonObjectMessageReceived(aJsonObj);

      //send a JsonArray
      String jsonArrayString = "[\n" +
         "{\n" +
         "\"precision\": \"zip\",\n" +
         "\"Latitude\":  37.7668,\n" +
         "\"Longitude\": -122.3959,\n" +
         "\"Address\":   \"\",\n" +
         "\"City\":      \"SAN FRANCISCO\",\n" +
         "\"State\":     \"CA\",\n" +
         "\"Zip\":       \"94107\",\n" +
         "\"Country\":   \"US\"\n" +
         "},\n" +
         "{\n" +
         "\"precision\": \"zip\",\n" +
         "\"Latitude\":  37.371991,\n" +
         "\"Longitude\": -122.026020,\n" +
         "\"Address\":   \"\",\n" +
         "\"City\":      \"SUNNYVALE\",\n" +
         "\"State\":     \"CA\",\n" +
         "\"Zip\":       \"94085\",\n" +
         "\"Country\":   \"US\"\n" +
         "}\n" +
         "]";
      JsonArray aJsonArray = new JsonArray(jsonArrayString);
      vertx.eventBus().send(incomingVertxAddress2, aJsonArray);

      handler.checkJsonArrayMessageReceived(aJsonArray);
   }

   /**
    * vertx events (incomingVertxAddress3)
    * ===> (inOutQueue2)
    * ===> (outgoing handler)
    * ===> public to vertx (outgoingVertxAddress2)
    */
   @Test
   public void testOutgoingEvents2() throws Exception {
      Vertx vertx = vertxManager.vertx();

      //regiser two handlers to receive outgoing messages
      VertxTestHandler handler1 = new VertxTestHandler();
      vertx.eventBus().registerHandler(outgoingVertxAddress2, handler1);
      VertxTestHandler handler2 = new VertxTestHandler();
      vertx.eventBus().registerHandler(outgoingVertxAddress2, handler2);

      //send a string message
      String greeting = "Hello World!";
      vertx.eventBus().send(incomingVertxAddress3, greeting);

      //check message in handler
      handler1.checkStringMessageReceived(greeting);
      handler2.checkStringMessageReceived(greeting);

      //send a Buffer message
      final byte[] content = greeting.getBytes(StandardCharsets.UTF_8);
      Buffer buffer = new Buffer(content);
      vertx.eventBus().send(incomingVertxAddress3, buffer);

      handler1.checkBufferMessageReceived(buffer);
      handler2.checkBufferMessageReceived(buffer);

      //send a boolean
      Boolean boolValue = Boolean.TRUE;
      vertx.eventBus().send(incomingVertxAddress3, boolValue);

      handler1.checkBooleanMessageReceived(boolValue);
      handler2.checkBooleanMessageReceived(boolValue);

      byte[] byteArray = greeting.getBytes(StandardCharsets.UTF_8);
      vertx.eventBus().send(incomingVertxAddress3, byteArray);

      handler1.checkByteArrayMessageReceived(byteArray);
      handler2.checkByteArrayMessageReceived(byteArray);

      //send a byte
      Byte aByte = (byte) 15;
      vertx.eventBus().send(incomingVertxAddress3, aByte);

      handler1.checkByteMessageReceived(aByte);
      handler2.checkByteMessageReceived(aByte);

      //send a Character
      Character aChar = 'a';
      vertx.eventBus().send(incomingVertxAddress3, aChar);

      handler1.checkCharacterMessageReceived(aChar);
      handler2.checkCharacterMessageReceived(aChar);

      //send a Double
      Double aDouble = 1234.56d;
      vertx.eventBus().send(incomingVertxAddress3, aDouble);

      handler1.checkDoubleMessageReceived(aDouble);
      handler2.checkDoubleMessageReceived(aDouble);

      //send a Float
      Float aFloat = 1234.56f;
      vertx.eventBus().send(incomingVertxAddress3, aFloat);

      handler1.checkFloatMessageReceived(aFloat);
      handler2.checkFloatMessageReceived(aFloat);

      //send an Integer
      Integer aInt = 1234;
      vertx.eventBus().send(incomingVertxAddress3, aInt);

      handler1.checkIntegerMessageReceived(aInt);
      handler2.checkIntegerMessageReceived(aInt);

      //send a Long
      Long aLong = 12345678L;
      vertx.eventBus().send(incomingVertxAddress3, aLong);

      handler1.checkLongMessageReceived(aLong);
      handler2.checkLongMessageReceived(aLong);

      //send a Short
      Short aShort = (short) 321;
      vertx.eventBus().send(incomingVertxAddress3, aShort);

      handler1.checkShortMessageReceived(aShort);
      handler2.checkShortMessageReceived(aShort);

      //send a JsonObject
      String jsonObjectString = "{\n" +
         "\"Image\": {\n" +
         "\"Width\":  800,\n" +
         "\"Height\": 600,\n" +
         "\"Title\":  \"View from 15th Floor\",\n" +
         "\"Thumbnail\": {\n" +
         "\"Url\":    \"http://www.example.com/image/481989943\",\n" +
         "\"Height\": 125,\n" +
         "\"Width\":  100\n" +
         "},\n" +
         "\"IDs\": [116, 943, 234, 38793]\n" +
         "}\n" +
         "}";
      JsonObject aJsonObj = new JsonObject(jsonObjectString);
      vertx.eventBus().send(incomingVertxAddress3, aJsonObj);

      handler1.checkJsonObjectMessageReceived(aJsonObj);
      handler2.checkJsonObjectMessageReceived(aJsonObj);

      //send a JsonArray
      String jsonArrayString = "[\n" +
         "{\n" +
         "\"precision\": \"zip\",\n" +
         "\"Latitude\":  37.7668,\n" +
         "\"Longitude\": -122.3959,\n" +
         "\"Address\":   \"\",\n" +
         "\"City\":      \"SAN FRANCISCO\",\n" +
         "\"State\":     \"CA\",\n" +
         "\"Zip\":       \"94107\",\n" +
         "\"Country\":   \"US\"\n" +
         "},\n" +
         "{\n" +
         "\"precision\": \"zip\",\n" +
         "\"Latitude\":  37.371991,\n" +
         "\"Longitude\": -122.026020,\n" +
         "\"Address\":   \"\",\n" +
         "\"City\":      \"SUNNYVALE\",\n" +
         "\"State\":     \"CA\",\n" +
         "\"Zip\":       \"94085\",\n" +
         "\"Country\":   \"US\"\n" +
         "}\n" +
         "]";
      JsonArray aJsonArray = new JsonArray(jsonArrayString);
      vertx.eventBus().send(incomingVertxAddress3, aJsonArray);

      handler1.checkJsonArrayMessageReceived(aJsonArray);
      handler2.checkJsonArrayMessageReceived(aJsonArray);

   }

   private ClientMessage receiveFromQueue(String queueName) throws Exception {
      ClientMessage msg = null;

      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory sf = createSessionFactory(locator); ClientSession session = sf.createSession(false, true, true)) {

         ClientConsumer consumer = session.createConsumer(queueName);
         session.start();
         msg = consumer.receive(60 * 1000);
         msg.acknowledge();
         session.commit();
      }
      return msg;
   }

   private void createVertxService() {
      System.setProperty("vertx.clusterManagerFactory", HazelcastClusterManagerFactory.class.getName());
      vertxManager = PlatformLocator.factory.createPlatformManager(Integer.valueOf(port), host);
   }

   private class VertxTestHandler implements Handler<BaseMessage<?>> {

      private volatile BaseMessage<?> vertxMsg = null;
      private final Object lock = new Object();

      @Override
      public void handle(BaseMessage<?> arg0) {
         synchronized (lock) {
            vertxMsg = arg0;
            lock.notify();
         }
      }

      void checkJsonArrayMessageReceived(JsonArray aJsonArray) {
         BaseMessage<?> msg = waitMessage();
         JsonArray body = (JsonArray) msg.body();
         assertEquals(aJsonArray, body);
      }

      void checkJsonObjectMessageReceived(final JsonObject aJsonObj) {
         BaseMessage<?> msg = waitMessage();
         JsonObject body = (JsonObject) msg.body();
         assertEquals(aJsonObj, body);
      }

      void checkShortMessageReceived(final Short aShort) {
         BaseMessage<?> msg = waitMessage();
         Short body = (Short) msg.body();
         assertEquals(aShort, body);
      }

      void checkLongMessageReceived(final Long aLong) {
         BaseMessage<?> msg = waitMessage();
         Long body = (Long) msg.body();
         assertEquals(aLong, body);
      }

      void checkIntegerMessageReceived(final Integer aInt) {
         BaseMessage<?> msg = waitMessage();
         Integer body = (Integer) msg.body();
         assertEquals(aInt, body);
      }

      void checkFloatMessageReceived(final Float aFloat) {
         BaseMessage<?> msg = waitMessage();
         Float body = (Float) msg.body();
         assertEquals(aFloat, body);
      }

      void checkDoubleMessageReceived(final Double aDouble) {
         BaseMessage<?> msg = waitMessage();
         Double body = (Double) msg.body();
         assertEquals(aDouble, body);
      }

      void checkCharacterMessageReceived(final Character aChar) {
         BaseMessage<?> msg = waitMessage();
         Character body = (Character) msg.body();
         assertEquals(aChar, body);
      }

      void checkByteMessageReceived(final Byte aByte) {
         BaseMessage<?> msg = waitMessage();
         Byte body = (Byte) msg.body();
         assertEquals(aByte, body);
      }

      void checkByteArrayMessageReceived(final byte[] byteArray) {
         BaseMessage<?> msg = waitMessage();
         byte[] body = (byte[]) msg.body();
         assertEquals(byteArray.length, body.length);
         for (int i = 0; i < byteArray.length; i++) {
            assertEquals(byteArray[i], body[i]);
         }
      }

      void checkBooleanMessageReceived(final Boolean boolValue) {
         BaseMessage<?> msg = waitMessage();
         Boolean body = (Boolean) msg.body();
         assertEquals(boolValue, body);
      }

      void checkStringMessageReceived(final String str) {
         BaseMessage<?> msg = waitMessage();
         String body = (String) msg.body();
         assertEquals(str, body);
      }

      void checkBufferMessageReceived(final Buffer buffer) {
         byte[] source = buffer.getBytes();
         BaseMessage<?> msg = waitMessage();
         Buffer body = (Buffer) msg.body();
         byte[] bytes = body.getBytes();
         assertEquals(source.length, bytes.length);
         for (int i = 0; i < bytes.length; i++) {
            assertEquals(source[i], bytes[i]);
         }
      }

      private BaseMessage<?> waitMessage() {
         BaseMessage<?> msg = null;
         synchronized (lock) {
            long timeout = System.currentTimeMillis() + 10000;
            while (vertxMsg == null && timeout > System.currentTimeMillis()) {
               try {
                  lock.wait(1000);
               } catch (InterruptedException e) {
               }
            }
            msg = vertxMsg;
            vertxMsg = null;
         }
         assertNotNull("Message didn't arrive after 10 seconds.", msg);
         return msg;
      }

   }

   @After
   @Override
   public void tearDown() throws Exception {
      vertxManager.stop();
      server.stop();
      server = null;

      // Something on vertx is setting the TCL to null what would break subsequent tests
      Thread.currentThread().setContextClassLoader(contextClassLoader);
      super.tearDown();
   }

   private CoreQueueConfiguration createCoreQueueConfiguration(String queueName) {
      return new CoreQueueConfiguration().setAddress(queueName).setName(queueName);
   }

   private ConnectorServiceConfiguration createOutgoingConnectorServiceConfiguration(HashMap<String, Object> config,
                                                                                     String name) {
      return new ConnectorServiceConfiguration().setFactoryClassName(VertxOutgoingConnectorServiceFactory.class.getName()).setParams(config).setName(name);
   }

   private ConnectorServiceConfiguration createIncomingConnectorServiceConfiguration(HashMap<String, Object> config,
                                                                                     String name) {
      return new ConnectorServiceConfiguration().setFactoryClassName(VertxIncomingConnectorServiceFactory.class.getName()).setParams(config).setName(name);
   }

   private HashMap<String, Object> createIncomingConnectionConfig(String vertxAddress, String incomingQueue) {
      HashMap<String, Object> config1 = new HashMap<>();
      config1.put(VertxConstants.HOST, host);
      config1.put(VertxConstants.PORT, port);
      config1.put(VertxConstants.VERTX_ADDRESS, vertxAddress);
      config1.put(VertxConstants.QUEUE_NAME, incomingQueue);
      return config1;
   }

   private HashMap<String, Object> createOutgoingConnectionConfig(String queueName, String vertxAddress) {
      HashMap<String, Object> config1 = new HashMap<>();
      config1.put(VertxConstants.HOST, host);
      config1.put(VertxConstants.PORT, port);
      config1.put(VertxConstants.QUEUE_NAME, queueName);
      config1.put(VertxConstants.VERTX_ADDRESS, vertxAddress);
      return config1;
   }
}
