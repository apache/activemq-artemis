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
package org.apache.activemq.artemis.tests.integration.stomp.v11;

import java.nio.charset.StandardCharsets;

import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * Some Stomp tests against server with persistence enabled are put here.
 */
public class ExtraStompTest extends StompV11TestBase {

   private StompClientConnection connV10;
   private StompClientConnection connV11;

   @Override
   @Before
   public void setUp() throws Exception {
      persistenceEnabled = true;
      super.setUp();
      connV10 = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);
      connV10.connect(defUser, defPass);
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      connV11.connect(defUser, defPass);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         connV10.disconnect();
         connV11.disconnect();
      }
      finally {
         super.tearDown();
      }
   }

   @Test
   public void testSendAndReceive10() throws Exception {
      String msg1 = "Hello World 1!";
      String msg2 = "Hello World 2!";

      ClientStompFrame frame = connV10.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", String.valueOf(msg1.getBytes(StandardCharsets.UTF_8).length));
      frame.addHeader("persistent", "true");
      frame.setBody(msg1);

      connV10.sendFrame(frame);

      ClientStompFrame frame2 = connV10.createFrame("SEND");
      frame2.addHeader("destination", getQueuePrefix() + getQueueName());
      frame2.addHeader("content-length", String.valueOf(msg2.getBytes(StandardCharsets.UTF_8).length));
      frame2.addHeader("persistent", "true");
      frame2.setBody(msg2);

      connV10.sendFrame(frame2);

      ClientStompFrame subFrame = connV10.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV10.sendFrame(subFrame);

      frame = connV10.receiveFrame();

      System.out.println("received " + frame);

      assertEquals("MESSAGE", frame.getCommand());

      assertEquals("a-sub", frame.getHeader("subscription"));

      assertNotNull(frame.getHeader("message-id"));

      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));

      assertEquals(msg1, frame.getBody());

      frame = connV10.receiveFrame();

      System.out.println("received " + frame);

      assertEquals("MESSAGE", frame.getCommand());

      assertEquals("a-sub", frame.getHeader("subscription"));

      assertNotNull(frame.getHeader("message-id"));

      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));

      assertEquals(msg2, frame.getBody());

      //unsub
      ClientStompFrame unsubFrame = connV10.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV10.sendFrame(unsubFrame);

   }

   @Test
   public void testSendAndReceive11() throws Exception {
      String msg1 = "Hello World 1!";
      String msg2 = "Hello World 2!";

      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", String.valueOf(msg1.getBytes(StandardCharsets.UTF_8).length));
      frame.addHeader("persistent", "true");
      frame.setBody(msg1);

      connV11.sendFrame(frame);

      ClientStompFrame frame2 = connV11.createFrame("SEND");
      frame2.addHeader("destination", getQueuePrefix() + getQueueName());
      frame2.addHeader("content-length", String.valueOf(msg2.getBytes(StandardCharsets.UTF_8).length));
      frame2.addHeader("persistent", "true");
      frame2.setBody(msg2);

      connV11.sendFrame(frame2);

      ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV11.sendFrame(subFrame);

      frame = connV11.receiveFrame();

      System.out.println("received " + frame);

      assertEquals("MESSAGE", frame.getCommand());

      assertEquals("a-sub", frame.getHeader("subscription"));

      assertNotNull(frame.getHeader("message-id"));

      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));

      assertEquals(msg1, frame.getBody());

      frame = connV11.receiveFrame();

      System.out.println("received " + frame);

      assertEquals("MESSAGE", frame.getCommand());

      assertEquals("a-sub", frame.getHeader("subscription"));

      assertNotNull(frame.getHeader("message-id"));

      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));

      assertEquals(msg2, frame.getBody());

      //unsub
      ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV11.sendFrame(unsubFrame);
   }

   @Test
   public void testNoGarbageAfterPersistentMessageV10() throws Exception {
      ClientStompFrame subFrame = connV10.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV10.sendFrame(subFrame);

      ClientStompFrame frame = connV10.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", "11");
      frame.addHeader("persistent", "true");
      frame.setBody("Hello World");

      connV10.sendFrame(frame);

      frame = connV10.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", "11");
      frame.addHeader("persistent", "true");
      frame.setBody("Hello World");

      connV10.sendFrame(frame);

      frame = connV10.receiveFrame(10000);

      System.out.println("received: " + frame);

      assertEquals("Hello World", frame.getBody());

      //if activemq sends trailing garbage bytes, the second message
      //will not be normal
      frame = connV10.receiveFrame(10000);

      System.out.println("received: " + frame);

      assertEquals("Hello World", frame.getBody());

      //unsub
      ClientStompFrame unsubFrame = connV10.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV10.sendFrame(unsubFrame);

   }

   @Test
   public void testNoGarbageOnPersistentRedeliveryV10() throws Exception {

      ClientStompFrame frame = connV10.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", "11");
      frame.addHeader("persistent", "true");
      frame.setBody("Hello World");

      connV10.sendFrame(frame);

      frame = connV10.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", "11");
      frame.addHeader("persistent", "true");
      frame.setBody("Hello World");

      connV10.sendFrame(frame);

      ClientStompFrame subFrame = connV10.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "client");

      connV10.sendFrame(subFrame);

      // receive but don't ack
      frame = connV10.receiveFrame(10000);
      frame = connV10.receiveFrame(10000);

      System.out.println("received: " + frame);

      //unsub
      ClientStompFrame unsubFrame = connV10.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV10.sendFrame(unsubFrame);

      subFrame = connV10.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV10.sendFrame(subFrame);

      frame = connV10.receiveFrame(10000);
      frame = connV10.receiveFrame(10000);

      //second receive will get problem if trailing bytes
      assertEquals("Hello World", frame.getBody());

      System.out.println("received again: " + frame);

      //unsub
      unsubFrame = connV10.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV10.sendFrame(unsubFrame);
   }

   @Test
   public void testNoGarbageAfterPersistentMessageV11() throws Exception {
      ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV11.sendFrame(subFrame);
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", "11");
      frame.addHeader("persistent", "true");
      frame.setBody("Hello World");

      connV11.sendFrame(frame);

      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", "11");
      frame.addHeader("persistent", "true");
      frame.setBody("Hello World");

      connV11.sendFrame(frame);
      frame = connV11.receiveFrame(10000);

      System.out.println("received: " + frame);

      assertEquals("Hello World", frame.getBody());

      //if activemq sends trailing garbage bytes, the second message
      //will not be normal
      frame = connV11.receiveFrame(10000);

      System.out.println("received: " + frame);

      assertEquals("Hello World", frame.getBody());

      //unsub
      ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV11.sendFrame(unsubFrame);
   }

   @Test
   public void testNoGarbageOnPersistentRedeliveryV11() throws Exception {
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", "11");
      frame.addHeader("persistent", "true");
      frame.setBody("Hello World");

      connV11.sendFrame(frame);

      frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", "11");
      frame.addHeader("persistent", "true");
      frame.setBody("Hello World");

      connV11.sendFrame(frame);

      ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "client");

      connV11.sendFrame(subFrame);

      // receive but don't ack
      frame = connV11.receiveFrame(10000);
      frame = connV11.receiveFrame(10000);

      System.out.println("received: " + frame);

      //unsub
      ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV11.sendFrame(unsubFrame);

      subFrame = connV11.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");

      connV11.sendFrame(subFrame);

      frame = connV11.receiveFrame(10000);
      frame = connV11.receiveFrame(10000);

      //second receive will get problem if trailing bytes
      assertEquals("Hello World", frame.getBody());

      System.out.println("received again: " + frame);

      //unsub
      unsubFrame = connV11.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV11.sendFrame(unsubFrame);
   }

}
