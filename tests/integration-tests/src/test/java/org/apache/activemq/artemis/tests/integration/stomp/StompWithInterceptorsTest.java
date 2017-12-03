/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.stomp;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrame;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrameInterceptor;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Test;

public class StompWithInterceptorsTest extends StompTestBase {

   @Override
   public List<String> getIncomingInterceptors() {
      List<String> stompIncomingInterceptor = new ArrayList<>();
      stompIncomingInterceptor.add("org.apache.activemq.artemis.tests.integration.stomp.StompWithInterceptorsTest$MyIncomingStompFrameInterceptor");
      stompIncomingInterceptor.add("org.apache.activemq.artemis.tests.integration.stomp.StompWithInterceptorsTest$MyCoreInterceptor");

      return stompIncomingInterceptor;
   }

   @Override
   public List<String> getOutgoingInterceptors() {
      List<String> stompOutgoingInterceptor = new ArrayList<>();
      stompOutgoingInterceptor.add("org.apache.activemq.artemis.tests.integration.stomp.StompWithInterceptorsTest$MyOutgoingStompFrameInterceptor");

      return stompOutgoingInterceptor;
   }

   @Test
   public void stompFrameInterceptor() throws Exception {
      MyIncomingStompFrameInterceptor.incomingInterceptedFrames.clear();
      MyOutgoingStompFrameInterceptor.outgoingInterceptedFrames.clear();

      Thread.sleep(200);

      // So we clear them here
      MyCoreInterceptor.incomingInterceptedFrames.clear();

      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");
      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      conn.sendFrame(subFrame);

      assertEquals(0, MyCoreInterceptor.incomingInterceptedFrames.size());
      sendJmsMessage(getName());

      // Something was supposed to be called on sendMessages
      assertTrue("core interceptor is not working", MyCoreInterceptor.incomingInterceptedFrames.size() > 0);

      conn.receiveFrame(10000);

      ClientStompFrame frame = conn.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");
      conn.sendFrame(frame);

      conn.disconnect();

      List<String> incomingCommands = new ArrayList<>(4);
      incomingCommands.add("CONNECT");
      incomingCommands.add("SUBSCRIBE");
      incomingCommands.add("SEND");
      incomingCommands.add("DISCONNECT");

      List<String> outgoingCommands = new ArrayList<>(3);
      outgoingCommands.add("CONNECTED");
      outgoingCommands.add("MESSAGE");
      outgoingCommands.add("MESSAGE");

      long timeout = System.currentTimeMillis() + 1000;

      // Things are async, giving some time to things arrive before we actually assert
      while (MyIncomingStompFrameInterceptor.incomingInterceptedFrames.size() < 4 &&
         MyOutgoingStompFrameInterceptor.outgoingInterceptedFrames.size() < 3 &&
         timeout > System.currentTimeMillis()) {
         Thread.sleep(10);
      }

      Wait.waitFor(() -> {
         return  MyIncomingStompFrameInterceptor.incomingInterceptedFrames.size() == 4;
      });
      Assert.assertEquals(4, MyIncomingStompFrameInterceptor.incomingInterceptedFrames.size());
      Wait.waitFor(() -> {
         return  MyOutgoingStompFrameInterceptor.outgoingInterceptedFrames.size() == 3;
      });
      Assert.assertEquals(3, MyOutgoingStompFrameInterceptor.outgoingInterceptedFrames.size());

      for (int i = 0; i < MyIncomingStompFrameInterceptor.incomingInterceptedFrames.size(); i++) {
         Assert.assertEquals(incomingCommands.get(i), MyIncomingStompFrameInterceptor.incomingInterceptedFrames.get(i).getCommand());
         Assert.assertEquals("incomingInterceptedVal", MyIncomingStompFrameInterceptor.incomingInterceptedFrames.get(i).getHeader("incomingInterceptedProp"));
      }

      for (int i = 0; i < MyOutgoingStompFrameInterceptor.outgoingInterceptedFrames.size(); i++) {
         Assert.assertEquals(outgoingCommands.get(i), MyOutgoingStompFrameInterceptor.outgoingInterceptedFrames.get(i).getCommand());
      }

      Assert.assertEquals("incomingInterceptedVal", MyOutgoingStompFrameInterceptor.outgoingInterceptedFrames.get(2).getHeader("incomingInterceptedProp"));
      Assert.assertEquals("outgoingInterceptedVal", MyOutgoingStompFrameInterceptor.outgoingInterceptedFrames.get(2).getHeader("outgoingInterceptedProp"));
   }

   public static class MyCoreInterceptor implements Interceptor {

      static List<Packet> incomingInterceptedFrames = new ArrayList<>();

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) {
         IntegrationTestLogger.LOGGER.info("Core intercepted: " + packet);
         incomingInterceptedFrames.add(packet);
         return true;
      }
   }

   public static class MyIncomingStompFrameInterceptor implements StompFrameInterceptor {

      static List<StompFrame> incomingInterceptedFrames = new ArrayList<>();

      @Override
      public boolean intercept(StompFrame stompFrame, RemotingConnection connection) {
         incomingInterceptedFrames.add(stompFrame);
         stompFrame.addHeader("incomingInterceptedProp", "incomingInterceptedVal");
         return true;
      }
   }

   public static class MyOutgoingStompFrameInterceptor implements StompFrameInterceptor {

      static List<StompFrame> outgoingInterceptedFrames = new ArrayList<>();

      @Override
      public boolean intercept(StompFrame stompFrame, RemotingConnection connection) {
         outgoingInterceptedFrames.add(stompFrame);
         stompFrame.addHeader("outgoingInterceptedProp", "outgoingInterceptedVal");
         return true;
      }
   }
}
