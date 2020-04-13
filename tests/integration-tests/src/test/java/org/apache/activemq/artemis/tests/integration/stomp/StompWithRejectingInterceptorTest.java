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
import java.util.Collections;
import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrame;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrameInterceptor;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Test;

public class StompWithRejectingInterceptorTest extends StompTestBase {

   @Override
   public List<String> getIncomingInterceptors() {
      List<String> stompIncomingInterceptor = new ArrayList<>();
      stompIncomingInterceptor.add(IncomingStompFrameRejectInterceptor.class.getName());

      return stompIncomingInterceptor;
   }

   @Test
   public void stompFrameInterceptor() throws Exception {
      IncomingStompFrameRejectInterceptor.interceptedFrames.clear();

      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World");
      conn.sendFrame(frame);
      conn.disconnect();

      assertTrue(Wait.waitFor(() -> IncomingStompFrameRejectInterceptor.interceptedFrames.size() == 3, 10000, 50));

      List<String> incomingCommands = new ArrayList<>(4);
      incomingCommands.add("CONNECT");
      incomingCommands.add("SEND");
      incomingCommands.add("DISCONNECT");

      for (int i = 0; i < IncomingStompFrameRejectInterceptor.interceptedFrames.size(); i++) {
         Assert.assertEquals(incomingCommands.get(i), IncomingStompFrameRejectInterceptor.interceptedFrames.get(i).getCommand());
      }

      Wait.assertFalse(() -> server.locateQueue(SimpleString.toSimpleString(getQueuePrefix() + getQueueName())).getMessageCount() > 0, 1000, 100);
   }

   public static class IncomingStompFrameRejectInterceptor implements StompFrameInterceptor {

      static List<StompFrame> interceptedFrames = Collections.synchronizedList(new ArrayList<>());

      @Override
      public boolean intercept(StompFrame stompFrame, RemotingConnection connection) {
         interceptedFrames.add(stompFrame);
         if (stompFrame.getCommand().equals(Stomp.Commands.SEND)) {
            return false;
         }
         return true;
      }
   }
}