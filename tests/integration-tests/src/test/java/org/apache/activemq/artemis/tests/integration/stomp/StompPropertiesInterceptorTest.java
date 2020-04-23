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
package org.apache.activemq.artemis.tests.integration.stomp;

import org.apache.activemq.artemis.core.protocol.stomp.StompFrame;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrameInterceptor;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.felix.resolver.util.ArrayMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(value = Parameterized.class)
public class StompPropertiesInterceptorTest extends StompTestBase {

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{{"ws+v12.stomp"}, {"tcp+v12.stomp"}});
   }

   @Override
   public List<String> getIncomingInterceptors() {
      List<String> stompIncomingInterceptor = new ArrayList<>();
      stompIncomingInterceptor.add("org.apache.activemq.artemis.tests.integration.stomp.StompPropertiesInterceptorTest$StompFramePropertiesInterceptor");
      return stompIncomingInterceptor;
   }

   @Override
   public List<String> getOutgoingInterceptors() {
      List<String> stompOutgoingInterceptor = new ArrayList<>();
      stompOutgoingInterceptor.add("org.apache.activemq.artemis.tests.integration.stomp.StompPropertiesInterceptorTest$StompFramePropertiesInterceptor");

      return stompOutgoingInterceptor;
   }

   public static class StompFramePropertiesInterceptor implements StompFrameInterceptor {

      @Override
      public boolean intercept(StompFrame stompFrame, RemotingConnection connection) {
         if (stompFrame.getCommand().equals("CONNECT") || stompFrame.getCommand().equals("CONNECTED")) {
            return true;
         }
         assertNotNull(stompFrame);
         assertEquals(stompFrame.getHeader(MY_HEADER), expectedProperties.get(MY_HEADER));
         assertEquals(stompFrame.getBody(), expectedProperties.get(MESSAGE_TEXT));
         return true;
      }
   }


   private static final String MESSAGE_TEXT = "messageText";
   private static final String MY_HEADER = "my-header";
   private static Map<String, Object> expectedProperties = new ArrayMap<>();

   @Test(timeout = 60000)
   public void testCheckInterceptedStompMessageProperties() throws Exception {
      final String msgText = "Test intercepted message";
      final String myHeader = "TestInterceptedHeader";
      expectedProperties.put(MESSAGE_TEXT, msgText);
      expectedProperties.put(MY_HEADER, myHeader);

      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      ClientStompFrame subFrame = conn.createFrame("SUBSCRIBE");

      subFrame.addHeader("subscription-type", "ANYCAST");
      subFrame.addHeader("destination", name.getMethodName());
      subFrame.addHeader("ack", "auto");
      subFrame.addHeader(MY_HEADER, myHeader);
      subFrame.setBody(msgText);

      conn.sendFrame(subFrame);

      ClientStompFrame frame = conn.createFrame("SEND");
      frame.addHeader("destination", name.getMethodName());
      frame.addHeader("ack", "auto");
      frame.addHeader(MY_HEADER, myHeader);
      conn.sendFrame(frame);

      conn.disconnect();

   }
}
