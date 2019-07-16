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

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StompWebSocketMaxFrameTest extends StompTestBase {

   private URI wsURI;

   private int wsport = 61614;

   private int stompWSMaxFrameSize = 131072; // 128kb

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{{"ws+v10.stomp"}, {"ws+v11.stomp"}, {"ws+v12.stomp"}});
   }

   @Override
   public void setUp() throws Exception {
      super.setUp();
      server.getRemotingService().createAcceptor("test", "tcp://127.0.0.1:" + wsport + "?stompMaxFramePayloadLength=" + stompWSMaxFrameSize).start();
      wsURI = createStompClientUri(scheme, hostname, wsport);
   }

   @Test
   public void testStompSendReceiveWithMaxFramePayloadLength() throws Exception {
      // Assert that sending message > default 64kb fails
      int size = 65536;
      String largeString1 = RandomStringUtils.randomAlphabetic(size);
      String largeString2 = RandomStringUtils.randomAlphabetic(size);

      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri, false);
      conn.getTransport().setMaxFrameSize(stompWSMaxFrameSize);
      conn.getTransport().connect();

      StompClientConnection conn2 = StompClientConnectionFactory.createClientConnection(wsURI, false);
      conn2.getTransport().setMaxFrameSize(stompWSMaxFrameSize);
      conn2.getTransport().connect();

      Wait.waitFor(() -> conn2.getTransport().isConnected() && conn.getTransport().isConnected(), 10000);
      conn.connect();
      conn2.connect();

      subscribeQueue(conn2, "sub1", getQueuePrefix() + getQueueName());

      try {
         // Client is kicked when sending frame > largest frame size.
         send(conn, getQueuePrefix() + getQueueName(), "text/plain", largeString1, false);
         Wait.waitFor(() -> !conn.getTransport().isConnected(), 2000);
         assertFalse(conn.getTransport().isConnected());

         send(conn2, getQueuePrefix() + getQueueName(), "text/plain", largeString2, false);
         Wait.waitFor(() -> !conn2.getTransport().isConnected(), 2000);
         assertTrue(conn2.getTransport().isConnected());

         ClientStompFrame frame = conn2.receiveFrame();
         assertNotNull(frame);
         assertEquals(largeString2, frame.getBody());

      } finally {
         conn2.closeTransport();
         conn.closeTransport();
      }
   }

}
