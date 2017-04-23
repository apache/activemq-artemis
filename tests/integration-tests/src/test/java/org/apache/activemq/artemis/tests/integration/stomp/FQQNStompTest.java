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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FQQNStompTest extends StompTestBase {

   private StompClientConnection conn;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      conn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      QueueQueryResult result = server.getActiveMQServer().queueQuery(new SimpleString(getQueueName()));
      assertTrue(result.isExists());
      System.out.println("address: " + result.getAddress() + " queue " + result.getName());
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         boolean connected = conn != null && conn.isConnected();
         if (connected) {
            try {
               conn.disconnect();
            } catch (Exception e) {
            }
         }
      } finally {
         super.tearDown();
      }
   }

   @Test
   //to receive from a FQQN queue like testQueue::testQueue
   //special care is needed as ":" is a reserved character
   //in STOMP. Clients need to escape it.
   public void testReceiveFQQN() throws Exception {
      conn.connect(defUser, defPass);
      subscribeQueue(conn, "sub-01", getQueueName() + "\\c\\c" + getQueueName());
      sendJmsMessage("Hello World!");
      ClientStompFrame frame = conn.receiveFrame(2000);
      assertNotNull(frame);
      assertEquals("Hello World!", frame.getBody());
      System.out.println("frame: " + frame);
      unsubscribe(conn, "sub-01");
   }

   @Test
   public void testReceiveFQQNSpecial() throws Exception {
      conn.connect(defUser, defPass);
      //::queue
      subscribeQueue(conn, "sub-01", "\\c\\c" + getQueueName());
      sendJmsMessage("Hello World!");
      ClientStompFrame frame = conn.receiveFrame(2000);
      assertNotNull(frame);
      assertEquals("Hello World!", frame.getBody());
      System.out.println("frame: " + frame);
      unsubscribe(conn, "sub-01");

      //queue::
      subscribeQueue(conn, "sub-01", getQueueName() + "\\c\\c");
      sendJmsMessage("Hello World!");
      frame = conn.receiveFrame(2000);
      assertNotNull(frame);
      assertEquals("ERROR", frame.getCommand());
      assertTrue(frame.getBody().contains(getQueueName()));
      assertTrue(frame.getBody().contains("not exist"));

      //need reconnect because stomp disconnect on error
      conn = StompClientConnectionFactory.createClientConnection("1.2", hostname, port);
      //:: will subscribe to no queue so no message received.
      subscribeQueue(conn, "sub-01", "\\c\\c");
      sendJmsMessage("Hello World!");
      frame = conn.receiveFrame(2000);
      assertNull(frame);
   }

}
