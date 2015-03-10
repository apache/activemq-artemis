/**
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
package org.apache.activemq.tests.integration.stomp;

import org.junit.Test;

import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.junit.Assert;

import org.apache.activemq.core.protocol.stomp.Stomp;
import org.apache.activemq.jms.server.JMSServerManager;

/**
 * A StompConnectionCleanupTest
 */
public class StompConnectionCleanupTest extends StompTestBase
{
   private static final long CONNECTION_TTL = 2000;

   @Test
   public void testConnectionCleanup() throws Exception
   {
      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);
      frame = receiveFrame(10000);

      //We send and consumer a message to ensure a STOMP connection and server session is created

      System.out.println("Received frame: " + frame);

      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("MESSAGE"));
      Assert.assertTrue(frame.indexOf("destination:") > 0);

      // Now we wait until the connection is cleared on the server, which will happen some time after ttl, since no data
      // is being sent

      long start = System.currentTimeMillis();

      while (true)
      {
         int connCount = server.getActiveMQServer().getRemotingService().getConnections().size();

         int sessionCount = server.getActiveMQServer().getSessions().size();

         // All connections and sessions should be timed out including STOMP + JMS connection

         if (connCount == 0 && sessionCount == 0)
         {
            break;
         }

         Thread.sleep(10);

         if (System.currentTimeMillis() - start > 10000)
         {
            fail("Timed out waiting for connection to be cleared up");
         }
      }
   }

   @Test
   public void testConnectionNotCleanedUp() throws Exception
   {
      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);
      frame = receiveFrame(10000);

      //We send and consumer a message to ensure a STOMP connection and server session is created

      Assert.assertTrue(frame.startsWith("CONNECTED"));

      MessageConsumer consumer = session.createConsumer(queue);

      long time = CONNECTION_TTL * 3;

      long start = System.currentTimeMillis();

      //Send msgs for an amount of time > connection_ttl make sure connection is not closed
      while (true)
      {
         //Send and receive a msg

         frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;
         sendFrame(frame);

         Message msg = consumer.receive(1000);
         assertNotNull(msg);

         Thread.sleep(100);

         if (System.currentTimeMillis() - start > time)
         {
            break;
         }
      }

   }

   @Override
   protected JMSServerManager createServer() throws Exception
   {
      JMSServerManager s = super.createServer();

      s.getActiveMQServer().getConfiguration().setConnectionTTLOverride(CONNECTION_TTL);

      return s;
   }
}
