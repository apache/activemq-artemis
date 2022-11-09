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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class StompLVQTest extends StompTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected StompClientConnection producerConn;
   protected StompClientConnection consumerConn;

   private final String queue = "lvq";

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server.createQueue(new QueueConfiguration(queue).setLastValue(true).setExclusive(true));

      producerConn = StompClientConnectionFactory.createClientConnection(uri);
      consumerConn = StompClientConnectionFactory.createClientConnection(uri);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         if (producerConn != null && producerConn.isConnected()) {
            try {
               producerConn.disconnect();
            } catch (Exception e) {
               // ignore
            }
         }
      } finally {
         producerConn.closeTransport();
      }

      try {
         if (consumerConn != null && consumerConn.isConnected()) {
            try {
               consumerConn.disconnect();
            } catch (Exception e) {
               // ignore
            }
         }
      } finally {
         consumerConn.closeTransport();
      }

      super.tearDown();
   }

   @Test
   public void testLVQ() throws Exception {

      producerConn.connect(defUser, defPass);
      consumerConn.connect(defUser, defPass);

      subscribe(consumerConn, "lvqtest", Stomp.Headers.Subscribe.AckModeValues.CLIENT, null, null, queue, true, 0);

      try {
         for (int i = 1; i <= 100; i++) {
            String uuid = UUID.randomUUID().toString();

            ClientStompFrame frame = producerConn.sendFrame(producerConn.createFrame(Stomp.Commands.SEND)
                                                       .addHeader(Stomp.Headers.Send.DESTINATION, queue)
                                                       .addHeader(Message.HDR_LAST_VALUE_NAME.toString(), "test")
                                                       .addHeader(Stomp.Headers.RECEIPT_REQUESTED, uuid)
                                                       .setBody(String.valueOf(i)));

            assertEquals(Stomp.Responses.RECEIPT, frame.getCommand());
            assertEquals(uuid, frame.getHeader(Stomp.Headers.Response.RECEIPT_ID));
         }
      } catch (Exception e) {
         logger.error(null, e);
      }

      List<ClientStompFrame> messages = new ArrayList<>();
      try {
         ClientStompFrame frame;

         while ((frame = consumerConn.receiveFrame(10000)) != null) {
            assertEquals(Stomp.Responses.MESSAGE, frame.getCommand());

            ack(consumerConn, null, frame);

            messages.add(frame);
         }
      } catch (Exception e) {
         logger.error(null, e);
      }

      Assert.assertEquals(2, messages.size());
      Assert.assertEquals("1", messages.get(0).getBody());
      Assert.assertEquals("100", messages.get(1).getBody());
   }
}