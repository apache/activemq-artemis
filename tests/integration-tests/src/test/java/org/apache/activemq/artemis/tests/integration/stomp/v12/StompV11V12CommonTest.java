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
package org.apache.activemq.artemis.tests.integration.stomp.v12;

import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.tests.integration.stomp.StompTestBase;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Tests here applies to both 1.1 and 1.2
 * Note the headers of ACK is different between
 * 1.1 and 1.2, thus we have the ackTx() method
 * to deal with that.
 */
@RunWith(Parameterized.class)
public class StompV11V12CommonTest extends StompTestBase {

   private StompClientConnection conn;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][]{{"tcp+v11.stomp"}, {"ws+v11.stomp"}, {"tcp+v12.stomp"}, {"ws+v12.stomp"}});
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      conn = StompClientConnectionFactory.createClientConnection(uri);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         boolean connected = conn != null && conn.isConnected();
         if (connected) {
            conn.disconnect();
         }
      } finally {
         super.tearDown();
         conn.closeTransport();
      }
   }

   @Test
   public void testAckTransactionalNoTx() throws Exception {

      try {
         conn.connect(defUser, defPass);

         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.CLIENT);

         sendJmsMessage(getName());

         ClientStompFrame frame = conn.receiveFrame();

         String messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);

         //ack with a non-exist tx, resulting in error response
         ClientStompFrame response = ack(conn, "sub1", messageID, "tx1", true);
         assertNotNull(response);
         assertEquals(Stomp.Responses.ERROR, response.getCommand());

         unsubscribe(conn, "sub1");
      } finally {
         conn.disconnect();
      }

      //message should be received because ack failure
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(1000);
      Assert.assertNotNull(message);
   }

   @Test
   public void testAckTransactionalTx() throws Exception {

      try {
         conn.connect(defUser, defPass);

         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.CLIENT);

         sendJmsMessage(getName(), 10);

         //remember 5th message id
         String messageID = null;
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
            messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);
         }

         //consume the rest
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
         }

         beginTransaction(conn, "tx1");

         ackTx(conn, "sub1", messageID, "tx1");

         commitTransaction(conn, "tx1");

         unsubscribe(conn, "sub1");
         conn.disconnect();

         //now reconnect and the 5 unacked should be received.
         conn = StompClientConnectionFactory.createClientConnection(uri);
         conn.connect(defUser, defPass);
         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
         }
      } finally {
         conn.disconnect();
      }
   }

   @Test
   public void testAckTransactionalTxAbort() throws Exception {

      try {
         conn.connect(defUser, defPass);

         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.CLIENT);

         sendJmsMessage(getName(), 10);

         //remember 5th message id
         String messageID = null;
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
            messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);
         }

         //consume the rest
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
         }

         beginTransaction(conn, "tx1");

         ackTx(conn, "sub1", messageID, "tx1");

         abortTransaction(conn, "tx1");
         unsubscribe(conn, "sub1");
         conn.disconnect();

         //now reconnect and the 10 unacked should be received.
         conn = StompClientConnectionFactory.createClientConnection(uri);
         conn.connect(defUser, defPass);
         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);
         for (int i = 0; i < 10; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
         }
      } finally {
         conn.disconnect();
      }
   }

   @Test
   public void testAckTransactionalTxIndividual() throws Exception {

      try {
         conn.connect(defUser, defPass);

         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.CLIENT_INDIVIDUAL);

         sendJmsMessage(getName(), 10);

         //remember 5th message id
         String messageID = null;
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
            messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);
         }

         //consume the rest
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
         }

         beginTransaction(conn, "tx1");

         ackTx(conn, "sub1", messageID, "tx1");

         commitTransaction(conn, "tx1");
         unsubscribe(conn, "sub1");
         conn.disconnect();

         //now reconnect and the 9 unacked should be received.
         conn = StompClientConnectionFactory.createClientConnection(uri);
         conn.connect(defUser, defPass);
         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);
         for (int i = 0; i < 9; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
         }
         ClientStompFrame frame = conn.receiveFrame(1000);
         assertNull(frame);
      } finally {
         conn.disconnect();
      }
   }

   @Test
   public void testAckTransactionalTxIndividualAbort() throws Exception {

      try {
         conn.connect(defUser, defPass);

         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.CLIENT_INDIVIDUAL);

         sendJmsMessage(getName(), 10);

         //remember 5th message id
         String messageID = null;
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
            messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);
         }

         //consume the rest
         for (int i = 0; i < 5; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
         }

         beginTransaction(conn, "tx1");

         ackTx(conn, "sub1", messageID, "tx1");

         abortTransaction(conn, "tx1");
         unsubscribe(conn, "sub1");
         conn.disconnect();

         //now reconnect and the 10 unacked should be received.
         conn = StompClientConnectionFactory.createClientConnection(uri);
         conn.connect(defUser, defPass);
         subscribe(conn, "sub1", Stomp.Headers.Subscribe.AckModeValues.AUTO);
         for (int i = 0; i < 10; i++) {
            ClientStompFrame frame = conn.receiveFrame();
            assertNotNull(frame);
         }
      } finally {
         conn.disconnect();
      }
   }

   @Test
   public void testTransactionalNAck() throws Exception {
      try {
         conn.connect(defUser, defPass);

         subscribe(conn, getName(), Stomp.Headers.Subscribe.AckModeValues.CLIENT_INDIVIDUAL);

         sendJmsMessage(getName());

         beginTransaction(conn, "tx1");

         ClientStompFrame frame = conn.receiveFrame();

         Assert.assertTrue(frame.getCommand().equals(Stomp.Responses.MESSAGE));
         Assert.assertNotNull(frame.getHeader(Stomp.Headers.Message.DESTINATION));
         Assert.assertTrue(frame.getBody().equals(getName()));
         Assert.assertNotNull(frame.getHeader(Stomp.Headers.Message.MESSAGE_ID));

         String messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);

         nackTx(conn, getName(), messageID, "tx1");

         commitTransaction(conn, "tx1");

         frame = conn.receiveFrame(500);

         assertNull(frame);

         //abort
         sendJmsMessage(getName());
         beginTransaction(conn, "tx2");
         frame = conn.receiveFrame();
         messageID = frame.getHeader(Stomp.Headers.Message.MESSAGE_ID);
         nackTx(conn, getName(), messageID, "tx2");
         abortTransaction(conn, "tx2");
         frame = conn.receiveFrame(2000);
         assertNotNull(frame);
         assertEquals(messageID, frame.getHeader(Stomp.Headers.Message.MESSAGE_ID));

         unsubscribe(conn, getName());
      } finally {
         conn.disconnect();
      }
   }

   public static ClientStompFrame ackTx(StompClientConnection conn,
                                        String subscriptionId,
                                        String mid,
                                        String txID) throws IOException, InterruptedException {
      if (conn.getVersion().equals("1.2")) {
         ClientStompFrame frame = conn.createFrame(Stomp.Commands.ACK)
                 .addHeader(Stomp.Headers.Subscribe.ID, mid)
                 .addHeader(Stomp.Headers.RECEIPT_REQUESTED, "response");
         if (txID != null) {
            frame.addHeader(Stomp.Headers.TRANSACTION, txID);
         }

         ClientStompFrame response = conn.sendFrame(frame);
         return response;
      }
      return ack(conn, subscriptionId, mid, txID, true);
   }

   public static ClientStompFrame nackTx(StompClientConnection conn,
                                      String subscriptionId,
                                      String mid,
                                      String txID) throws IOException, InterruptedException {
      if (conn.getVersion().equals("1.2")) {
         ClientStompFrame frame = conn.createFrame(Stomp.Commands.NACK)
                 .addHeader(Stomp.Headers.Subscribe.ID, mid)
                 .addHeader(Stomp.Headers.RECEIPT_REQUESTED, "response");
         if (txID != null) {
            frame.addHeader(Stomp.Headers.TRANSACTION, txID);
         }

         ClientStompFrame response = conn.sendFrame(frame);
         return response;
      }
      return nack(conn, subscriptionId, mid, txID, true);
   }

}
