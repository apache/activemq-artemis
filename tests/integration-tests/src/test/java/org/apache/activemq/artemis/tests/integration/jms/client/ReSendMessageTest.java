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
package org.apache.activemq.artemis.tests.integration.jms.client;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Receive Messages and resend them, like the bridge would do
 */
public class ReSendMessageTest extends JMSTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Queue queue;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testResendWithLargeMessage() throws Exception {
      conn = cf.createConnection();
      conn.start();

      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      ArrayList<Message> msgs = new ArrayList<>();

      for (int i = 0; i < 10; i++) {
         BytesMessage bm = sess.createBytesMessage();
         bm.setObjectProperty(ActiveMQJMSConstants.JMS_ACTIVEMQ_INPUT_STREAM, ActiveMQTestBase.createFakeLargeStream(2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE));
         msgs.add(bm);

         MapMessage mm = sess.createMapMessage();
         mm.setBoolean("boolean", true);
         mm.setByte("byte", (byte) 3);
         mm.setBytes("bytes", new byte[]{(byte) 3, (byte) 4, (byte) 5});
         mm.setChar("char", (char) 6);
         mm.setDouble("double", 7.0);
         mm.setFloat("float", 8.0f);
         mm.setInt("int", 9);
         mm.setLong("long", 10L);
         mm.setObject("object", new String("this is an object"));
         mm.setShort("short", (short) 11);
         mm.setString("string", "this is a string");

         msgs.add(mm);
         msgs.add(sess.createTextMessage("hello" + i));
         msgs.add(sess.createObjectMessage(new SomeSerializable("hello" + i)));
      }

      internalTestResend(msgs, sess);
   }

   @Test
   public void testResendWithMapMessagesOnly() throws Exception {
      conn = cf.createConnection();
      conn.start();

      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      ArrayList<Message> msgs = new ArrayList<>();

      for (int i = 0; i < 1; i++) {
         MapMessage mm = sess.createMapMessage();
         mm.setBoolean("boolean", true);
         mm.setByte("byte", (byte) 3);
         mm.setBytes("bytes", new byte[]{(byte) 3, (byte) 4, (byte) 5});
         mm.setChar("char", (char) 6);
         mm.setDouble("double", 7.0);
         mm.setFloat("float", 8.0f);
         mm.setInt("int", 9);
         mm.setLong("long", 10L);
         mm.setObject("object", new String("this is an object"));
         mm.setShort("short", (short) 11);
         mm.setString("string", "this is a string");

         msgs.add(mm);

         MapMessage emptyMap = sess.createMapMessage();
         msgs.add(emptyMap);
      }

      internalTestResend(msgs, sess);
   }

   public void internalTestResend(final ArrayList<Message> msgs, final Session sess) throws Exception {
      MessageProducer prod = sess.createProducer(queue);

      for (Message msg : msgs) {
         prod.send(msg);
      }

      sess.commit();

      MessageConsumer cons = sess.createConsumer(queue);

      for (int i = 0; i < msgs.size(); i++) {
         Message msg = cons.receive(5000);
         Assert.assertNotNull(msg);

         prod.send(msg);
      }

      Assert.assertNull(cons.receiveNoWait());

      sess.commit();

      for (Message originalMessage : msgs) {
         Message copiedMessage = cons.receive(5000);
         Assert.assertNotNull(copiedMessage);

         Assert.assertEquals(copiedMessage.getClass(), originalMessage.getClass());

         sess.commit();

         if (copiedMessage instanceof BytesMessage) {
            BytesMessage copiedBytes = (BytesMessage) copiedMessage;

            for (int i = 0; i < copiedBytes.getBodyLength(); i++) {
               Assert.assertEquals(ActiveMQTestBase.getSamplebyte(i), copiedBytes.readByte());
            }
         } else if (copiedMessage instanceof MapMessage) {
            MapMessage copiedMap = (MapMessage) copiedMessage;
            MapMessage originalMap = (MapMessage) originalMessage;
            if (originalMap.getString("str") != null) {
               Assert.assertEquals(originalMap.getString("str"), copiedMap.getString("str"));
            }
            if (originalMap.getObject("long") != null) {
               Assert.assertEquals(originalMap.getLong("long"), copiedMap.getLong("long"));
            }
            if (originalMap.getObject("int") != null) {
               Assert.assertEquals(originalMap.getInt("int"), copiedMap.getInt("int"));
            }
            if (originalMap.getObject("object") != null) {
               Assert.assertEquals(originalMap.getObject("object"), copiedMap.getObject("object"));
            }
         } else if (copiedMessage instanceof ObjectMessage) {
            Assert.assertNotSame(((ObjectMessage) originalMessage).getObject(), ((ObjectMessage) copiedMessage).getObject());
            Assert.assertEquals(((ObjectMessage) originalMessage).getObject(), ((ObjectMessage) copiedMessage).getObject());
         } else if (copiedMessage instanceof TextMessage) {
            Assert.assertEquals(((TextMessage) originalMessage).getText(), ((TextMessage) copiedMessage).getText());
         }
      }

   }

   public static class SomeSerializable implements Serializable {

      private static final long serialVersionUID = -8576054940441747312L;

      final String txt;

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + (txt == null ? 0 : txt.hashCode());
         return result;
      }

      @Override
      public boolean equals(final Object obj) {
         if (this == obj) {
            return true;
         }
         if (obj == null) {
            return false;
         }
         if (getClass() != obj.getClass()) {
            return false;
         }
         SomeSerializable other = (SomeSerializable) obj;
         if (txt == null) {
            if (other.txt != null) {
               return false;
            }
         } else if (!txt.equals(other.txt)) {
            return false;
         }
         return true;
      }

      SomeSerializable(final String txt) {
         this.txt = txt;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   @Override
   protected void createCF(final List<TransportConfiguration> connectorConfigs,
                           final String... jndiBindings) throws Exception {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      int callTimeout = 30000;

      jmsServer.createConnectionFactory("ManualReconnectionToSingleServerTest", false, JMSFactoryType.CF, registerConnectors(server, connectorConfigs), null, ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, ActiveMQClient.DEFAULT_CONNECTION_TTL, callTimeout, ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, true, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_COMPRESS_LARGE_MESSAGES, ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE, ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE, ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE, ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE, ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND, ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND, ActiveMQClient.DEFAULT_AUTO_GROUP, ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE, ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS, ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE, retryInterval, retryIntervalMultiplier, ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL, reconnectAttempts, ActiveMQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION, null, jndiBindings);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      queue = createQueue("queue1");
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
