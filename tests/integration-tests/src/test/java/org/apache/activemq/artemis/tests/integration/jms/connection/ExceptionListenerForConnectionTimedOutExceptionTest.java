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
package org.apache.activemq.artemis.tests.integration.jms.connection;

import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

public class ExceptionListenerForConnectionTimedOutExceptionTest extends JMSTestBase {

   private Queue queue;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      queue = createQueue("TestQueue");
   }

   @Test(timeout = 60000)
   public void testOnAcknowledge() throws Exception {
      Connection sendConnection = null;
      Connection connection = null;
      AtomicReference<JMSException> exceptionOnConnection = new AtomicReference<>();

      try {
         ((ActiveMQConnectionFactory) cf).setOutgoingInterceptorList(OutBoundPacketCapture.class.getName());
         ((ActiveMQConnectionFactory) cf).setIncomingInterceptorList(SessAcknowledgeCauseResponseTimeout.class.getName());
         ((ActiveMQConnectionFactory) cf).setBlockOnAcknowledge(true);

         sendConnection = cf.createConnection();

         final Session sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageProducer producer = sendSession.createProducer(queue);

         TextMessage message = sendSession.createTextMessage();

         message.setText("Message");

         producer.send(message);

         connection = cf.createConnection();
         connection.start();
         connection.setExceptionListener(exceptionOnConnection::set);
         final Session consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         final MessageConsumer messageConsumer = consumerSession.createConsumer(queue);

         TextMessage message1 = (TextMessage) messageConsumer.receive(1000);

         assertEquals("Message", message1.getText());

         message1.acknowledge();

         fail("JMSException expected");

      } catch (JMSException e) {
         assertTrue(e.getCause() instanceof ActiveMQConnectionTimedOutException);
         //Ensure JMS Connection ExceptionListener was also invoked
         assertTrue(Wait.waitFor(() -> exceptionOnConnection.get() != null, 2000, 100));
         assertTrue(exceptionOnConnection.get().getCause() instanceof ActiveMQConnectionTimedOutException);
      } finally {
         if (connection != null) {
            connection.close();
         }
         if (sendConnection != null) {
            sendConnection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testOnSend() throws Exception {
      Connection sendConnection = null;
      Connection connection = null;
      AtomicReference<JMSException> exceptionOnConnection = new AtomicReference<>();

      try {
         ((ActiveMQConnectionFactory) cf).setOutgoingInterceptorList(OutBoundPacketCapture.class.getName());
         ((ActiveMQConnectionFactory) cf).setIncomingInterceptorList(SessSendCauseResponseTimeout.class.getName());

         sendConnection = cf.createConnection();
         sendConnection.setExceptionListener(exceptionOnConnection::set);
         final Session sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageProducer producer = sendSession.createProducer(queue);

         TextMessage message = sendSession.createTextMessage();

         message.setText("Message");

         producer.send(message);

         fail("JMSException expected");

      } catch (JMSException e) {
         assertTrue(e.getCause() instanceof ActiveMQConnectionTimedOutException);
         //Ensure JMS Connection ExceptionListener was also invoked
         assertTrue(Wait.waitFor(() -> exceptionOnConnection.get() != null, 2000, 100));
         assertTrue(exceptionOnConnection.get().getCause() instanceof ActiveMQConnectionTimedOutException);

      } finally {
         if (connection != null) {
            connection.close();
         }
         if (sendConnection != null) {
            sendConnection.close();
         }
      }
   }


   static Packet lastPacketSent;

   public static class OutBoundPacketCapture implements Interceptor {

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException {
         lastPacketSent = packet;
         return true;
      }
   }

   public static class SessAcknowledgeCauseResponseTimeout implements Interceptor {

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException {
         if (lastPacketSent.getType() == PacketImpl.SESS_ACKNOWLEDGE && packet.getType() == PacketImpl.NULL_RESPONSE) {
            return false;
         }
         return true;
      }
   }

   public static class SessSendCauseResponseTimeout implements Interceptor {

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException {
         if (lastPacketSent.getType() == PacketImpl.SESS_SEND && packet.getType() == PacketImpl.NULL_RESPONSE) {
            return false;
         }
         return true;
      }
   }
}
