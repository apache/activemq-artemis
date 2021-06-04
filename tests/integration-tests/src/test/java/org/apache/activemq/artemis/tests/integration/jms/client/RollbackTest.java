/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.logging.Logger;
import org.junit.Before;
import org.junit.Test;

public class RollbackTest extends ActiveMQTestBase {

   protected ActiveMQServer server;

   private static Logger log = Logger.getLogger(RollbackTest.class);

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(false, createDefaultInVMConfig());
      server.getConfiguration().getIncomingInterceptorClassNames().add(MyInterceptor.class.getName());
      server.start();
   }

   @Test
   public void testFailedRollback() throws Exception {
      final String TOPIC = "myTopic";
      final String SUBSCRIPTION = "mySub";

      try (ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://0")) {
         connectionFactory.setCallTimeout(1000); // fail fast
         connectionFactory.setReconnectAttempts(-1);
         connectionFactory.setConfirmationWindowSize(1024 * 1024);

         try (Connection consumerConnection = connectionFactory.createConnection()) {
            consumerConnection.start();
            final Session session = consumerConnection.createSession(Session.SESSION_TRANSACTED);
            Topic topic = session.createTopic(TOPIC);
            final MessageConsumer messageConsumer = session.createSharedDurableConsumer(topic, SUBSCRIPTION);
            MessageProducer p = session.createProducer(topic);
            p.send(session.createMessage());
            p.close();
            session.commit();

            try {
               Message m = messageConsumer.receive(2000);
               assertNotNull(m);
               // the interceptor will block this first rollback and trigger a failure, the failure will cause the client to re-attach its session
               session.rollback();
               fail();
            } catch (JMSException jmsException) {
               // expected
            }

            try {
               session.rollback();
            } catch (JMSException e) {
               fail("Rollback failed again! Giving up. " + e.getMessage());
            }

            Message m = messageConsumer.receive(2000);
            assertNotNull(m);
            try {
               session.commit();
            } catch (JMSException e) {
               fail("Commit failed. " + e.getMessage());
            }
         }
      }

      assertEquals(0L, server.locateQueue(SUBSCRIPTION).getMessageCount());
   }

   public static class MyInterceptor implements Interceptor {
      private boolean intercepted = false;

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException {
         if (!intercepted && packet.getType() == PacketImpl.SESS_ROLLBACK) {
            intercepted = true;
            return false;
         }
         return true;
      }
   }
}
