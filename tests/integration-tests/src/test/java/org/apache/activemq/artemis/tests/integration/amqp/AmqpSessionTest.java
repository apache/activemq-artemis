/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Session;
import org.junit.Test;

public class AmqpSessionTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testCreateSession() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();
      assertNotNull(session);
      connection.close();
   }

   @Test(timeout = 60000)
   public void testSessionClosedDoesNotGetReceiverDetachFromRemote() throws Exception {
      AmqpClient client = createAmqpClient();
      assertNotNull(client);

      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectClosedResource(Session session) {
            IntegrationTestLogger.LOGGER.info("Session closed: " + session.getContext());
         }

         @Override
         public void inspectDetachedResource(Receiver receiver) {
            markAsInvalid("Broker should not detach receiver linked to closed session.");
         }

         @Override
         public void inspectClosedResource(Receiver receiver) {
            markAsInvalid("Broker should not close receiver linked to closed session.");
         }
      });

      AmqpConnection connection = addConnection(client.connect());
      assertNotNull(connection);
      AmqpSession session = connection.createSession();
      assertNotNull(session);
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      assertNotNull(receiver);

      session.close();

      connection.getStateInspector().assertValid();
      connection.close();
   }
}
