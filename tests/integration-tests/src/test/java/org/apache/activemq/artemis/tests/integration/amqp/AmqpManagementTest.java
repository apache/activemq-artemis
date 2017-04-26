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

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.junit.Assert;
import org.junit.Test;

public class AmqpManagementTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testManagementQueryOverAMQP() throws Throwable {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         String destinationAddress = getQueueName(1);
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender("activemq.management");
         AmqpReceiver receiver = session.createReceiver(destinationAddress);
         receiver.flow(10);

         // Create request message for getQueueNames query
         AmqpMessage request = new AmqpMessage();
         request.setApplicationProperty("_AMQ_ResourceName", ResourceNames.BROKER);
         request.setApplicationProperty("_AMQ_OperationName", "getQueueNames");
         request.setReplyToAddress(destinationAddress);
         request.setText("[]");

         sender.send(request);
         AmqpMessage response = receiver.receive(5, TimeUnit.SECONDS);
         Assert.assertNotNull(response);
         assertNotNull(response);
         Object section = response.getWrappedMessage().getBody();
         assertTrue(section instanceof AmqpValue);
         Object value = ((AmqpValue) section).getValue();
         assertTrue(value instanceof String);
         assertTrue(((String) value).length() > 0);
         assertTrue(((String) value).contains(destinationAddress));
         response.accept();
      } finally {
         connection.close();
      }
   }
}
