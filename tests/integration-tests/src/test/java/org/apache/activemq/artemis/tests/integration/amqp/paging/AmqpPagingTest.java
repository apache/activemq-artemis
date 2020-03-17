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
package org.apache.activemq.artemis.tests.integration.amqp.paging;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Assert;
import org.junit.Test;

public class AmqpPagingTest extends AmqpClientTestSupport {

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      super.addConfiguration(server);
      final Map<String, AddressSettings> addressesSettings = server.getConfiguration().getAddressesSettings();
      addressesSettings.get("#").setMaxSizeBytes(100000).setPageSizeBytes(10000);
   }

   @Test(timeout = 60000)
   public void testPaging() throws Exception {
      final int MSG_SIZE = 1000;
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < MSG_SIZE; i++) {
         builder.append('0');
      }
      final String data = builder.toString();
      final int MSG_COUNT = 1_000;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName(), true);

      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.setPresettle(true);
      receiver.flow(10);
      Assert.assertNull("somehow the queue had messages from a previous test", receiver.receiveNoWait());
      receiver.flow(0);
      for (int i = 0; i < MSG_COUNT; i++) {
         AmqpMessage message = new AmqpMessage();
         message.setText(data);
         sender.send(message);
      }
      sender.close();
      receiver.flow(MSG_COUNT);
      for (int i = 0; i < MSG_COUNT; i++) {
         AmqpMessage receive = receiver.receive(10, TimeUnit.SECONDS);
         assertNotNull("Not received anything after " + i + " receive", receive);
         receive.accept();
      }
      receiver.close();
      connection.close();
   }

}
