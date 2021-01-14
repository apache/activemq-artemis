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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AmqpPagingTest extends AmqpClientTestSupport {

   @Parameterized.Parameters(name = "durability={0}, readWholePage={1}")
   public static Collection getParams() {
      return Arrays.asList(new Object[][]{
         {Boolean.TRUE, true}, {Boolean.TRUE, false},
         {Boolean.FALSE, true}, {Boolean.FALSE, false},
         {null, true}, {null, false}});
   }

   private final Boolean durable;
   private final boolean readWholePage;

   public AmqpPagingTest(Boolean durable, boolean readWholePage) {
      this.durable = durable;
      this.readWholePage = readWholePage;
   }

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      super.addConfiguration(server);
      final Map<String, AddressSettings> addressesSettings = server.getConfiguration()
         .setReadWholePage(readWholePage)
         .getAddressesSettings();
      addressesSettings.get("#")
         .setMaxSizeBytes(100000)
         .setPageSizeBytes(10000);
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
         if (durable != null) {
            message.setDurable(durable);
         }
         sender.send(message);
      }
      sender.close();
      final Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertEquals(MSG_COUNT, queueView::getMessageCount);
      PagingStore pagingStore = server.getPagingManager().getPageStore(SimpleString.toSimpleString(getQueueName()));
      Assert.assertTrue(pagingStore.isPaging());
      final int pageCacheMaxSize = server.getConfiguration().getAddressesSettings().get("#").getPageCacheMaxSize();
      Assert.assertThat("the size of the messages or the number of messages isn't enough",
                        pagingStore.getNumberOfPages(), Matchers.greaterThan(pageCacheMaxSize));
      receiver.flow(MSG_COUNT);
      for (int i = 0; i < MSG_COUNT; i++) {
         AmqpMessage receive = receiver.receive(10, TimeUnit.SECONDS);
         assertNotNull("Not received anything after " + i + " receive", receive);
         Assert.assertEquals(durable == null ? false : durable.booleanValue(), receive.isDurable());
         receive.accept();
      }
      receiver.close();
      connection.close();
   }


   @Test(timeout = 60000)
   public void testSizeCalculationsForApplicationProperties() throws Exception {
      final int MSG_SIZE = 1000;
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < MSG_SIZE; i++) {
         builder.append('0');
      }
      final String data = builder.toString();
      final int MSG_COUNT = 1;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName(), true);

      // use selector expression that references a property to force decode of application properties
      AmqpReceiver receiver = session.createReceiver(getQueueName(), "myData IS NOT NULL");
      receiver.setPresettle(true);
      receiver.flow(10);
      Assert.assertNull("somehow the queue had messages from a previous test", receiver.receiveNoWait());
      receiver.flow(0);

      AmqpMessage message = new AmqpMessage();
      message.setText(data);

      // large message property also
      message.setApplicationProperty("myData", data);

      if (durable != null) {
         message.setDurable(durable);
      }
      sender.send(message);

      PagingStore pagingStore = server.getPagingManager().getPageStore(SimpleString.toSimpleString(getQueueName()));

      // verify page usage reflects data + 2*application properties (encoded and decoded)
      assertTrue(Wait.waitFor(() -> {
         return pagingStore.getAddressSize() > 3000;
      }));

      receiver.flow(MSG_COUNT);
      AmqpMessage receive = receiver.receive(10, TimeUnit.MINUTES);
      assertNotNull("Not received anything after receive", receive);
      receive.accept();

      assertTrue(Wait.waitFor(() -> {
         return pagingStore.getAddressSize() == 0;
      }));

      // send another with duplicate id property, to force early decode
      message = new AmqpMessage();
      message.setText(data);

      // ensures application properties are referenced
      message.setApplicationProperty("_AMQ_DUPL_ID", "1");

      // large message property also
      message.setApplicationProperty("myData", data);

      if (durable != null) {
         message.setDurable(durable);
      }
      sender.send(message);

      sender.close();

      // verify page usage reflects data + 2*application properties (encoded and decoded)
      assertTrue(Wait.waitFor(() -> {
         return pagingStore.getAddressSize() > 3000;
      }));

      receiver.flow(MSG_COUNT);
      receive = receiver.receive(10, TimeUnit.MINUTES);
      assertNotNull("Not received anything after receive", receive);
      receive.accept();

      receiver.close();
      connection.close();
   }

}
