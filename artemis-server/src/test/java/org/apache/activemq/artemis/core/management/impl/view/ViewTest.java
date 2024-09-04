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
package org.apache.activemq.artemis.core.management.impl.view;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ViewTest {

   @Test
   public void testDefaultConnectionViewNullOptions() {
      ConnectionView connectionView = new ConnectionView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      connectionView.setOptions(null);
   }

   @Test
   public void testDefaultConnectionViewEmptyOptions() {
      ConnectionView connectionView = new ConnectionView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      connectionView.setOptions("");
   }

   @Test
   public void testDefaultSessionViewNullOptions() {
      SessionView sessionView = new SessionView();
      // sanity check to ensure this doesn't just blow up
      sessionView.setOptions(null);
   }

   @Test
   public void testDefaultSessionViewEmptyOptions() {
      SessionView sessionView = new SessionView();
      // sanity check to ensure this doesn't just blow up
      sessionView.setOptions("");
   }

   @Test
   public void testDefaultAddressViewNullOptions() {
      AddressView addressView = new AddressView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      addressView.setOptions(null);
   }

   @Test
   public void testDefaultAddressViewEmptyOptions() {
      AddressView addressView = new AddressView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      addressView.setOptions("");
   }

   @Test
   public void testDefaultQueueViewNullOptions() {
      QueueView queueView = new QueueView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      queueView.setOptions(null);
   }

   @Test
   public void testDefaultQueueViewEmptyOptions() {
      QueueView queueView = new QueueView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      queueView.setOptions("");
   }

   @Test
   public void testDefaultConsumerViewNullOptions() {
      ConsumerView consumerView = new ConsumerView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      consumerView.setOptions(null);
   }

   @Test
   public void testDefaultConsumerViewEmptyOptions() {
      ConsumerView consumerView = new ConsumerView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      consumerView.setOptions("");
   }

   @Test
   public void testDefaultProducerViewNullOptions() {
      ProducerView producerView = new ProducerView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      producerView.setOptions(null);
   }

   @Test
   public void testDefaultProducerViewEmptyOptions() {
      ProducerView producerView = new ProducerView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      producerView.setOptions("");
   }
}
