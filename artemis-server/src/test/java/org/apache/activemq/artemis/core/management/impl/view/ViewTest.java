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

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
   public void testConnectionViewLegacySort() {
      ConnectionView connectionView = new ConnectionView(Mockito.mock(ActiveMQServer.class));
      assertNotEquals("protocol", connectionView.getDefaultOrderColumn());
      connectionView.setOptions("{\"field\":\"protocol\",\"operation\":\"EQUALS\",\"value\":\"CORE\",\"sortColumn\":\"protocol\",\"sortOrder\":\"asc\"}");
      assertEquals("protocol", connectionView.getSortField());
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
   public void testSessionViewLegacySort() {
      SessionView sessionView = new SessionView();
      assertNotEquals("user", sessionView.getDefaultOrderColumn());
      sessionView.setOptions("{\"field\":\"user\",\"operation\":\"EQUALS\",\"value\":\"123\",\"sortColumn\":\"user\",\"sortOrder\":\"asc\"}");
      assertEquals("user", sessionView.getSortField());
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
   public void testAddressViewLegacySort() {
      AddressView addressView = new AddressView(Mockito.mock(ActiveMQServer.class));
      assertNotEquals("name", addressView.getDefaultOrderColumn());
      addressView.setOptions("{\"field\":\"name\",\"operation\":\"EQUALS\",\"value\":\"123\",\"sortColumn\":\"name\",\"sortOrder\":\"asc\"}");
      assertEquals("name", addressView.getSortField());
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
   public void testQueueViewLegacySort() {
      QueueView view = new QueueView(Mockito.mock(ActiveMQServer.class));
      assertNotEquals("id", view.getDefaultOrderColumn());
      view.setOptions("{\"field\":\"id\",\"operation\":\"EQUALS\",\"value\":\"123\",\"sortColumn\":\"id\",\"sortOrder\":\"asc\"}");
      assertEquals("id", view.getSortField());
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
   public void testConsumerViewLegacySort() {
      ConsumerView view = new ConsumerView(Mockito.mock(ActiveMQServer.class));
      assertNotEquals("user", view.getDefaultOrderColumn());
      view.setOptions("{\"field\":\"user\",\"operation\":\"EQUALS\",\"value\":\"123\",\"sortColumn\":\"user\",\"sortOrder\":\"asc\"}");
      assertEquals("user", view.getSortField());
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

   @Test
   public void testProducerViewLegacySort() {
      ProducerView view = new ProducerView(Mockito.mock(ActiveMQServer.class));
      assertNotEquals("user", view.getDefaultOrderColumn());
      view.setOptions("{\"field\":\"user\",\"operation\":\"EQUALS\",\"value\":\"123\",\"sortColumn\":\"user\",\"sortOrder\":\"asc\"}");
      assertEquals("user", view.getSortField());
   }

   @Test
   public void testPageSizeAndPageNumber() {
      ActiveMQAbstractView myView = new ActiveMQAbstractView() {
         @Override
         Object getField(Object o, String fieldName) {
            return null;
         }

         @Override
         public Class getClassT() {
            return null;
         }

         @Override
         public JsonObjectBuilder toJson(Object obj) {
            return null;
         }

         @Override
         public String getDefaultOrderColumn() {
            return "";
         }
      };

      List<Integer> list = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
         list.add(i);
      }

      myView.setCollection(list);

      // one or more inputs is -1
      assertEquals(list.size(), myView.getPagedResult(-1, -1).size());
      assertEquals(list.size(), myView.getPagedResult(123, -1).size());
      assertEquals(list.size(), myView.getPagedResult(-1, 123).size());

      // page 0 - not really valid but still "works"
      assertEquals(0, myView.getPagedResult(0, 123).size());


      assertEquals(123, myView.getPagedResult(1, 123).size());

      // last page
      assertEquals(100, myView.getPagedResult(10, 100).size());

      // past the last page
      assertEquals(0, myView.getPagedResult(11, 100).size());
   }
}
