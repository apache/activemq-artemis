/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.openwire.util;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OpenWireUtilTest {

   // toCoreProduceAddress tests
   @Test
   void toCoreProduceAddress_shouldReturnNullForNullDestination() {
      assertNull(OpenWireUtil.toCoreProduceAddress(null));
   }

   @Test
   void toCoreProduceAddress_shouldReturnPhysicalNameForTemporaryDestination() {
      ActiveMQDestination temporaryDestination = new ActiveMQTempQueue("temp-queue://ID:123");
      assertEquals("temp-queue://ID:123", OpenWireUtil.toCoreProduceAddress(temporaryDestination));
   }

   @Test
   void toCoreProduceAddress_shouldReturnPhysicalNameForAdvisoryTopic() {
      ActiveMQDestination advisoryTopic = AdvisorySupport.getConnectionAdvisoryTopic();
      assertEquals(advisoryTopic.getPhysicalName(), OpenWireUtil.toCoreProduceAddress(advisoryTopic));
   }

   @Test
   void toCoreProduceAddress_shouldPreserveFQQNAddress() {
      ActiveMQDestination fqqnDestination = new ActiveMQQueue("address::queue");
      assertEquals("address::queue", OpenWireUtil.toCoreProduceAddress(fqqnDestination));
   }

   @Test
   void toCoreProduceAddress_shouldEscapeBackslashesInNonFQQNAddress() {
      ActiveMQDestination nonFqqnDestination = new ActiveMQQueue("my\\queue\\name");
      assertEquals("my\\\\queue\\\\name", OpenWireUtil.toCoreProduceAddress(nonFqqnDestination));
   }

   @Test
   void toCoreProduceAddress_shouldOnlyEscapeAddressPartInFQQNWithBackslash() {
      ActiveMQDestination d = new ActiveMQQueue("addr\\\\part::q\\\\name");
      assertEquals("addr\\\\\\\\part::q\\\\name", OpenWireUtil.toCoreProduceAddress(d));
   }

   @Test
   void toCoreProduceAddress_shouldNotStripMismatchedScheme() {
      ActiveMQDestination d = new ActiveMQTopic("queue://foo\\bar");
      assertEquals("queue://foo\\\\bar", OpenWireUtil.toCoreProduceAddress(d));
   }

   @Test
   void toCoreProduceAddress_shouldStripMatchingScheme() {
      ActiveMQDestination d = new ActiveMQQueue("queue://foo\\bar");
      assertEquals("foo\\\\bar", OpenWireUtil.toCoreProduceAddress(d));
   }

   // toCoreConsumePattern tests
   @Test
   void toCoreConsumePattern_shouldReturnNullForNullDestination() {
      ActiveMQServer server = mock(ActiveMQServer.class);
      assertNull(OpenWireUtil.toCoreConsumePattern(null, server));
   }

   @Test
   void toCoreConsumePattern_shouldReturnPhysicalNameForTemporaryDestination() {
      ActiveMQServer server = mock(ActiveMQServer.class);
      ActiveMQDestination temporaryDestination = new ActiveMQTempQueue("temp-queue://ID:123");
      assertEquals("temp-queue://ID:123", OpenWireUtil.toCoreConsumePattern(temporaryDestination, server));
   }

   @Test
   void toCoreConsumePattern_shouldReturnPhysicalNameForAdvisoryTopic() {
      ActiveMQServer server = mock(ActiveMQServer.class);
      ActiveMQDestination advisoryTopic = AdvisorySupport.getConnectionAdvisoryTopic();
      assertEquals(advisoryTopic.getPhysicalName(), OpenWireUtil.toCoreConsumePattern(advisoryTopic, server));
   }

   @Test
   void toCoreConsumePattern_shouldHandleFQQNWithWildcards() {
      ActiveMQServer server = mockServerWith(new WildcardConfiguration().setDelimiter('.')
                                                                        .setAnyWords('%')
                                                                        .setSingleWord('*'));
      ActiveMQDestination fqqnDestination = new ActiveMQQueue("address.*.topic::my.queue");
      assertEquals("address.*.topic::my.queue", OpenWireUtil.toCoreConsumePattern(fqqnDestination, server));

      fqqnDestination = new ActiveMQQueue("address.>.topic::my.queue");
      assertEquals("address.%.topic::my.queue", OpenWireUtil.toCoreConsumePattern(fqqnDestination, server));
   }

   @Test
   void toCoreConsumePattern_shouldPreserveVirtualTopicConsumerQueue() {
      ActiveMQServer server = mock(ActiveMQServer.class);
      ActiveMQDestination virtualTopicConsumerQueue = new ActiveMQQueue("Consumer.A.VirtualTopic.Test");
      assertEquals("Consumer.A.VirtualTopic.Test", OpenWireUtil.toCoreConsumePattern(virtualTopicConsumerQueue, server));
   }

   @Test
   void toCoreConsumePattern_shouldConvertNonFqqnWildcard() {
      ActiveMQServer server = mockServerWith(new WildcardConfiguration().setDelimiter('.')
                                                                        .setAnyWords('%')
                                                                        .setSingleWord('*'));
      ActiveMQDestination d = new ActiveMQQueue("a.>.b.*");
      assertEquals("a.%.b.*", OpenWireUtil.toCoreConsumePattern(d, server));
   }

   @Test
   void toCoreConsumePattern_shouldPassthroughVirtualTopicConsumerWithScheme() {
      ActiveMQServer server = mockServerWithDefaults();
      ActiveMQDestination d = new ActiveMQQueue("queue://Consumer.A.VirtualTopic.Orders.>");
      assertEquals("Consumer.A.VirtualTopic.Orders.>", OpenWireUtil.toCoreConsumePattern(d, server));
   }

   @Test
   void toCoreConsumePattern_shouldTreatPatternDestinationAsWildcard() {
      ActiveMQServer server = mockServerWith(new WildcardConfiguration().setDelimiter('.')
                                                                        .setAnyWords('#')
                                                                        .setSingleWord('*'));
      ActiveMQQueue d = new ActiveMQQueue("foo.>.bar");
      assertEquals("foo.#.bar", OpenWireUtil.toCoreConsumePattern(d, server));
   }

   // Helper methods
   private ActiveMQServer mockServerWith(WildcardConfiguration wc) {
      ActiveMQServer server = mock(ActiveMQServer.class);
      Configuration cfg = mock(Configuration.class);
      when(server.getConfiguration()).thenReturn(cfg);
      when(cfg.getWildcardConfiguration()).thenReturn(wc);
      return server;
   }

   private ActiveMQServer mockServerWithDefaults() {
      WildcardConfiguration wc = new WildcardConfiguration().setDelimiter('.').setAnyWords('>').setSingleWord('*');
      return mockServerWith(wc);
   }
}
