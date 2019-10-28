/*
 * Copyright The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.util.EnumSet;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentAddressBindingEncoding;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class AddressBindingEncodingTest extends Assert {

   @Test
   public void testEncodeDecode() {
      final SimpleString name = RandomUtil.randomSimpleString();
      final boolean autoCreated = RandomUtil.randomBoolean();
      final EnumSet<RoutingType> routingTypes = EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST);

      PersistentAddressBindingEncoding encoding = new PersistentAddressBindingEncoding(name,
                                                                                       routingTypes,
                                                                                       autoCreated);
      int size = encoding.getEncodeSize();
      ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(size);
      encoding.encode(encodedBuffer);

      PersistentAddressBindingEncoding decoding = new PersistentAddressBindingEncoding();
      decoding.decode(encodedBuffer);

      assertEquals(name, decoding.getName());
      assertEquals(autoCreated, decoding.autoCreated);
      assertEquals(routingTypes, decoding.routingTypes);
   }
}
