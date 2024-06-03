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
package org.apache.activemq.artemis.core.persistence.impl.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentQueueBindingEncoding;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.jupiter.api.Test;

public class QueueBindingEncodingTest {

   @Test
   public void testEncodeDecode() {
      final SimpleString name = RandomUtil.randomSimpleString();
      final SimpleString address = RandomUtil.randomSimpleString();
      final SimpleString filterString = RandomUtil.randomSimpleString();
      final SimpleString user = RandomUtil.randomSimpleString();
      final boolean autoCreated = RandomUtil.randomBoolean();
      final int maxConsumers = RandomUtil.randomInt();
      final boolean purgeOnNoConsumers = RandomUtil.randomBoolean();
      final boolean exclusive = RandomUtil.randomBoolean();
      final boolean groupRebalance = RandomUtil.randomBoolean();
      final int groupBuckets = RandomUtil.randomInt();
      final SimpleString groupFirstKey = RandomUtil.randomSimpleString();
      final boolean lastValue = RandomUtil.randomBoolean();
      final SimpleString lastValueKey = RandomUtil.randomSimpleString();
      final boolean nonDestructive = RandomUtil.randomBoolean();
      final int consumersBeforeDispatch = RandomUtil.randomInt();
      final long delayBeforeDispatch = RandomUtil.randomLong();
      final boolean autoDelete = RandomUtil.randomBoolean();
      final long autoDeleteDelay = RandomUtil.randomLong();
      final long autoDeleteMessageCount = RandomUtil.randomLong();
      final byte routingType = RandomUtil.randomByte();
      final boolean configurationManaged = RandomUtil.randomBoolean();
      final long ringSize = RandomUtil.randomLong();
      final boolean enabled = RandomUtil.randomBoolean();
      final boolean groupRebalancePauseDispatch = RandomUtil.randomBoolean();
      final boolean internal = RandomUtil.randomBoolean();

      PersistentQueueBindingEncoding encoding = new PersistentQueueBindingEncoding(name,
                                                                                   address,
                                                                                   filterString,
                                                                                   user,
                                                                                   autoCreated,
                                                                                   maxConsumers,
                                                                                   purgeOnNoConsumers,
                                                                                   enabled,
                                                                                   exclusive,
                                                                                   groupRebalance,
                                                                                   groupRebalancePauseDispatch,
                                                                                   groupBuckets,
                                                                                   groupFirstKey,
                                                                                   lastValue,
                                                                                   lastValueKey,
                                                                                   nonDestructive,
                                                                                   consumersBeforeDispatch,
                                                                                   delayBeforeDispatch,
                                                                                   autoDelete,
                                                                                   autoDeleteDelay,
                                                                                   autoDeleteMessageCount,
                                                                                   routingType,
                                                                                   configurationManaged,
                                                                                   ringSize,
                                                                                   internal);
      int size = encoding.getEncodeSize();
      ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(size);
      encoding.encode(encodedBuffer);

      PersistentQueueBindingEncoding decoding = new PersistentQueueBindingEncoding();
      decoding.decode(encodedBuffer);

      assertEquals(name, decoding.getQueueName());
      assertEquals(address, decoding.getAddress());
      assertEquals(filterString, decoding.getFilterString());
      assertEquals(user, decoding.getUser());
      assertEquals(autoCreated, decoding.isAutoCreated());
      assertEquals(maxConsumers, decoding.getMaxConsumers());
      assertEquals(purgeOnNoConsumers, decoding.isPurgeOnNoConsumers());
      assertEquals(enabled, decoding.isEnabled());
      assertEquals(exclusive, decoding.isExclusive());
      assertEquals(groupRebalance, decoding.isGroupRebalance());
      assertEquals(groupBuckets, decoding.getGroupBuckets());
      assertEquals(groupFirstKey, decoding.getGroupFirstKey());
      assertEquals(lastValue, decoding.isLastValue());
      assertEquals(lastValueKey, decoding.getLastValueKey());
      assertEquals(nonDestructive, decoding.isNonDestructive());
      assertEquals(consumersBeforeDispatch, decoding.getConsumersBeforeDispatch());
      assertEquals(delayBeforeDispatch, decoding.getDelayBeforeDispatch());
      assertEquals(autoDelete, decoding.isAutoDelete());
      assertEquals(autoDeleteDelay, decoding.getAutoDeleteDelay());
      assertEquals(autoDeleteMessageCount, decoding.getAutoDeleteMessageCount());
      assertEquals(routingType, decoding.getRoutingType());
      assertEquals(configurationManaged, decoding.isConfigurationManaged());
      assertEquals(ringSize, decoding.getRingSize());
      assertEquals(groupRebalancePauseDispatch, decoding.isGroupRebalancePauseDispatch());
      assertEquals(internal, decoding.isInternal());
   }
}
