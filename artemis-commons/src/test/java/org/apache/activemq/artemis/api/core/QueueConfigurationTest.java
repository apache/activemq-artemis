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
package org.apache.activemq.artemis.api.core;

import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class QueueConfigurationTest {

   @Test
   public void testSetGroupRebalancePauseDispatch() {
      QueueConfiguration queueConfiguration = new QueueConfiguration("TEST");

      Assert.assertEquals(null, queueConfiguration.isGroupRebalancePauseDispatch());

      queueConfiguration.setGroupRebalancePauseDispatch(true);
      Assert.assertEquals(true, queueConfiguration.isGroupRebalancePauseDispatch());

      queueConfiguration.setGroupRebalancePauseDispatch(false);
      Assert.assertEquals(false, queueConfiguration.isGroupRebalancePauseDispatch());

      queueConfiguration.set(QueueConfiguration.GROUP_REBALANCE_PAUSE_DISPATCH, Boolean.toString(true));
      Assert.assertEquals(true, queueConfiguration.isGroupRebalancePauseDispatch());

      queueConfiguration.set(QueueConfiguration.GROUP_REBALANCE_PAUSE_DISPATCH, Boolean.toString(false));
      Assert.assertEquals(false, queueConfiguration.isGroupRebalancePauseDispatch());
   }

   @Test
   public void testFqqn() {
      final SimpleString ADDRESS = RandomUtil.randomSimpleString();
      final SimpleString QUEUE = RandomUtil.randomSimpleString();
      QueueConfiguration queueConfiguration = new QueueConfiguration(CompositeAddress.toFullyQualified(ADDRESS, QUEUE));
      Assert.assertEquals(ADDRESS, queueConfiguration.getAddress());
      Assert.assertEquals(QUEUE, queueConfiguration.getName());
      Assert.assertTrue(queueConfiguration.isFqqn());
   }

   @Test
   public void testFqqnNegative() {
      final SimpleString ADDRESS = RandomUtil.randomSimpleString();
      final SimpleString QUEUE = RandomUtil.randomSimpleString();
      QueueConfiguration queueConfiguration = new QueueConfiguration(QUEUE).setAddress(ADDRESS);
      Assert.assertEquals(ADDRESS, queueConfiguration.getAddress());
      Assert.assertEquals(QUEUE, queueConfiguration.getName());
      Assert.assertFalse(queueConfiguration.isFqqn());
   }

   @Test
   public void testFqqnViaAddress() {
      final SimpleString ADDRESS = RandomUtil.randomSimpleString();
      final SimpleString QUEUE = RandomUtil.randomSimpleString();
      QueueConfiguration queueConfiguration = new QueueConfiguration(RandomUtil.randomSimpleString()).setAddress(CompositeAddress.toFullyQualified(ADDRESS, QUEUE));
      Assert.assertEquals(ADDRESS, queueConfiguration.getAddress());
      Assert.assertEquals(QUEUE, queueConfiguration.getName());
      Assert.assertTrue(queueConfiguration.isFqqn());
   }
}
