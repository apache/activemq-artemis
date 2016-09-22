/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.junit.Assert;
import org.junit.Test;

public class QueueConfigTest {

   @Test
   public void addressMustBeDefaultedToName() {
      final QueueConfig queueConfig = QueueConfig.builderWith(1L, new SimpleString("queue_name")).build();
      Assert.assertEquals(queueConfig.name(), queueConfig.address());
   }

   @Test(expected = IllegalStateException.class)
   public void cannotAllowNullAddress() {
      QueueConfig.builderWith(1L, new SimpleString("queue_name"), null);
   }

   @Test(expected = IllegalStateException.class)
   public void cannotAllowNullNameWithoutAddress() {
      QueueConfig.builderWith(1L, null);
   }

   @Test(expected = IllegalStateException.class)
   public void cannotAllowNullNameWithAddress() {
      QueueConfig.builderWith(1L, null, new SimpleString("queue_address"));
   }
}
