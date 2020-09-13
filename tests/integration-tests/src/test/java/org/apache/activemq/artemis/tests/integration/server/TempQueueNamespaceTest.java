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
package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.SingleServerTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Test;

public class TempQueueNamespaceTest extends SingleServerTestBase {

   @Test
   public void testTempQueueNamespace() throws Exception {
      final String TEMP_QUEUE_NAMESPACE = "temp";
      server.getConfiguration().setTemporaryQueueNamespace(TEMP_QUEUE_NAMESPACE);
      server.getAddressSettingsRepository().addMatch(TEMP_QUEUE_NAMESPACE + ".#", new AddressSettings().setDefaultRingSize(10));
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false).setTemporary(true));

      assertEquals(10, (long) server.locateQueue(queue).getQueueConfiguration().getRingSize());

      session.close();
   }

   @Test
   public void testTempQueueNamespaceNegative() throws Exception {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createQueue(new QueueConfiguration(queue).setAddress(address).setDurable(false).setTemporary(true));

      assertNotEquals(10, (long) server.locateQueue(queue).getQueueConfiguration().getRingSize());

      session.close();
   }
}
