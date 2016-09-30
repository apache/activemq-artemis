/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.activemq.artemis.api.core.management;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AddressSettingsInfoTest {

   @Test
   public void shouldLoadFromJSON() {
      String json = "{\n" +
         "\"addressFullMessagePolicy\":\"fullPolicy\",\n" +
         "\"maxSizeBytes\":500,\n" +
         "\"pageSizeBytes\":200,\n" +
         "\"pageCacheMaxSize\":3,\n" +
         "\"maxDeliveryAttempts\":3,\n" +
         "\"redeliveryDelay\":70000,\n" +
         "\"redeliveryMultiplier\":1.5,\n" +
         "\"maxRedeliveryDelay\":100000,\n" +
         "\"DLA\":\"deadLettersGoHere\",\n" +
         "\"expiryAddress\":\"\",\n" +
         "\"lastValueQueue\":true,\n" +
         "\"redistributionDelay\":10004,\n" +
         "\"sendToDLAOnNoRoute\":true,\n" +
         "\"slowConsumerThreshold\":200,\n" +
         "\"slowConsumerCheckPeriod\":300,\n" +
         "\"slowConsumerPolicy\":\"retire\",\n" +
         "\"autoCreateJmsQueues\":true,\n" +
         "\"autoDeleteJmsQueues\":false,\n" +
         "\"autoCreateJmsTopics\":true,\n" +
         "\"autoDeleteJmsTopics\":false\n" +
         "}";
      AddressSettingsInfo addressSettingsInfo = AddressSettingsInfo.from(json);
      assertEquals("fullPolicy", addressSettingsInfo.getAddressFullMessagePolicy());
      assertEquals(500L, addressSettingsInfo.getMaxSizeBytes());
      assertEquals(200L, addressSettingsInfo.getPageSizeBytes());
      assertEquals(3, addressSettingsInfo.getPageCacheMaxSize());
      assertEquals(3, addressSettingsInfo.getMaxDeliveryAttempts());
      assertEquals(70000, addressSettingsInfo.getRedeliveryDelay());
      assertEquals(1.5, addressSettingsInfo.getRedeliveryMultiplier(), 0);
      assertEquals(100000, addressSettingsInfo.getMaxRedeliveryDelay());
      assertEquals("deadLettersGoHere", addressSettingsInfo.getDeadLetterAddress());
      assertEquals("", addressSettingsInfo.getExpiryAddress());
      assertTrue(addressSettingsInfo.isLastValueQueue());
      assertEquals(10004L, addressSettingsInfo.getRedistributionDelay());
      assertTrue(addressSettingsInfo.isSendToDLAOnNoRoute());
      assertEquals(200L, addressSettingsInfo.getSlowConsumerThreshold());
      assertEquals(300L, addressSettingsInfo.getSlowConsumerCheckPeriod());
      assertEquals("retire", addressSettingsInfo.getSlowConsumerPolicy());
      assertTrue(addressSettingsInfo.isAutoCreateJmsQueues());
      assertTrue(addressSettingsInfo.isAutoCreateJmsTopics());
      assertFalse(addressSettingsInfo.isAutoDeleteJmsQueues());
      assertFalse(addressSettingsInfo.isAutoDeleteJmsTopics());
   }

}
