/**
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
package org.apache.activemq.core.settings;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AddressSettingsTest extends UnitTestCase
{
   @Test
   public void testDefaults()
   {
      AddressSettings addressSettings = new AddressSettings();
      Assert.assertEquals(null, addressSettings.getDeadLetterAddress());
      Assert.assertEquals(null, addressSettings.getExpiryAddress());
      Assert.assertEquals(AddressSettings.DEFAULT_MAX_DELIVERY_ATTEMPTS, addressSettings.getMaxDeliveryAttempts());
      Assert.assertEquals(addressSettings.getMaxSizeBytes(), AddressSettings.DEFAULT_MAX_SIZE_BYTES);
      Assert.assertEquals(AddressSettings.DEFAULT_PAGE_SIZE, addressSettings.getPageSizeBytes());
      Assert.assertEquals(AddressSettings.DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT,
                          addressSettings.getMessageCounterHistoryDayLimit());
      Assert.assertEquals(AddressSettings.DEFAULT_REDELIVER_DELAY, addressSettings.getRedeliveryDelay());
      Assert.assertEquals(AddressSettings.DEFAULT_REDELIVER_MULTIPLIER,
                          addressSettings.getRedeliveryMultiplier(),
                          0.000001);
      Assert.assertEquals(AddressSettings.DEFAULT_SLOW_CONSUMER_THRESHOLD, addressSettings.getSlowConsumerThreshold());
      Assert.assertEquals(AddressSettings.DEFAULT_SLOW_CONSUMER_CHECK_PERIOD, addressSettings.getSlowConsumerCheckPeriod());
      Assert.assertEquals(AddressSettings.DEFAULT_SLOW_CONSUMER_POLICY, addressSettings.getSlowConsumerPolicy());

   }

   @Test
   public void testSingleMerge()
   {
      AddressSettings addressSettings = new AddressSettings();
      AddressSettings addressSettingsToMerge = new AddressSettings();
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      addressSettingsToMerge.setDeadLetterAddress(DLQ);
      addressSettingsToMerge.setExpiryAddress(exp);
      addressSettingsToMerge.setMaxDeliveryAttempts(1000);
      addressSettingsToMerge.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);
      addressSettingsToMerge.setMaxSizeBytes(1001);
      addressSettingsToMerge.setMessageCounterHistoryDayLimit(1002);
      addressSettingsToMerge.setRedeliveryDelay(1003);
      addressSettingsToMerge.setPageSizeBytes(1004);
      addressSettings.merge(addressSettingsToMerge);
      Assert.assertEquals(addressSettings.getDeadLetterAddress(), DLQ);
      Assert.assertEquals(addressSettings.getExpiryAddress(), exp);
      Assert.assertEquals(addressSettings.getMaxDeliveryAttempts(), 1000);
      Assert.assertEquals(addressSettings.getMaxSizeBytes(), 1001);
      Assert.assertEquals(addressSettings.getMessageCounterHistoryDayLimit(), 1002);
      Assert.assertEquals(addressSettings.getRedeliveryDelay(), 1003);
      Assert.assertEquals(addressSettings.getPageSizeBytes(), 1004);
      Assert.assertEquals(AddressFullMessagePolicy.DROP, addressSettings.getAddressFullMessagePolicy());
   }

   @Test
   public void testMultipleMerge()
   {
      AddressSettings addressSettings = new AddressSettings();
      AddressSettings addressSettingsToMerge = new AddressSettings();
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      addressSettingsToMerge.setDeadLetterAddress(DLQ);
      addressSettingsToMerge.setExpiryAddress(exp);
      addressSettingsToMerge.setMaxDeliveryAttempts(1000);
      addressSettingsToMerge.setMaxSizeBytes(1001);
      addressSettingsToMerge.setMessageCounterHistoryDayLimit(1002);
      addressSettingsToMerge.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);
      addressSettings.merge(addressSettingsToMerge);

      AddressSettings addressSettingsToMerge2 = new AddressSettings();
      SimpleString exp2 = new SimpleString("testExpiryQueue2");
      addressSettingsToMerge2.setExpiryAddress(exp2);
      addressSettingsToMerge2.setMaxSizeBytes(2001);
      addressSettingsToMerge2.setRedeliveryDelay(2003);
      addressSettingsToMerge2.setRedeliveryMultiplier(2.5);
      addressSettings.merge(addressSettingsToMerge2);

      Assert.assertEquals(addressSettings.getDeadLetterAddress(), DLQ);
      Assert.assertEquals(addressSettings.getExpiryAddress(), exp);
      Assert.assertEquals(addressSettings.getMaxDeliveryAttempts(), 1000);
      Assert.assertEquals(addressSettings.getMaxSizeBytes(), 1001);
      Assert.assertEquals(addressSettings.getMessageCounterHistoryDayLimit(), 1002);
      Assert.assertEquals(addressSettings.getRedeliveryDelay(), 2003);
      Assert.assertEquals(addressSettings.getRedeliveryMultiplier(), 2.5, 0.000001);
      Assert.assertEquals(AddressFullMessagePolicy.DROP, addressSettings.getAddressFullMessagePolicy());
   }

   @Test
   public void testMultipleMergeAll()
   {
      AddressSettings addressSettings = new AddressSettings();
      AddressSettings addressSettingsToMerge = new AddressSettings();
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      addressSettingsToMerge.setDeadLetterAddress(DLQ);
      addressSettingsToMerge.setExpiryAddress(exp);
      addressSettingsToMerge.setMaxSizeBytes(1001);
      addressSettingsToMerge.setRedeliveryDelay(1003);
      addressSettingsToMerge.setRedeliveryMultiplier(1.0);
      addressSettingsToMerge.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);
      addressSettings.merge(addressSettingsToMerge);

      AddressSettings addressSettingsToMerge2 = new AddressSettings();
      SimpleString exp2 = new SimpleString("testExpiryQueue2");
      SimpleString DLQ2 = new SimpleString("testDlq2");
      addressSettingsToMerge2.setExpiryAddress(exp2);
      addressSettingsToMerge2.setDeadLetterAddress(DLQ2);
      addressSettingsToMerge2.setMaxDeliveryAttempts(2000);
      addressSettingsToMerge2.setMaxSizeBytes(2001);
      addressSettingsToMerge2.setMessageCounterHistoryDayLimit(2002);
      addressSettingsToMerge2.setRedeliveryDelay(2003);
      addressSettingsToMerge2.setRedeliveryMultiplier(2.0);
      addressSettingsToMerge2.setMaxRedeliveryDelay(5000);
      addressSettingsToMerge.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.merge(addressSettingsToMerge2);

      Assert.assertEquals(addressSettings.getDeadLetterAddress(), DLQ);
      Assert.assertEquals(addressSettings.getExpiryAddress(), exp);
      Assert.assertEquals(addressSettings.getMaxDeliveryAttempts(), 2000);
      Assert.assertEquals(addressSettings.getMaxSizeBytes(), 1001);
      Assert.assertEquals(addressSettings.getMessageCounterHistoryDayLimit(), 2002);
      Assert.assertEquals(addressSettings.getRedeliveryDelay(), 1003);
      Assert.assertEquals(addressSettings.getRedeliveryMultiplier(), 1.0, 0.000001);
      Assert.assertEquals(addressSettings.getMaxRedeliveryDelay(), 5000);
      Assert.assertEquals(AddressFullMessagePolicy.DROP, addressSettings.getAddressFullMessagePolicy());
   }
}
