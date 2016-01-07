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
package org.apache.activemq.artemis.tests.integration.persistence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSetting;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AddressSettingsConfigurationStorageTest extends StorageManagerTestBase {

   private Map<SimpleString, PersistedAddressSetting> mapExpectedAddresses;

   public AddressSettingsConfigurationStorageTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      mapExpectedAddresses = new HashMap<>();
   }

   protected void addAddress(StorageManager journal1, String address, AddressSettings setting) throws Exception {
      SimpleString str = new SimpleString(address);
      PersistedAddressSetting persistedSetting = new PersistedAddressSetting(str, setting);
      mapExpectedAddresses.put(str, persistedSetting);
      journal1.storeAddressSetting(persistedSetting);
   }

   @Test
   public void testStoreSecuritySettings() throws Exception {
      createStorage();

      AddressSettings setting = new AddressSettings();

      setting = new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK).setDeadLetterAddress(new SimpleString("some-test"));

      addAddress(journal, "a2", setting);

      journal.stop();

      createStorage();

      checkAddresses(journal);

      setting = new AddressSettings().setDeadLetterAddress(new SimpleString("new-adddress"));

      // Replacing the first setting
      addAddress(journal, "a1", setting);

      journal.stop();

      createStorage();

      checkAddresses(journal);

      journal.stop();

      journal = null;

   }

   /**
    * @param journal1
    * @throws Exception
    */
   private void checkAddresses(StorageManager journal1) throws Exception {
      List<PersistedAddressSetting> listSetting = journal1.recoverAddressSettings();

      assertEquals(mapExpectedAddresses.size(), listSetting.size());

      for (PersistedAddressSetting el : listSetting) {
         PersistedAddressSetting el2 = mapExpectedAddresses.get(el.getAddressMatch());

         assertEquals(el.getAddressMatch(), el2.getAddressMatch());
         assertEquals(el.getSetting(), el2.getSetting());
      }
   }
}
