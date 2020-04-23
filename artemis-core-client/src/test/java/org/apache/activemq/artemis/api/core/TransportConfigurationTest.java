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

package org.apache.activemq.artemis.api.core;

import java.util.HashMap;

import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;

public class TransportConfigurationTest {

   @Test
   public void testEquals() {
      TransportConfiguration configuration = new TransportConfiguration("SomeClass", new HashMap<String, Object>(), null);
      TransportConfiguration configuration2 = new TransportConfiguration("SomeClass", new HashMap<String, Object>(), null);

      Assert.assertEquals(configuration, configuration2);
      Assert.assertEquals(configuration.hashCode(), configuration2.hashCode());

      HashMap<String, Object> configMap1 = new HashMap<>();
      configMap1.put("host", "localhost");
      HashMap<String, Object> configMap2 = new HashMap<>();
      configMap2.put("host", "localhost");

      configuration = new TransportConfiguration("SomeClass", configMap1, null);
      configuration2 = new TransportConfiguration("SomeClass", configMap2, null);
      Assert.assertEquals(configuration, configuration2);
      Assert.assertEquals(configuration.hashCode(), configuration2.hashCode());

      configuration = new TransportConfiguration("SomeClass", configMap1, "name1");
      configuration2 = new TransportConfiguration("SomeClass", configMap2, "name2");
      Assert.assertNotEquals(configuration, configuration2);
      Assert.assertNotEquals(configuration.hashCode(), configuration2.hashCode());
      Assert.assertTrue(configuration.isEquivalent(configuration2));

      configMap1 = new HashMap<>();
      configMap1.put("host", "localhost");
      configMap2 = new HashMap<>();
      configMap2.put("host", "localhost3");
      configuration = new TransportConfiguration("SomeClass", configMap1, null);
      configuration2 = new TransportConfiguration("SomeClass", configMap2, null);
      Assert.assertNotEquals(configuration, configuration2);
      Assert.assertNotEquals(configuration.hashCode(), configuration2.hashCode());

   }

   @Test
   public void testToStringObfuscatesPasswords() {
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secret_password");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secret_password");

      TransportConfiguration configuration = new TransportConfiguration("SomeClass", params, null);

      Assert.assertThat(configuration.toString(), not(containsString("secret_password")));
   }

}
