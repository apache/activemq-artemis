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
package org.apache.activemq.artemis.dto.test;

import org.apache.activemq.artemis.dto.BindingDTO;
import org.junit.Assert;
import org.junit.Test;

public class BindingDTOTest extends Assert {

   @Test
   public void testDefault() {
      BindingDTO binding = new BindingDTO();

      Assert.assertNull(binding.getIncludedTLSProtocols());
      Assert.assertNull(binding.getExcludedTLSProtocols());
      Assert.assertNull(binding.getIncludedCipherSuites());
      Assert.assertNull(binding.getExcludedCipherSuites());
   }

   @Test
   public void testValues() {
      BindingDTO binding = new BindingDTO();

      binding.setIncludedTLSProtocols("TLSv1.2");
      Assert.assertArrayEquals(new String[] {"TLSv1.2"}, binding.getIncludedTLSProtocols());

      binding.setExcludedTLSProtocols("TLSv1,TLSv1.1");
      Assert.assertArrayEquals(new String[] {"TLSv1", "TLSv1.1"}, binding.getExcludedTLSProtocols());

      binding.setIncludedCipherSuites( "^SSL_.*$");
      Assert.assertArrayEquals(new String[] {"^SSL_.*$"}, binding.getIncludedCipherSuites());

      binding.setExcludedCipherSuites( "^.*_(MD5|SHA|SHA1)$,^TLS_RSA_.*$,^.*_NULL_.*$,^.*_anon_.*$");
      Assert.assertArrayEquals(new String[] {"^.*_(MD5|SHA|SHA1)$", "^TLS_RSA_.*$", "^.*_NULL_.*$", "^.*_anon_.*$"}, binding.getExcludedCipherSuites());
   }

   @Test
   public void testEmptyValues() {
      BindingDTO binding = new BindingDTO();

      binding.setIncludedTLSProtocols("");
      Assert.assertArrayEquals(new String[] {""}, binding.getIncludedTLSProtocols());

      binding.setExcludedTLSProtocols("");
      Assert.assertArrayEquals(new String[] {""}, binding.getExcludedTLSProtocols());

      binding.setIncludedCipherSuites("");
      Assert.assertArrayEquals(new String[] {""}, binding.getIncludedCipherSuites());

      binding.setExcludedCipherSuites("");
      Assert.assertArrayEquals(new String[] {""}, binding.getExcludedCipherSuites());
   }

   @Test
   public void testNullValues() {
      BindingDTO binding = new BindingDTO();

      binding.setIncludedTLSProtocols(null);
      Assert.assertNull(binding.getIncludedTLSProtocols());

      binding.setExcludedTLSProtocols(null);
      Assert.assertNull(binding.getExcludedTLSProtocols());

      binding.setIncludedCipherSuites(null);
      Assert.assertNull(binding.getIncludedCipherSuites());

      binding.setExcludedCipherSuites(null);
      Assert.assertNull(binding.getExcludedCipherSuites());
   }
}
