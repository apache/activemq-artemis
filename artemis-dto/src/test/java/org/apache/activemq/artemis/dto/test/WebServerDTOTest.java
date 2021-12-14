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

import org.apache.activemq.artemis.dto.WebServerDTO;
import org.junit.Assert;
import org.junit.Test;

public class WebServerDTOTest extends Assert {

   @Test
   public void testDefault() {
      WebServerDTO webServer = new WebServerDTO();

      Assert.assertNull(webServer.getIncludedTLSProtocols());
      Assert.assertNull(webServer.getExcludedTLSProtocols());
      Assert.assertNull(webServer.getIncludedCipherSuites());
      Assert.assertNull(webServer.getExcludedCipherSuites());
   }

   @Test
   public void testValues() {
      WebServerDTO webServer = new WebServerDTO();

      webServer.setIncludedTLSProtocols("TLSv1.2");
      Assert.assertArrayEquals(new String[] {"TLSv1.2"}, webServer.getIncludedTLSProtocols());

      webServer.setExcludedTLSProtocols("TLSv1,TLSv1.1");
      Assert.assertArrayEquals(new String[] {"TLSv1", "TLSv1.1"}, webServer.getExcludedTLSProtocols());

      webServer.setIncludedCipherSuites( "^SSL_.*$");
      Assert.assertArrayEquals(new String[] {"^SSL_.*$"}, webServer.getIncludedCipherSuites());

      webServer.setExcludedCipherSuites( "^.*_(MD5|SHA|SHA1)$,^TLS_RSA_.*$,^.*_NULL_.*$,^.*_anon_.*$");
      Assert.assertArrayEquals(new String[] {"^.*_(MD5|SHA|SHA1)$", "^TLS_RSA_.*$", "^.*_NULL_.*$", "^.*_anon_.*$"}, webServer.getExcludedCipherSuites());
   }

   @Test
   public void testEmptyValues() {
      WebServerDTO webServer = new WebServerDTO();

      webServer.setIncludedTLSProtocols("");
      Assert.assertArrayEquals(new String[] {""}, webServer.getIncludedTLSProtocols());

      webServer.setExcludedTLSProtocols("");
      Assert.assertArrayEquals(new String[] {""}, webServer.getExcludedTLSProtocols());

      webServer.setIncludedCipherSuites("");
      Assert.assertArrayEquals(new String[] {""}, webServer.getIncludedCipherSuites());

      webServer.setExcludedCipherSuites("");
      Assert.assertArrayEquals(new String[] {""}, webServer.getExcludedCipherSuites());
   }

   @Test
   public void testNullValues() {
      WebServerDTO webServer = new WebServerDTO();

      webServer.setIncludedTLSProtocols(null);
      Assert.assertNull(webServer.getIncludedTLSProtocols());

      webServer.setExcludedTLSProtocols(null);
      Assert.assertNull(webServer.getExcludedTLSProtocols());

      webServer.setIncludedCipherSuites(null);
      Assert.assertNull(webServer.getIncludedCipherSuites());

      webServer.setExcludedCipherSuites(null);
      Assert.assertNull(webServer.getExcludedCipherSuites());
   }
}
