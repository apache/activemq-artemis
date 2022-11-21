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
package org.apache.activemq.artemis.spi.core.security.jaas.kubernetes.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;

import org.apache.activemq.artemis.spi.core.security.jaas.ServiceAccountPrincipal;
import org.junit.Test;

public class ServiceAccountPrincipalTest {

   @Test
   public void testFullName() {
      String name = "system:serviceaccounts:some-ns:some-sa";

      ServiceAccountPrincipal principal = new ServiceAccountPrincipal(name);

      assertThat(principal.getNamespace(), is("some-ns"));
      assertThat(principal.getSaName(), is("some-sa"));
      assertThat(principal.getName(), is(name));
   }

   @Test
   public void testSimpleName() {
      String name = "foo";

      ServiceAccountPrincipal principal = new ServiceAccountPrincipal(name);

      assertThat(principal.getName(), is("foo"));
      assertNull(principal.getSaName());
      assertNull(principal.getNamespace());
   }

}
