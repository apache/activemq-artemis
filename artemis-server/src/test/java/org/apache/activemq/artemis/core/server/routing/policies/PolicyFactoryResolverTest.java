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
package org.apache.activemq.artemis.core.server.routing.policies;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PolicyFactoryResolverTest {

   @Test
   public void resolveOk() throws Exception {
      PolicyFactoryResolver instance = PolicyFactoryResolver.getInstance();
      assertNotNull(instance.resolve(ConsistentHashPolicy.NAME));
   }

   @Test(expected = ClassNotFoundException.class)
   public void resolveError() throws Exception {
      PolicyFactoryResolver instance = PolicyFactoryResolver.getInstance();
      assertNotNull(instance.resolve("NOT PRESENT"));
   }

   @Test
   public void keyFromName() throws Exception {
      PolicyFactoryResolver instance = PolicyFactoryResolver.getInstance();
      assertEquals("New", instance.keyFromClassName("NewPolicyFactory"));
   }
}
