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
package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.junit.Assert;
import org.junit.Test;

public class MirrorAddressFilterTest {

   @Test
   public void testAddressFilter() {
      Assert.assertTrue(new MirrorAddressFilter("").match(new SimpleString("any")));
      Assert.assertTrue(new MirrorAddressFilter("test").match(new SimpleString("test123")));
      Assert.assertTrue(new MirrorAddressFilter("a,b").match(new SimpleString("b")));
      Assert.assertTrue(new MirrorAddressFilter("!c").match(new SimpleString("a")));
      Assert.assertTrue(new MirrorAddressFilter("!a,!").match(new SimpleString("b123")));
      Assert.assertFalse(new MirrorAddressFilter("a,b,!ab").match(new SimpleString("ab")));
      Assert.assertFalse(new MirrorAddressFilter("!a,!b").match(new SimpleString("b123")));
      Assert.assertFalse(new MirrorAddressFilter("a,").match(new SimpleString("b")));
   }
}
