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
package org.apache.activemq.cli.test;

import org.apache.activemq.artemis.cli.commands.util.DestinationUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DestinationUtilTest {

   private static final String TEST_FQQN_URL = "fqqn://myQueue0::myAddress0";
   private static final String TEST_FQQN = "myQueue0::myAddress0";
   private static final String TEST_QUEUE_NAME = "myQueue0";
   private static final String TEST_ADDR_NAME = "myAddress0";

   @Test
   public void testFQQNUtil() throws Exception {
      String qName = DestinationUtil.getQueueFromFQQN(TEST_FQQN_URL);
      String addrName = DestinationUtil.getAddressFromFQQN(TEST_FQQN_URL);
      String fqqn = DestinationUtil.getFQQNFromDestination(TEST_FQQN_URL);
      assertEquals(TEST_QUEUE_NAME, qName);
      assertEquals(TEST_ADDR_NAME, addrName);
      assertEquals(TEST_FQQN, fqqn);
   }
}
