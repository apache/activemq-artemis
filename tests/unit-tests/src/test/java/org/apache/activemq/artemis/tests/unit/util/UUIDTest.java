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
package org.apache.activemq.artemis.tests.unit.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.jupiter.api.Test;

public class UUIDTest extends ActiveMQTestBase {

   static final int MANY_TIMES = 100000;

   @Test
   public void testManyUUIDs() throws Exception {
      Set<String> uuidsSet = new HashSet<>();

      UUIDGenerator gen = UUIDGenerator.getInstance();
      for (int i = 0; i < getTimes(); i++) {
         uuidsSet.add(gen.generateStringUUID());
      }

      // we put them in a set to check duplicates
      assertEquals(getTimes(), uuidsSet.size());
   }

   protected int getTimes() {
      return MANY_TIMES;
   }

   @Test
   public void testStringToUuidConversion() {
      UUIDGenerator gen = UUIDGenerator.getInstance();
      for (int i = 0; i < MANY_TIMES; i++) {
         final UUID uuid = gen.generateUUID();
         final String uuidString = uuid.toString();
         byte[] data2 = UUID.stringToBytes(uuidString);
         final UUID uuid2 = new UUID(UUID.TYPE_TIME_BASED, data2);
         assertEqualsByteArrays(uuid.asBytes(), data2);
         assertEquals(uuid, uuid2, uuidString);
         assertEquals(uuidString, uuid2.toString(), uuidString);
      }
   }
}
